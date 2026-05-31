package main

import (
	"bufio"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ----------------------- 嵌入静态文件 -----------------------

//go:embed index.html
var staticFiles embed.FS

// ----------------------- 数据类型定义 -----------------------

// DataCenterInfo 数据中心信息
type DataCenterInfo struct {
	DataCenter string
	Region     string
	City       string
	IPCount    int
	MinLatency int // 毫秒
}

// ScanResult 扫描结果
type ScanResult struct {
	IP          string
	DataCenter  string
	Region      string
	City        string
	LatencyStr  string
	TCPDuration time.Duration
}

// TestResult 测试结果
type TestResult struct {
	IP         string
	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
	LossRate   float64
	Speed      string
}

// location 位置信息
type location struct {
	Iata   string  `json:"iata"`
	Lat    float64 `json:"lat"`
	Lon    float64 `json:"lon"`
	Cca2   string  `json:"cca2"`
	Region string  `json:"region"`
	City   string  `json:"city"`
}

// ----------------------- 全局变量 -----------------------

var (
	// 扫描结果存储
	scanResults []ScanResult
	scanMutex   sync.Mutex

	// 位置信息映射
	locationMap map[string]location

	// WebSocket 升级器
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// WebSocket 写入锁
	wsMutex sync.Mutex

	// 全局任务锁
	taskMutex     sync.Mutex
	isTaskRunning bool

	// 命令行参数
	listenPort   int
	speedTestURL string
	dataDir      string
)

func SetSpeedTestURL(u string) {
	speedTestURL = u
}

func SetDataDir(dir string) {
	dataDir = dir
}

func dataPath(name string) string {
	if dataDir == "" {
		return name
	}
	return filepath.Join(dataDir, name)
}

// ----------------------- 主函数 -----------------------

func StartServer(port int, url string) error {
	listenPort = port
	speedTestURL = url

	initLocations()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data, err := staticFiles.ReadFile("index.html")
		if err != nil {
			http.Error(w, "无法加载页面", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	http.HandleFunc("/ws", handleWebSocket)

	addr := fmt.Sprintf(":%d", listenPort)
	fmt.Printf("服务启动于 http://localhost:%d\n", listenPort)
	fmt.Printf("测速地址: %s\n", speedTestURL)

	return http.ListenAndServe(addr, nil)
}

// ----------------------- WebSocket 处理 -----------------------

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("WebSocket 升级失败:", err)
		return
	}
	defer ws.Close()

	for {
		// 读取客户端消息
		_, msg, err := ws.ReadMessage()
		if err != nil {
			break
		}

		var request struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(msg, &request); err != nil {
			continue
		}

		// 根据消息类型分发任务
		switch request.Type {
		case "start_task":
			var params struct {
				IPType   int    `json:"ipType"`
				Threads  int    `json:"threads"`
				Port     int    `json:"port"`
				Delay    int    `json:"delay"`
				SpeedURL string `json:"speedUrl"`
			}
			json.Unmarshal(request.Data, &params)
			if params.SpeedURL != "" {
				SetSpeedTestURL(params.SpeedURL)
			}
			go runUnifiedTask(ws, params.IPType, params.Threads)

		case "start_test":
			var params struct {
				DC    string `json:"dc"`
				Port  int    `json:"port"`
				Delay int    `json:"delay"`
			}
			json.Unmarshal(request.Data, &params)
			go runDetailedTest(ws, params.DC, params.Port, params.Delay)

		case "start_speed_test":
			var params struct {
				IP       string `json:"ip"`
				Port     int    `json:"port"`
				SpeedURL string `json:"speedUrl"`
			}
			json.Unmarshal(request.Data, &params)
			if params.SpeedURL != "" {
				SetSpeedTestURL(params.SpeedURL)
			}
			go runSpeedTest(ws, params.IP, params.Port)
		}
	}
}

func sendWSMessage(ws *websocket.Conn, msgType string, data interface{}) {
	wsMutex.Lock()
	defer wsMutex.Unlock()
	msg := map[string]interface{}{
		"type": msgType,
		"data": data,
	}
	ws.WriteJSON(msg)
}

// ----------------------- 核心逻辑 -----------------------

func initLocations() {
	filename := dataPath("locations.json")
	url := "https://www.baipiao.eu.org/cloudflare/locations"
	var locations []location
	var body []byte
	var err error

	// 检查本地文件是否存在
	if _, err = os.Stat(filename); os.IsNotExist(err) {
		fmt.Printf("本地 %s 不存在，正在从服务器下载...\n", filename)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("获取位置信息失败:", err)
			return
		}
		defer resp.Body.Close()
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("读取响应内容失败:", err)
			return
		}
		// 保存到本地
		if err := saveToFile(filename, string(body)); err != nil {
			fmt.Println("保存位置信息文件失败:", err)
		}
	} else {
		fmt.Printf("读取本地 %s 文件...\n", filename)
		body, err = os.ReadFile(filename)
		if err != nil {
			fmt.Println("读取本地位置文件失败:", err)
			return
		}
	}

	if err := json.Unmarshal(body, &locations); err != nil {
		fmt.Println("解析位置信息JSON失败:", err)
		return
	}

	locationMap = make(map[string]location)
	for _, loc := range locations {
		locationMap[loc.Iata] = loc
	}
	fmt.Printf("已加载 %d 个数据中心位置信息\n", len(locationMap))
}

func runUnifiedTask(ws *websocket.Conn, ipType int, scanMaxThreads int) {
	taskMutex.Lock()
	if isTaskRunning {
		taskMutex.Unlock()
		sendWSMessage(ws, "error", "已有任务正在运行，请等待完成后再试")
		return
	}
	isTaskRunning = true
	taskMutex.Unlock()

	defer func() {
		taskMutex.Lock()
		isTaskRunning = false
		taskMutex.Unlock()
	}()

	sendWSMessage(ws, "log", "开始扫描任务...")

	// 确定文件名和URL
	var filename, apiURL string
	if ipType == 6 {
		filename = dataPath("ips-v6.txt")
		apiURL = "https://www.baipiao.eu.org/cloudflare/ips-v6"
	} else {
		filename = dataPath("ips-v4.txt")
		apiURL = "https://www.baipiao.eu.org/cloudflare/ips-v4"
	}

	var content string
	var err error

	// 检查本地文件是否存在
	if _, err = os.Stat(filename); os.IsNotExist(err) {
		sendWSMessage(ws, "log", fmt.Sprintf("本地 %s 不存在，正在下载...", filename))
		content, err = getURLContent(apiURL)
		if err != nil {
			sendWSMessage(ws, "error", "下载 IP 列表失败: "+err.Error())
			return
		}
		// 保存到本地
		if err := saveToFile(filename, content); err != nil {
			sendWSMessage(ws, "log", "警告: 保存IP文件失败: "+err.Error())
		}
	} else {
		sendWSMessage(ws, "log", fmt.Sprintf("读取本地 %s 文件...", filename))
		content, err = getFileContent(filename)
		if err != nil {
			sendWSMessage(ws, "error", "读取本地 IP 列表失败: "+err.Error())
			return
		}
	}

	ipList := parseIPList(content)
	if ipType == 6 {
		ipList = getRandomIPv6s(ipList)
	} else {
		ipList = getRandomIPv4s(ipList)
	}

	scanMutex.Lock()
	scanResults = []ScanResult{}
	scanMutex.Unlock()

	sendWSMessage(ws, "log", fmt.Sprintf("正在扫描 %d 个 IP 地址...", len(ipList)))

	var wg sync.WaitGroup
	wg.Add(len(ipList))
	thread := make(chan struct{}, scanMaxThreads)
	var count int
	total := len(ipList)

	for _, ip := range ipList {
		thread <- struct{}{}
		go func(ip string) {
			defer func() {
				<-thread
				wg.Done()
				scanMutex.Lock()
				count++
				currentCount := count
				scanMutex.Unlock()
				if currentCount%10 == 0 || currentCount == total {
					sendWSMessage(ws, "scan_progress", map[string]int{
						"current": currentCount,
						"total":   total,
					})
				}
			}()

			dialer := &net.Dialer{Timeout: 1 * time.Second}
			start := time.Now()
			conn, err := dialer.Dial("tcp", net.JoinHostPort(ip, "80"))
			if err != nil {
				return
			}
			defer conn.Close()
			tcpDuration := time.Since(start)

			client := http.Client{
				Transport: &http.Transport{
					Dial: func(network, addr string) (net.Conn, error) { return conn, nil },
				},
				Timeout: 1 * time.Second,
			}

			requestURL := "http://" + net.JoinHostPort(ip, "80") + "/cdn-cgi/trace"
			req, _ := http.NewRequest("GET", requestURL, nil)
			req.Header.Set("User-Agent", "Mozilla/5.0")
			req.Close = true
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return
			}
			bodyStr := string(bodyBytes)
			if strings.Contains(bodyStr, "uag=Mozilla/5.0") {
				regex := regexp.MustCompile(`colo=([A-Z]+)`)
				matches := regex.FindStringSubmatch(bodyStr)
				if len(matches) > 1 {
					dataCenter := matches[1]
					loc := locationMap[dataCenter]
					res := ScanResult{
						IP:          ip,
						DataCenter:  dataCenter,
						Region:      loc.Region,
						City:        loc.City,
						LatencyStr:  fmt.Sprintf("%d ms", tcpDuration.Milliseconds()),
						TCPDuration: tcpDuration,
					}
					scanMutex.Lock()
					scanResults = append(scanResults, res)
					scanMutex.Unlock()
					sendWSMessage(ws, "scan_result", res)
				}
			}
		}(ip)
	}
	wg.Wait()

	// ---------------- Bug Fix: 检查是否有结果 ----------------
	scanMutex.Lock()
	resultsCount := len(scanResults)
	scanMutex.Unlock()

	if resultsCount == 0 {
		sendWSMessage(ws, "error", "扫描完成，但未发现任何有效IP。请检查网络状态或尝试更换IP类型/增加延迟阈值。")
		return
	}
	// ------------------------------------------------------

	scanMutex.Lock()
	sort.Slice(scanResults, func(i, j int) bool {
		return scanResults[i].TCPDuration < scanResults[j].TCPDuration
	})
	scanMutex.Unlock()

	dcMap := make(map[string]*DataCenterInfo)
	scanMutex.Lock()
	for _, res := range scanResults {
		if _, ok := dcMap[res.DataCenter]; !ok {
			dcMap[res.DataCenter] = &DataCenterInfo{
				DataCenter: res.DataCenter,
				Region:     res.Region,
				City:       res.City,
				IPCount:    0,
				MinLatency: 999999,
			}
		}
		info := dcMap[res.DataCenter]
		info.IPCount++
		lat, _ := strconv.Atoi(strings.TrimSuffix(res.LatencyStr, " ms"))
		if lat < info.MinLatency {
			info.MinLatency = lat
		}
	}
	scanMutex.Unlock()

	var dcList []DataCenterInfo
	for _, info := range dcMap {
		dcList = append(dcList, *info)
	}
	sort.Slice(dcList, func(i, j int) bool {
		return dcList[i].MinLatency < dcList[j].MinLatency
	})

	sendWSMessage(ws, "log", "扫描完成，请选择数据中心进行详细测试")
	sendWSMessage(ws, "scan_complete_wait_dc", dcList)
}

func runDetailedTest(ws *websocket.Conn, selectedDC string, port int, delay int) {
	var testIPList []string
	scanMutex.Lock()
	for _, res := range scanResults {
		if selectedDC == "" || res.DataCenter == selectedDC {
			testIPList = append(testIPList, res.IP)
		}
	}
	scanMutex.Unlock()

	if len(testIPList) == 0 {
		sendWSMessage(ws, "error", "没有找到可测试的 IP 地址")
		return
	}

	sendWSMessage(ws, "log", fmt.Sprintf("开始对 %s 的 %d 个 IP 进行详细测试...", selectedDC, len(testIPList)))

	var results []TestResult
	var resMutex sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(testIPList))
	thread := make(chan struct{}, 50)
	var count int
	total := len(testIPList)

	for _, ip := range testIPList {
		thread <- struct{}{}
		go func(ip string) {
			defer func() {
				<-thread
				wg.Done()
				scanMutex.Lock()
				count++
				currentCount := count
				scanMutex.Unlock()
				if currentCount%5 == 0 || currentCount == total {
					sendWSMessage(ws, "test_progress", map[string]int{
						"current": currentCount,
						"total":   total,
					})
				}
			}()

			dialer := &net.Dialer{Timeout: time.Duration(delay) * time.Millisecond}
			successCount := 0
			totalLatency := time.Duration(0)
			minLatency := time.Duration(math.MaxInt64)
			maxLatency := time.Duration(0)

			for i := 0; i < 10; i++ {
				start := time.Now()
				conn, err := dialer.Dial("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
				if err != nil {
					continue
				}
				latency := time.Since(start)
				if latency > time.Duration(delay)*time.Millisecond {
					conn.Close()
					continue
				}
				successCount++
				totalLatency += latency
				if latency < minLatency {
					minLatency = latency
				}
				if latency > maxLatency {
					maxLatency = latency
				}
				conn.Close()
			}

			if successCount > 0 {
				avgLatency := totalLatency / time.Duration(successCount)
				lossRate := float64(10-successCount) / 10.0
				res := TestResult{
					IP:         ip,
					MinLatency: minLatency,
					MaxLatency: maxLatency,
					AvgLatency: avgLatency,
					LossRate:   lossRate,
				}
				// 实时发送一个结果给前端（仅作展示）
				sendWSMessage(ws, "test_result", res)

				// 收集结果
				resMutex.Lock()
				results = append(results, res)
				resMutex.Unlock()
			}
		}(ip)
	}
	wg.Wait()

	// ==========================================
	// 后端排序逻辑: 丢包 -> 最小(ms取整) -> 最大 -> 平均
	// ==========================================
	sort.Slice(results, func(i, j int) bool {
		// 1. 丢包率 (升序)
		if results[i].LossRate != results[j].LossRate {
			return results[i].LossRate < results[j].LossRate
		}

		// 2. 最小延迟 (毫秒取整比较, 升序)
		// 核心逻辑：将纳秒转为毫秒整数，忽略微小差异
		minI := results[i].MinLatency / time.Millisecond
		minJ := results[j].MinLatency / time.Millisecond
		if minI != minJ {
			return minI < minJ
		}

		// 3. 最大延迟 (升序)
		// 只有在最小延迟的毫秒数一样时，才比较最大延迟
		if results[i].MaxLatency != results[j].MaxLatency {
			return results[i].MaxLatency < results[j].MaxLatency
		}

		// 4. 平均延迟 (升序)
		return results[i].AvgLatency < results[j].AvgLatency
	})

	// 发送排序后的完整列表给前端
	sendWSMessage(ws, "test_complete", results)
}

func runSpeedTest(ws *websocket.Conn, ip string, port int) {
	sendWSMessage(ws, "log", fmt.Sprintf("开始对 IP %s 端口 %d 进行测速...", ip, port))
	scheme := "http"
	if port == 443 || port == 2053 || port == 2083 || port == 2087 || port == 2096 || port == 8443 {
		scheme = "https"
	}

	testURL := speedTestURL
	if !strings.HasPrefix(testURL, "http://") && !strings.HasPrefix(testURL, "https://") {
		testURL = scheme + "://" + testURL
	}

	parsedURL, err := url.Parse(testURL)
	if err != nil {
		sendWSMessage(ws, "speed_test_result", map[string]string{
			"ip":    ip,
			"speed": "URL解析错误",
		})
		return
	}
	hostname := parsedURL.Hostname()

	client := http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return net.Dial("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
			},
			TLSHandshakeTimeout: 10 * time.Second,
		},
		Timeout: 15 * time.Second,
	}

	fullURL := fmt.Sprintf("%s://%s%s", scheme, hostname, parsedURL.RequestURI())
	req, _ := http.NewRequest("GET", fullURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		sendWSMessage(ws, "speed_test_result", map[string]string{
			"ip":    ip,
			"speed": "连接错误",
		})
		sendWSMessage(ws, "log", "测速失败: "+err.Error())
		return
	}
	defer resp.Body.Close()

	buf := make([]byte, 32*1024)
	var totalBytes int64
	var maxSpeed float64
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastBytes := int64(0)
	lastTime := start
	done := false
	for !done {
		select {
		case <-timeout:
			done = true
		case <-ticker.C:
			now := time.Now()
			duration := now.Sub(lastTime).Seconds()
			if duration > 0 {
				bytesDiff := totalBytes - lastBytes
				currentSpeed := float64(bytesDiff) / duration / 1024 / 1024
				if currentSpeed > maxSpeed {
					maxSpeed = currentSpeed
				}
			}
			lastBytes = totalBytes
			lastTime = now
		default:
			n, err := resp.Body.Read(buf)
			if n > 0 {
				totalBytes += int64(n)
			}
			if err != nil {
				done = true
			}
		}
	}

	speedStr := fmt.Sprintf("%.2f MB/s", maxSpeed)
	sendWSMessage(ws, "speed_test_result", map[string]string{
		"ip":    ip,
		"speed": speedStr,
	})
	sendWSMessage(ws, "log", fmt.Sprintf("IP %s 测速完成: %s", ip, speedStr))
}

func getURLContent(targetURL string) (string, error) {
	resp, err := http.Get(targetURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// getFileContent 读取本地文件内容
func getFileContent(filename string) (string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// saveToFile 保存内容到文件
func saveToFile(filename, content string) error {
	dir := filepath.Dir(filename)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(filename, []byte(content), 0644)
}

func parseIPList(content string) []string {
	scanner := bufio.NewScanner(strings.NewReader(content))
	var ipList []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			ipList = append(ipList, line)
		}
	}
	return ipList
}

func getRandomIPv4s(ipList []string) []string {
	var randomIPs []string
	for _, subnet := range ipList {
		baseIP := strings.TrimSuffix(subnet, "/24")
		octets := strings.Split(baseIP, ".")
		if len(octets) != 4 {
			continue
		}
		octets[3] = fmt.Sprintf("%d", rand.Intn(256))
		randomIPs = append(randomIPs, strings.Join(octets, "."))
	}
	return randomIPs
}

func getRandomIPv6s(ipList []string) []string {
	var randomIPs []string
	for _, subnet := range ipList {
		baseIP := strings.TrimSuffix(subnet, "/48")
		sections := strings.Split(baseIP, ":")
		if len(sections) < 3 {
			continue
		}
		sections = sections[:3]
		for i := 0; i < 5; i++ {
			sections = append(sections, fmt.Sprintf("%x", rand.Intn(65536)))
		}
		randomIPs = append(randomIPs, strings.Join(sections, ":"))
	}
	return randomIPs
}
// 新增 main 函数作为安卓端启动入口
func main() {
    // 默认在手机本地 8080 端口启动服务
    err := StartServer(8080, "speed.cloudflare.com/__down?bytes=100000000")
    if err != nil {
        fmt.Println("服务启动失败:", err)
    }
}
