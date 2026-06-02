package cfdata

import (
	"bufio"
	"context"
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

//go:embed index.html
var staticFiles embed.FS

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

var (
	scanResults []ScanResult
	scanMutex   sync.Mutex

	locationMap map[string]location
	locMutex    sync.RWMutex

	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	wsMutex sync.Mutex

	taskMutex     sync.Mutex
	isTaskRunning bool

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

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("WebSocket 升级失败:", err)
		return
	}
	defer ws.Close()

	for {
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
			if params.Threads <= 0 {
				params.Threads = 50
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

// ========== 修复: 总是发送 locations 加载日志 ==========
func ensureLocations(ws *websocket.Conn) bool {
	locMutex.RLock()
	loaded := locationMap != nil && len(locationMap) > 0
	count := len(locationMap)
	locMutex.RUnlock()

	if !loaded {
		sendWSMessage(ws, "log", "位置信息未加载，正在初始化...")
		initLocations()

		locMutex.RLock()
		loaded = locationMap != nil && len(locationMap) > 0
		count = len(locationMap)
		locMutex.RUnlock()

		if !loaded {
			sendWSMessage(ws, "error", "位置信息加载失败，请检查网络或 locations.json 文件")
			return false
		}
	}

	sendWSMessage(ws, "log", fmt.Sprintf("已加载 %d 个数据中心位置信息", count))
	return true
}

func initLocations() {
	filename := dataPath("locations.json")
	url := "https://www.baipiao.eu.org/cloudflare/locations"
	var locations []location
	var body []byte
	var err error

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

	locMutex.Lock()
	locationMap = make(map[string]location)
	for _, loc := range locations {
		locationMap[loc.Iata] = loc
	}
	locMutex.Unlock()
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

	if !ensureLocations(ws) {
		return
	}

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

	if _, err = os.Stat(filename); os.IsNotExist(err) {
		sendWSMessage(ws, "log", fmt.Sprintf("本地 %s 不存在，正在下载...", filename))
		content, err = getURLContent(apiURL)
		if err != nil {
			sendWSMessage(ws, "error", "下载 IP 列表失败: "+err.Error())
			return
		}
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
	var countMutex sync.Mutex

	for _, ip := range ipList {
		thread <- struct{}{}
		go func(ip string) {
			defer func() {
				<-thread
				wg.Done()
				countMutex.Lock()
				count++
				currentCount := count
				countMutex.Unlock()
				if currentCount%10 == 0 || currentCount == total {
					sendWSMessage(ws, "scan_progress", map[string]int{
						"current": currentCount,
						"total":   total,
					})
				}
			}()

			// ========== 修复核心: 使用 http.ReadResponse 正确解析 HTTP 响应 ==========
			targetAddr := net.JoinHostPort(ip, "80")

			// 1. 建立 TCP 连接并测量延迟
			start := time.Now()
			conn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
			if err != nil {
				return
			}
			tcpDuration := time.Since(start)

			// 2. 设置超时
			conn.SetDeadline(time.Now().Add(5 * time.Second))

			// 3. 发送 HTTP 请求
			httpReq := fmt.Sprintf("GET /cdn-cgi/trace HTTP/1.1\r\n"+
				"Host: %s\r\n"+
				"User-Agent: Mozilla/5.0\r\n"+
				"Connection: close\r\n\r\n", targetAddr)

			_, err = conn.Write([]byte(httpReq))
			if err != nil {
				conn.Close()
				return
			}

			// 4. 使用 http.ReadResponse 解析响应（自动处理 chunked encoding）
			reader := bufio.NewReader(conn)
			resp, err := http.ReadResponse(reader, nil)
			if err != nil {
				conn.Close()
				return
			}

			// 5. 读取 body
			bodyBytes, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			conn.Close()
			if err != nil {
				return
			}

			// 6. 解析 body
			bodyStr := string(bodyBytes)
			if strings.Contains(bodyStr, "uag=Mozilla/5.0") {
				regex := regexp.MustCompile(`colo=([A-Z]+)`)
				matches := regex.FindStringSubmatch(bodyStr)
				if len(matches) > 1 {
					dataCenter := matches[1]
					locMutex.RLock()
					loc := locationMap[dataCenter]
					locMutex.RUnlock()
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

	scanMutex.Lock()
	resultsCount := len(scanResults)
	scanMutex.Unlock()

	if resultsCount == 0 {
		sendWSMessage(ws, "error", "扫描完成，但未发现任何有效IP。请检查网络状态或尝试更换IP类型/增加延迟阈值。")
		return
	}

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
	var countMutex sync.Mutex

	for _, ip := range testIPList {
		thread <- struct{}{}
		go func(ip string) {
			defer func() {
				<-thread
				wg.Done()
				countMutex.Lock()
				count++
				currentCount := count
				countMutex.Unlock()
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
				sendWSMessage(ws, "test_result", res)
				resMutex.Lock()
				results = append(results, res)
				resMutex.Unlock()
			}
		}(ip)
	}
	wg.Wait()

	sort.Slice(results, func(i, j int) bool {
		if results[i].LossRate != results[j].LossRate {
			return results[i].LossRate < results[j].LossRate
		}
		minI := results[i].MinLatency / time.Millisecond
		minJ := results[j].MinLatency / time.Millisecond
		if minI != minJ {
			return minI < minJ
		}
		if results[i].MaxLatency != results[j].MaxLatency {
			return results[i].MaxLatency < results[j].MaxLatency
		}
		return results[i].AvgLatency < results[j].AvgLatency
	})

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

	// 使用标准 http.Client 进行测速（测速不需要精确控制连接）
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
			},
			TLSHandshakeTimeout: 10 * time.Second,
			DisableKeepAlives:   true,
		},
		Timeout: 15 * time.Second,
	}

	fullURL := fmt.Sprintf("%s://%s%s", scheme, hostname, parsedURL.RequestURI())
	req, _ := http.NewRequest("GET", fullURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Host = hostname

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

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func getFileContent(filename string) (string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

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
