import (
	"bufio"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
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

type DataCenterInfo struct {
	DataCenter string
	Region     string
	City       string
	IPCount    int
	MinLatency int
}

type ScanResult struct {
	IP          string
	DataCenter  string
	Region      string
	City        string
	LatencyStr  string
	TCPDuration time.Duration
}

type TestResult struct {
	IP         string
	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
	LossRate   float64
	Speed      string
}

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
	scanResults   []ScanResult
	scanMutex     sync.Mutex
	locationMap   map[string]location
	upgrader      = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	wsMutex       sync.Mutex
	taskMutex     sync.Mutex
	isTaskRunning bool
	listenPort    int
	speedTestURL  string
	dataDir       string
)

func SetSpeedTestURL(u string) { speedTestURL = u }
func SetDataDir(dir string)     { dataDir = dir }
func dataPath(name string) string {
	if dataDir == "" { return name }
	return filepath.Join(dataDir, name)
}

// ----------------------- 主逻辑 -----------------------

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
	return http.ListenAndServe(fmt.Sprintf(":%d", listenPort), nil)
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil { return }
	defer ws.Close()

	for {
		_, msg, err := ws.ReadMessage()
		if err != nil { break }
		var request struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(msg, &request) != nil { continue }

		switch request.Type {
		case "start_task":
			var p struct{ IPType, Threads, Port, Delay int; SpeedURL string }
			json.Unmarshal(request.Data, &p)
			if p.SpeedURL != "" { SetSpeedTestURL(p.SpeedURL) }
			go runUnifiedTask(ws, p.IPType, p.Threads)
		case "start_test":
			var p struct{ DC string; Port, Delay int }
			json.Unmarshal(request.Data, &p)
			go runDetailedTest(ws, p.DC, p.Port, p.Delay)
		case "start_speed_test":
			var p struct{ IP, SpeedURL string; Port int }
			json.Unmarshal(request.Data, &p)
			if p.SpeedURL != "" { SetSpeedTestURL(p.SpeedURL) }
			go runSpeedTest(ws, p.IP, p.Port)
		}
	}
}

func sendWSMessage(ws *websocket.Conn, msgType string, data interface{}) {
	wsMutex.Lock()
	defer wsMutex.Unlock()
	ws.WriteJSON(map[string]interface{}{"type": msgType, "data": data})
}

func initLocations() {
	filename := dataPath("locations.json")
	var body []byte
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		resp, err := http.Get("https://www.baipiao.eu.org/cloudflare/locations")
		if err == nil {
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			saveToFile(filename, string(body))
		}
	} else {
		body, _ = os.ReadFile(filename)
	}
	var locations []location
	json.Unmarshal(body, &locations)
	locationMap = make(map[string]location)
	for _, loc := range locations { locationMap[loc.Iata] = loc }
}

func runUnifiedTask(ws *websocket.Conn, ipType int, scanMaxThreads int) {
	taskMutex.Lock()
	if isTaskRunning { taskMutex.Unlock(); return }
	isTaskRunning = true
	taskMutex.Unlock()
	defer func() { isTaskRunning = false }()

	filename, apiURL := dataPath("ips-v4.txt"), "https://www.baipiao.eu.org/cloudflare/ips-v4"
	if ipType == 6 { filename, apiURL = dataPath("ips-v6.txt"), "https://www.baipiao.eu.org/cloudflare/ips-v6" }

	content, _ := os.ReadFile(filename)
	if len(content) == 0 {
		contentStr, _ := getURLContent(apiURL)
		saveToFile(filename, contentStr)
		content = []byte(contentStr)
	}

	ipList := parseIPList(string(content))
	if ipType == 6 { ipList = getRandomIPv6s(ipList) } else { ipList = getRandomIPv4s(ipList) }

	scanMutex.Lock()
	scanResults = []ScanResult{}
	scanMutex.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(ipList))
	thread := make(chan struct{}, scanMaxThreads)
	for _, ip := range ipList {
		thread <- struct{}{}
		go func(ip string) {
			defer func() { <-thread; wg.Done() }()
			dialer := &net.Dialer{Timeout: 1 * time.Second}
			start := time.Now()
			conn, err := dialer.Dial("tcp", net.JoinHostPort(ip, "80"))
			if err != nil { return }
			defer conn.Close()
			
			client := http.Client{Transport: &http.Transport{Dial: func(n, a string) (net.Conn, error) { return conn, nil }}, Timeout: 1 * time.Second}
			req, _ := http.NewRequest("GET", "http://"+ip+"/cdn-cgi/trace", nil)
			req.Header.Set("User-Agent", "Mozilla/5.0")
			resp, err := client.Do(req)
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if strings.Contains(string(body), "uag=Mozilla/5.0") {
					regex := regexp.MustCompile(`colo=([A-Z]+)`)
					m := regex.FindStringSubmatch(string(body))
					if len(m) > 1 {
						res := ScanResult{IP: ip, DataCenter: m[1], LatencyStr: fmt.Sprintf("%d ms", time.Since(start).Milliseconds()), TCPDuration: time.Since(start)}
						scanMutex.Lock()
						scanResults = append(scanResults, res)
						scanMutex.Unlock()
						sendWSMessage(ws, "scan_result", res)
					}
				}
			}
		}(ip)
	}
	wg.Wait()
}

func runDetailedTest(ws *websocket.Conn, dc string, port, delay int) {}
func runSpeedTest(ws *websocket.Conn, ip string, port int) {}

func getURLContent(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil { return "", err }
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), nil
}

func saveToFile(fn, content string) error {
	os.MkdirAll(filepath.Dir(fn), 0755)
	return os.WriteFile(fn, []byte(content), 0644)
}

func parseIPList(content string) []string {
	var list []string
	s := bufio.NewScanner(strings.NewReader(content))
	for s.Scan() {
		if l := strings.TrimSpace(s.Text()); l != "" { list = append(list, l) }
	}
	return list
}

func getRandomIPv4s(list []string) []string {
	var res []string
	for _, s := range list {
		base := strings.Split(strings.TrimSuffix(s, "/24"), ".")
		if len(base) == 4 {
			base[3] = strconv.Itoa(rand.Intn(256))
			res = append(res, strings.Join(base, "."))
		}
	}
	return res
}

func getRandomIPv6s(list []string) []string {
	var res []string
	for _, s := range list {
		parts := strings.Split(strings.TrimSuffix(s, "/48"), ":")[:3]
		for i := 0; i < 5; i++ { parts = append(parts, fmt.Sprintf("%x", rand.Intn(65536))) }
		res = append(res, strings.Join(parts, ":"))
	}
	return res
}
