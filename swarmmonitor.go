package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type Config struct {
	QBitHost     string
	QBitUsername string
	QBitPassword string
	InfluxHost   string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string
	SocketPath   string
}

type TorrentInfo struct {
	Name     string  `json:"name"`
	Hash     string  `json:"hash"`
	Size     int64   `json:"size"`
	Progress float64 `json:"progress"`
}

type PeerInfo struct {
	IP          string  `json:"ip"`
	Client      string  `json:"client"`
	Progress    float64 `json:"progress"`
	Downloaded  int64   `json:"downloaded"`
	Uploaded    int64   `json:"uploaded"`
	TimestampNS int64   `json:"timestamp"`
	Hash        string  `json:"hash"`
	Name        string  `json:"name"` // Torrent name
}

type TorrentMonitor struct {
	startTime time.Time
	hash      string
	name      string
	active    bool
	mu        sync.Mutex
}

type SwarmMonitor struct {
	config       Config
	client       *http.Client
	influxClient influxdb2.Client
	cookie       string
	monitors     map[string]*TorrentMonitor
	mu           sync.RWMutex
}

func NewSwarmMonitor(config Config) (*SwarmMonitor, error) {
	monitor := &SwarmMonitor{
		config:   config,
		client:   &http.Client{},
		monitors: make(map[string]*TorrentMonitor),
	}

	monitor.influxClient = influxdb2.NewClient(config.InfluxHost, config.InfluxToken)

	return monitor, nil
}

func (m *SwarmMonitor) login() error {
	data := url.Values{}
	data.Set("username", m.config.QBitUsername)
	data.Set("password", m.config.QBitPassword)

	resp, err := m.client.PostForm(fmt.Sprintf("%s/api/v2/auth/login", m.config.QBitHost), data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	for _, cookie := range resp.Cookies() {
		if cookie.Name == "SID" {
			m.cookie = cookie.Value
			return nil
		}
	}
	return fmt.Errorf("no session cookie found")
}

func (m *SwarmMonitor) getTorrentInfo(hash string) (*TorrentInfo, error) {
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/api/v2/torrents/info?hashes=%s", m.config.QBitHost, hash), nil)
	if err != nil {
		return nil, err
	}

	req.AddCookie(&http.Cookie{Name: "SID", Value: m.cookie})
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var torrents []TorrentInfo
	if err := json.NewDecoder(resp.Body).Decode(&torrents); err != nil {
		return nil, err
	}

	if len(torrents) == 0 {
		return nil, fmt.Errorf("torrent not found")
	}

	return &torrents[0], nil
}

func (m *SwarmMonitor) getPeerInfo(hash string, name string) ([]PeerInfo, error) {
	req, err := http.NewRequest("GET",
		fmt.Sprintf("%s/api/v2/sync/torrent_peers/%s", m.config.QBitHost, hash), nil)
	if err != nil {
		return nil, err
	}

	req.AddCookie(&http.Cookie{Name: "SID", Value: m.cookie})
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var peers []PeerInfo
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, err
	}

	now := time.Now().UnixNano()
	for i := range peers {
		peers[i].TimestampNS = now
		peers[i].Hash = hash
		peers[i].Name = name
	}

	return peers, nil
}

func (m *SwarmMonitor) writePeerData(peers []PeerInfo) error {
	writeAPI := m.influxClient.WriteAPIBlocking(m.config.InfluxOrg, m.config.InfluxBucket)

	for _, peer := range peers {
		p := influxdb2.NewPoint(
			"peer_stats",
			map[string]string{
				"ip":     peer.IP,
				"client": peer.Client,
				"hash":   peer.Hash,
				"name":   peer.Name,
			},
			map[string]interface{}{
				"progress":   peer.Progress,
				"downloaded": peer.Downloaded,
				"uploaded":   peer.Uploaded,
			},
			time.Unix(0, peer.TimestampNS),
		)

		if err := writeAPI.WritePoint(context.Background(), p); err != nil {
			return err
		}
	}

	return nil
}

func (m *SwarmMonitor) monitorTorrent(hash string) {
	torrentInfo, err := m.getTorrentInfo(hash)
	if err != nil {
		log.Printf("Error getting torrent info for %s: %v", hash, err)
		return
	}

	monitor := &TorrentMonitor{
		startTime: time.Now(),
		hash:      hash,
		name:      torrentInfo.Name,
		active:    true,
	}

	m.mu.Lock()
	m.monitors[hash] = monitor
	m.mu.Unlock()

	for start := time.Now(); time.Since(start) < 30*time.Minute; {
		monitor.mu.Lock()
		if !monitor.active {
			monitor.mu.Unlock()
			return
		}
		monitor.mu.Unlock()

		elapsed := time.Since(start)
		var interval time.Duration

		switch {
		case elapsed < 10*time.Minute:
			interval = time.Second
		case elapsed < 30*time.Minute:
			interval = 5 * time.Second
		}

		peers, err := m.getPeerInfo(hash, torrentInfo.Name)
		if err != nil {
			log.Printf("Error getting peer info for %s (%s): %v", torrentInfo.Name, hash, err)
			time.Sleep(interval)
			continue
		}

		if err := m.writePeerData(peers); err != nil {
			log.Printf("Error writing peer data for %s (%s): %v", torrentInfo.Name, hash, err)
		}

		time.Sleep(interval)
	}

	monitor.mu.Lock()
	monitor.active = false
	monitor.mu.Unlock()
}

func (m *SwarmMonitor) periodicCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m.mu.RLock()
		hashes := make([]string, 0, len(m.monitors))
		for hash, monitor := range m.monitors {
			monitor.mu.Lock()
			if !monitor.active {
				hashes = append(hashes, hash)
			}
			monitor.mu.Unlock()
		}
		m.mu.RUnlock()

		for _, hash := range hashes {
			torrentInfo, err := m.getTorrentInfo(hash)
			if err != nil {
				log.Printf("Error getting torrent info for %s: %v", hash, err)
				continue
			}

			peers, err := m.getPeerInfo(hash, torrentInfo.Name)
			if err != nil {
				log.Printf("Error getting peer info for %s (%s): %v", torrentInfo.Name, hash, err)
				continue
			}

			if len(peers) > 0 {
				if err := m.writePeerData(peers); err != nil {
					log.Printf("Error writing peer data for %s (%s): %v", torrentInfo.Name, hash, err)
				}
			}
		}
	}
}

func (m *SwarmMonitor) listenSocket() error {
	if err := os.Remove(m.config.SocketPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	listener, err := net.Listen("unix", m.config.SocketPath)
	if err != nil {
		return err
	}
	defer listener.Close()

	go m.periodicCheck()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}

		var hash string
		if err := json.NewDecoder(conn).Decode(&hash); err != nil {
			log.Printf("Decode error: %v", err)
			conn.Close()
			continue
		}
		conn.Close()

		go m.monitorTorrent(hash)
	}
}

func main() {
	socketPath := flag.String("socket", "/tmp/torrent-monitor.sock", "Unix socket path")
	flag.Parse()

	config := Config{
		QBitHost:     "http://localhost:8080",
		QBitUsername: "admin",
		QBitPassword: "adminpass",
		InfluxHost:   "http://localhost:8086",
		InfluxToken:  "your-token",
		InfluxOrg:    "your-org",
		InfluxBucket: "torrent-metrics",
		SocketPath:   *socketPath,
	}

	monitor, err := NewSwarmMonitor(config)
	if err != nil {
		log.Fatal(err)
	}

	if err := monitor.login(); err != nil {
		log.Fatal("Login failed:", err)
	}

	log.Printf("Listening on socket: %s", config.SocketPath)
	if err := monitor.listenSocket(); err != nil {
		log.Fatal(err)
	}
}
