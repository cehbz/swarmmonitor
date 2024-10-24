package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cehbz/qbittorrent"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	_ "modernc.org/sqlite" // instead of github.com/mattn/go-sqlite3
)

type TorrentNotification struct {
	Hash string
	ID   int
}

var newTorrentChan chan TorrentNotification

var (
	socketPath   string = "/tmp/swarmmonitor.sock"
	qBitAddr     string = "127.0.0.1"
	qBitPort     string = "8080"
	qBitUsername string = "admin"
	qBitPassword string = "adminpass"
	configPath   string = os.Getenv("HOME") + "/.config/swarmmonitor/config.toml"
)

var rootCmd *cobra.Command

func initDB() *sql.DB {
	var err error
	db, err := sql.Open("sqlite", "./torrents.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS torrents (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE
	);

	CREATE TABLE IF NOT EXISTS peers (
		id INTEGER PRIMARY KEY,
		ip TEXT NOT NULL,
		port INTEGER NOT NULL,
		country_code TEXT,
		client TEXT,
		UNIQUE(ip, port)  -- prevent duplicate peer entries
	);

	CREATE TABLE IF NOT EXISTS peer_metrics (
		timestamp TIMESTAMP NOT NULL,
		peer_id INTEGER NOT NULL,
		torrent_id INTEGER NOT NULL,
		uploaded INTEGER NOT NULL,
		downloaded INTEGER NOT NULL,
		pct_complete REAL NOT NULL,
		FOREIGN KEY (peer_id) REFERENCES peers(id),
		FOREIGN KEY (torrent_id) REFERENCES torrents(id)
	);

	CREATE TABLE IF NOT EXISTS main_metrics (
		timestamp TIMESTAMP NOT NULL,
		torrent_id INTEGER NOT NULL,
		uploaded INTEGER NOT NULL,
		downloaded INTEGER NOT NULL,
		FOREIGN KEY (torrent_id) REFERENCES torrents(id)
	);
	`
	_, err = db.Exec(schema)
	if err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}
	return db
}

func monitorTorrent(name, hash string, client *qbittorrent.Client, ctx context.Context, db *sql.DB) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	var torrentID int
	err := db.QueryRow(`
		INSERT INTO torrents (name, hash)
		VALUES (?, ?)
		ON CONFLICT(hash)
		DO UPDATE SET name = excluded.name
		RETURNING id
	`, name, hash).Scan(&torrentID)
	if err != nil {
		log.Printf("Error inserting or getting torrent ID: %v", err)
		return
	}

	// Send notification about the new torrent
	select {
	case newTorrentChan <- TorrentNotification{Hash: hash, ID: torrentID}:
	default:
		log.Printf("Warning: newTorrentChan is full, couldn't notify about new torrent %s", hash)
	}

	lastRid := 0
	lastUploaded := make(map[int]int64)
	lastDownloaded := make(map[int]int64)
	for {
		select {
		case <-ticker.C:
			peers, err := client.SyncTorrentPeers(hash, lastRid)
			if err != nil {
				log.Printf("Error getting peer info for hash %s: %v", hash, err)
				continue
			}
			lastRid = peers.Rid

			func() {
				tx, err := db.Begin()
				if err != nil {
					log.Printf("Error starting transaction: %v", err)
					return
				}
				defer func() {
					if p := recover(); p != nil {
						tx.Rollback()
						panic(p)
					} else if err != nil {
						tx.Rollback()
					} else {
						err = tx.Commit()
						if err != nil {
							log.Printf("Error committing transaction: %v", err)
						}
					}
				}()

				for _, peer := range peers.Peers {
					var peerID int
					err := tx.QueryRow(`
								INSERT INTO peers (ip, port, client)
								VALUES (?, ?, ?)
								ON CONFLICT(ip, port)
								DO UPDATE SET client = excluded.client
								RETURNING id
							`, peer.IP, peer.Port, peer.Client).Scan(&peerID)
					if err != nil {
						log.Printf("Error inserting or getting peer ID: %v", err)
						continue
					}
					ts := time.Now().UTC()
					uploaded := peer.Uploaded - lastUploaded[peerID]
					if uploaded < 0 {
						uploaded = 0
					}
					downloaded := peer.Downloaded - lastDownloaded[peerID]
					if downloaded < 0 {
						downloaded = 0
					}
					_, err = tx.Exec(`INSERT INTO peer_metrics (timestamp, peer_id, torrent_id, uploaded, downloaded, pct_complete)
					VALUES (?, ?, ?, ?, ?, ?)`,
						ts.Format(time.RFC3339), peerID, torrentID, uploaded, downloaded, peer.Progress)
					if err != nil {
						log.Printf("Error inserting peer metrics: %v", err)
						continue
					}

					fmt.Printf("%s peer_metrics %40s,%15s:%-5d: %20s %5.3f %12d %12d\n",
						ts.Format(time.Stamp), hash, peer.IP, peer.Port, peer.Client, peer.Progress, downloaded, uploaded)
					lastUploaded[peerID] = peer.Uploaded
					lastDownloaded[peerID] = peer.Downloaded
				}
			}()
		case <-ctx.Done():
			log.Printf("Stopping monitor for hash: %s", hash)
			return
		}
	}
}

func periodicCheck(client *qbittorrent.Client, ctx context.Context, db *sql.DB, newTorrentChan <-chan TorrentNotification) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	rid := 0
	mainData, err := client.SyncMainData(rid)
	if err != nil {
		log.Fatalf("Error getting initial main data: %v", err)
	}
	lastUploaded := make(map[string]int64)
	lastDownloaded := make(map[string]int64)
	hash_ids := make(map[string]int)

	func() {
		tx, err := db.Begin()
		if err != nil {
			log.Fatalf("Error starting transaction: %v", err)
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				panic(p)
			} else if err != nil {
				tx.Rollback()
			} else {
				if commitErr := tx.Commit(); commitErr != nil {
					log.Printf("Error committing transaction: %v", commitErr)
				}
			}
		}()

		// Insert torrents into the torrents table and remember their ids
		for hash, torrent := range mainData.Torrents {
			var id int
			err = tx.QueryRow(`INSERT INTO torrents (hash, name) VALUES (?, ?)
				ON CONFLICT(hash) DO UPDATE SET name = excluded.name
				RETURNING id`,
				hash, torrent.Name).Scan(&id)
			if err != nil {
				log.Printf("Error inserting torrent %s: %v", hash, err)
				continue
			}
			hash_ids[hash] = id
			lastUploaded[hash] = torrent.Uploaded
			lastDownloaded[hash] = torrent.Downloaded
		}
	}()

	for {
		select {
		case <-ticker.C:
			mainData, err := client.SyncMainData(rid)
			if err != nil {
				log.Printf("Error getting main data: %v", err)
				continue
			}
			rid = mainData.Rid

			func() {
				tx, err := db.Begin()
				if err != nil {
					log.Printf("Error starting transaction: %v", err)
					return
				}
				defer func() {
					if p := recover(); p != nil {
						tx.Rollback()
						panic(p)
					} else if err != nil {
						tx.Rollback()
					} else {
						err = tx.Commit()
						if err != nil {
							log.Printf("Error committing transaction: %v", err)
						}
					}
				}()

				for hash, torrent := range mainData.Torrents {
					if torrent.Uploaded > lastUploaded[hash] || torrent.Downloaded > lastDownloaded[hash] {
						// Insert or update main metrics
						uploaded := torrent.Uploaded - lastUploaded[hash]
						if uploaded < 0 {
							uploaded = 0
						}
						downloaded := torrent.Downloaded - lastDownloaded[hash]
						if downloaded < 0 {
							downloaded = 0
						}
						ts := time.Now().UTC()
						_, err := tx.Exec(`INSERT INTO main_metrics (timestamp, torrent_id, uploaded, downloaded)
							VALUES (?, ?, ?, ?)`,
							ts.Format(time.RFC3339), hash_ids[hash], uploaded, downloaded)
						if err != nil {
							log.Printf("Error inserting main metrics (%s): %v", hash, err)
							continue
						}
						fmt.Printf("%s main_metrics %40s: %12d %12d\n", ts.Format(time.Stamp), hash, uploaded, downloaded)

						lastUploaded[hash] = torrent.Uploaded
						lastDownloaded[hash] = torrent.Downloaded
					}
				}
			}()

		case notification := <-newTorrentChan:
			hash_ids[notification.Hash] = notification.ID
			log.Printf("Added new torrent to periodic check: %s (ID: %d)", notification.Hash, notification.ID)

		case done := <-ctx.Done():
			log.Printf("Context done: %v", done)
			return
		}
	}
}

func run(cmd *cobra.Command, args []string) {
	loadConfig(configPath) // Override defaults with config file values

	qbClient, err := qbittorrent.NewClient(qBitUsername, qBitPassword, qBitAddr, qBitPort)
	if err != nil {
		log.Fatalf("Failed to create qBittorrent client: %v", err)
	}

	db := initDB()

	// Context to manage goroutines
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(fmt.Errorf("deferred cancel"))

	newTorrentChan = make(chan TorrentNotification, 10) // adjust buffer size as needed

	go periodicCheck(qbClient, ctx, db, newTorrentChan)

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed to remove existing socket: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to listen on socket: %v", err)
	}
	defer listener.Close()

	log.Printf("Listening on socket: %s", socketPath)

	// Handle interrupt signals for graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// Goroutine to accept connections, main code waits for interrupts
	go func() {
		for {
			select {
			case done := <-ctx.Done():
				log.Printf("Context cancelled: %v", done)
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					if netError, ok := err.(net.Error); ok && netError.Timeout() {
						continue
					}
					log.Printf("Accept error: %v", err)
					break
				}
				handleConnection(ctx, conn, qbClient, db)
			}
		}
	}()

	<-interrupt
	log.Println("Received interrupt signal. Shutting down...")
	cancel(fmt.Errorf("received interrupt signal"))
}

func handleConnection(ctx context.Context, conn net.Conn, qbClient *qbittorrent.Client, db *sql.DB) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Printf("Read error: %v", err)
		} else {
			log.Println("Connection closed before receiving input")
		}
		return
	}

	input := scanner.Text()
	fields := strings.Fields(input)
	if len(fields) != 2 {
		log.Println("Expected name and hash separated by whitespace. Ignoring.")
		return
	}

	// we assume Fields can't ever return empty strings
	name := fields[0]
	hash := fields[1]
	log.Printf("Received name: %s, hash: %s", name, hash)

	go monitorTorrent(name, hash, qbClient, ctx, db)
}

func main() {
	rootCmd = &cobra.Command{
		Use:   "swarmmonitor",
		Short: "SwarmMonitor is a tool to monitor torrent swarms",
		Run:   run,
	}

	rootCmd.Flags().StringVarP(&configPath, "config", "c", configPath, "Path to config file")
	rootCmd.Flags().StringVar(&socketPath, "socket", "", "Unix socket path")
	rootCmd.Flags().StringVar(&qBitAddr, "qbit-addr", "", "qBittorrent host")
	rootCmd.Flags().StringVar(&qBitPort, "qbit-port", "", "qBittorrent webui port")
	rootCmd.Flags().StringVar(&qBitUsername, "qbit-username", "", "qBittorrent username")
	rootCmd.Flags().StringVar(&qBitPassword, "qbit-password", "", "qBittorrent password")

	if err := rootCmd.Execute(); err != nil {
		log.Printf("Error executing rootCmd: %v", err)
		os.Exit(1)
	}
}

func loadConfig(configPath string) {
	viper.SetConfigFile(configPath)
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("No config file found or error reading config file: %v\n", err)
		return
	}
	log.Printf("Using config file: %s\n", viper.ConfigFileUsed())

	flags := map[string]*string{
		"socket":        &socketPath,
		"qbit-addr":     &qBitAddr,
		"qbit-port":     &qBitPort,
		"qbit-username": &qBitUsername,
		"qbit-password": &qBitPassword,
	}

	for flag, ptr := range flags {
		if rootCmd.Flags().Changed(flag) {
			continue
		}
		if v := viper.GetString(flag); v != "" {
			*ptr = v
		}
	}
}
