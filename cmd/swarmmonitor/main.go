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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cehbz/qbittorrent"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type TorrentNotification struct {
	Hash string
	ID   int
}

var newTorrentChan chan TorrentNotification

var (
	socketPath   string = fmt.Sprintf("/run/user/%d/swarmmonitor.sock", os.Getuid())
	qBitAddr     string = "127.0.0.1"
	qBitPort     string = "8080"
	qBitUsername string = "admin"
	qBitPassword string = "adminpass"
	pgSocketDir  string = "/var/run/postgresql"
	pgPort       string = "5432"
	pgDatabase   string = "swarmmonitor"
	pgUser       string = "postgres"
	pgPassword   string = "postgrespass"
	configPath   string = os.Getenv("HOME") + "/.config/swarmmonitor/config.toml"
)

var rootCmd *cobra.Command

func initDB() *sql.DB {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		pgUser, pgPassword, pgDatabase, pgSocketDir, pgPort)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS torrents (
		id SERIAL PRIMARY KEY,
		name TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE
	);

	CREATE TABLE IF NOT EXISTS peers (
		id SERIAL PRIMARY KEY,
		ip TEXT NOT NULL,
		port INTEGER NOT NULL,
		country_code TEXT,
		client TEXT,
		UNIQUE(ip, port)
	);

	CREATE TABLE IF NOT EXISTS peer_metrics (
		timestamp TIMESTAMP NOT NULL,
		peer_id INTEGER NOT NULL,
		torrent_id INTEGER NOT NULL,
		uploaded BIGINT NOT NULL,
		downloaded BIGINT NOT NULL,
		pct_complete REAL NOT NULL,
		FOREIGN KEY (peer_id) REFERENCES peers(id),
		FOREIGN KEY (torrent_id) REFERENCES torrents(id)
	);

	CREATE TABLE IF NOT EXISTS main_metrics (
		timestamp TIMESTAMP NOT NULL,
		torrent_id INTEGER NOT NULL,
		uploaded BIGINT NOT NULL,
		downloaded BIGINT NOT NULL,
		FOREIGN KEY (torrent_id) REFERENCES torrents(id)
	);
	`
	_, err = db.Exec(schema)
	if err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}
	return db
}

func monitorTorrent(hash, _ /* tracker */, name string, client *qbittorrent.Client, ctx context.Context, db *sql.DB) {
	var torrentID int
	if err := db.QueryRow(`
		INSERT INTO torrents (hash, name)
		VALUES ($1, $2)
		ON CONFLICT(hash) DO UPDATE SET name = excluded.name
		RETURNING id`, hash, name).Scan(&torrentID); err != nil {
		log.Printf("Error inserting or getting torrent ID: %v", err)
		return
	}

	select {
	case newTorrentChan <- TorrentNotification{Hash: hash, ID: torrentID}:
	default:
		log.Printf("Warning: newTorrentChan is full, couldn't notify about new torrent %s", hash)
	}

	// First phase: every second for 10 minutes
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	phase2 := time.After(10 * time.Minute)
	done := time.After(30 * time.Minute)

	rid := 0
	lastUploaded := make(map[int]int64)
	lastDownloaded := make(map[int]int64)

	for {
		select {
		case <-ticker.C:
			if err := updatePeerMetrics(client, db, hash, torrentID, &rid, lastUploaded, lastDownloaded); err != nil {
				log.Printf("Error updating peer metrics: %v", err)
			}

		case <-phase2:
			ticker.Reset(5 * time.Second)

		case <-done:
			log.Printf("Monitoring complete for hash: %s", hash)
			return

		case <-ctx.Done():
			log.Printf("Stopping monitor for hash: %s", hash)
			return
		}
	}
}

func updatePeerMetrics(client *qbittorrent.Client, db *sql.DB, hash string, torrentID int,
	rid *int, lastUploaded, lastDownloaded map[int]int64) error {

	peers, err := client.SyncTorrentPeers(hash, *rid)
	if err != nil {
		return fmt.Errorf("getting peer info: %w", err)
	}
	*rid = peers.Rid

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback()

	ts := time.Now().UTC()
	for _, peer := range peers.Peers {
		var peerID int
		err := tx.QueryRow(`
            INSERT INTO peers (ip, port, client)
            VALUES ($1, $2, $3)
            ON CONFLICT(ip, port) DO UPDATE SET client = excluded.client
            RETURNING id`, peer.IP, peer.Port, peer.Client).Scan(&peerID)
		if err != nil {
			log.Printf("Error inserting/getting peer ID: %v", err)
			continue
		}

		uploaded := max(0, peer.Uploaded-lastUploaded[peerID])
		downloaded := max(0, peer.Downloaded-lastDownloaded[peerID])

		_, err = tx.Exec(`
            INSERT INTO peer_metrics (timestamp, peer_id, torrent_id, uploaded, downloaded, pct_complete)
            VALUES ($1, $2, $3, $4, $5, $6)`,
			ts.Format(time.RFC3339), peerID, torrentID, uploaded, downloaded, peer.Progress)
		if err != nil {
			log.Printf("Error inserting peer metrics: %v", err)
			continue
		}

		log.Printf("%s peer_metrics %40s,%15s:%-5d: %20s %5.3f %12d %12d\n",
			ts.Format(time.Stamp), hash, peer.IP, peer.Port, peer.Client, peer.Progress, downloaded, uploaded)
		lastUploaded[peerID] = peer.Uploaded
		lastDownloaded[peerID] = peer.Downloaded
	}

	return tx.Commit()
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
			err = tx.QueryRow(`
				INSERT INTO torrents (hash, name)
				VALUES ($1, $2)
				ON CONFLICT (hash)
				DO UPDATE SET name = excluded.name
				RETURNING id
			`, hash, torrent.Name).Scan(&id)
			if err != nil {
				log.Fatalf("Error inserting torrent %s: %v", hash, err)
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
							VALUES ($1, $2, $3, $4)`,
							ts.Format(time.RFC3339), hash_ids[hash], uploaded, downloaded)
						if err != nil {
							log.Printf("Error inserting main metrics (%s): %v", hash, err)
							continue
						}
						log.Printf("%s main_metrics %40s: %12d %12d\n", ts.Format(time.Stamp), hash, uploaded, downloaded)

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
	// Set up logging
	logPath := filepath.Join(os.Getenv("HOME"), "logs")
	if err := os.MkdirAll(logPath, 0755); err != nil {
		log.Fatal(err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(logPath, "swarmmonitor.log"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

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

	log.Println("Starting periodic check")
	go periodicCheck(qbClient, ctx, db, newTorrentChan)

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed to remove existing socket: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to listen on socket: %v", err)
	}
	defer listener.Close()

	// Handle interrupt signals for graceful shutdown
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	log.Printf("Listening on %s", socketPath)
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
	fields := strings.SplitN(input, " ", 3)
	if len(fields) < 3 {
		log.Println("Expected hash, tracker, and name separated by whitespace. Ignoring.")
		return
	}

	hash := fields[0]
	tracker := fields[1]
	name := fields[2]
	log.Printf("Received hash: %s, tracker: %s, name: %s", hash, tracker, name)

	go monitorTorrent(hash, tracker, name, qbClient, ctx, db)
}

func main() {
	rootCmd = &cobra.Command{
		Use:   "swarmmonitor",
		Short: "SwarmMonitor is a tool to monitor torrent swarms",
		Run:   run,
	}

	// Define flags with literal defaults
	rootCmd.Flags().StringVarP(&configPath, "config", "c", os.Getenv("HOME")+"/.config/swarmmonitor/config.toml", "Path to config file")
	rootCmd.Flags().StringVar(&socketPath, "socket", fmt.Sprintf("/run/user/%d/swarmmonitor.sock", os.Getuid()), "Unix socket path")
	rootCmd.Flags().StringVar(&qBitAddr, "qbit-addr", "127.0.0.1", "qBittorrent host")
	rootCmd.Flags().StringVar(&qBitPort, "qbit-port", "8080", "qBittorrent webui port")
	rootCmd.Flags().StringVar(&qBitUsername, "qbit-username", "admin", "qBittorrent username")
	rootCmd.Flags().StringVar(&qBitPassword, "qbit-password", "adminpass", "qBittorrent password")
	rootCmd.Flags().StringVar(&pgSocketDir, "pg-socket-dir", "/var/run/postgresql", "PostgreSQL Unix socket directory")
	rootCmd.Flags().StringVar(&pgPort, "pg-port", "5432", "PostgreSQL port")
	rootCmd.Flags().StringVar(&pgDatabase, "pg-database", "swarmmonitor", "PostgreSQL database name")
	rootCmd.Flags().StringVar(&pgUser, "pg-user", "postgres", "PostgreSQL user")
	rootCmd.Flags().StringVar(&pgPassword, "pg-password", "postgrespass", "PostgreSQL password")

	// Parse flags
	if err := rootCmd.Execute(); err != nil {
		log.Printf("Error executing rootCmd: %v", err)
		os.Exit(1)
	}

	// Now load config and only override values that weren't explicitly set via flags
	loadConfig(configPath)
}

func loadConfig(configPath string) {
	viper.SetConfigFile(configPath)
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("No config file found or error reading config file: %v\n", err)
		return
	}

	// Only update values that are still set to their flag defaults
	flags := map[string]*string{
		"socket":        &socketPath,
		"qbit-addr":     &qBitAddr,
		"qbit-port":     &qBitPort,
		"qbit-username": &qBitUsername,
		"qbit-password": &qBitPassword,
		"pg-socket-dir": &pgSocketDir,
		"pg-port":       &pgPort,
		"pg-database":   &pgDatabase,
		"pg-user":       &pgUser,
		"pg-password":   &pgPassword,
	}

	for flag, ptr := range flags {
		if !rootCmd.Flags().Changed(flag) {
			if v := viper.GetString(flag); v != "" {
				*ptr = v
			}
		}
	}
}
