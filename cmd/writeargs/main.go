package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	// Set up logging
	logPath := filepath.Join(os.Getenv("HOME"), "logs")
	if err := os.MkdirAll(logPath, 0755); err != nil {
		log.Fatal(err)
	}

	logFile, err := os.OpenFile(
		filepath.Join(logPath, "writeargs.log"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	logger := log.New(logFile, "", log.LstdFlags)

	// Log invocation
	logger.Printf("%#v\n", os.Args)

	if len(os.Args) < 3 {
		logger.Printf("Error: insufficient arguments\n")
		fmt.Fprintf(os.Stderr, "Usage: %s <filename> [args...]\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}

	filename := os.Args[1]
	args := strings.Join(os.Args[2:], " ") + "\n"

	// For Unix sockets, we need to establish a connection first
	if conn, err := net.Dial("unix", filename); err == nil {
		defer conn.Close()
		if _, err := conn.Write([]byte(args)); err != nil {
			logger.Printf("Error writing to socket %s: %v\n", filename, err)
			log.Fatal(err)
		}
		logger.Printf("%s: %s", filename, args)
		return
	}

	// Fall back to regular file write if not a socket
	if err := os.WriteFile(filename, []byte(args), 0644); err != nil {
		logger.Printf("Error writing to file %s: %v\n", filename, err)
		log.Fatal(err)
	}
	logger.Printf("%s: %s", filename, args)
}
