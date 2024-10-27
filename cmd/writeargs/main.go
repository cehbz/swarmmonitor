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
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <filename> [args...]\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}

	filename := os.Args[1]
	args := strings.Join(os.Args[2:], " ") + "\n"

	// For Unix sockets, we need to establish a connection first
	conn, err := net.Dial("unix", filename)
	if err != nil {
		// Fall back to regular file write if not a socket
		err = os.WriteFile(filename, []byte(args), 0644)
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	defer conn.Close()

	// Write to socket
	_, err = conn.Write([]byte(args))
	if err != nil {
		log.Fatal(err)
	}
}
