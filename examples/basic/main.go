package main

import (
	"fmt"
	"log"

	"github.com/tsmask/redka"
)

func main() {
	// Start Redka server in a goroutine.
	// Uses SQLite in-memory database by default.
	ready, srv := redka.StartAsync(":6380", ":memory:")
	if err := <-ready; err != nil {
		log.Fatalf("failed to start redka: %v", err)
	}
	fmt.Println("redka server started on :6380")

	// Wait for shutdown signal (Ctrl+C or SIGTERM).
	srv.WaitForShutdown()
}
