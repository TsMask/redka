package main

import (
	"log"

	"github.com/tsmask/redka"
	"github.com/tsmask/redka/config"
)

func main() {
	// Custom configuration example.
	cfg := config.DefaultConfig()
	cfg.Port = 6380
	cfg.Password = "secret"
	cfg.MaxClients = 5000
	cfg.SlowlogMaxLen = 256
	cfg.SlowlogThreshold = 2000 // ms
	cfg.CleanupInterval = 30    // seconds

	dbdsn := "sqlite:/tmp/redka.sqlite?vfs=memdb"

	// Start with DBDSN.
	ready, srv := redka.StartAsyncWithConfig(":6380", dbdsn, cfg)
	if err := <-ready; err != nil {
		log.Fatalf("failed to start redka: %v", err)
	}
	log.Println("redka server started on :6380 with DBDSN")

	srv.WaitForShutdown()
}
