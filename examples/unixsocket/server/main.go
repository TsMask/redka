package main

import (
	"log"

	"github.com/tsmask/redka"
	"github.com/tsmask/redka/config"
)

func main() {
	// Unix socket configuration example.
	cfg := config.DefaultConfig()
	cfg.Sock = "/tmp/redka.sock"
	cfg.DBDSN = "sqlite:/tmp/redka_unixsocket.sqlite?vfs=memdb"
	cfg.MaxClients = 5000

	// Start with Unix socket.
	ready, srv := redka.StartAsyncWithConfig("", cfg.DBDSN, cfg)
	err := <-ready
	if err != nil {
		log.Fatalf("failed to start redka: %v", err)
	}
	log.Printf("server ready, network=%s addr=%s", cfg.Network(), cfg.Address())
	log.Println("redka server started on unix socket:", cfg.Sock)

	srv.WaitForShutdown()
}
