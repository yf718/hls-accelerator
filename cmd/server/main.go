package main

import (
	"log"
	"os"

	"hls-accelerator/internal/config"
	"hls-accelerator/internal/proxy"
)

func main() {
	// Optional: Load config from file if exists
	if err := config.LoadConfig("config.json"); err != nil {
		log.Println("Note: config.json not found or invalid, using defaults")
	}

	// Create cache dir if not exists
	if err := os.MkdirAll(config.GlobalConfig.CacheDir, 0755); err != nil {
		log.Fatalf("Failed to create cache directory: %v", err)
	}

	server, err := proxy.NewServer()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
}
