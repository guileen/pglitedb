package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver"
	"github.com/guileen/pglitedb/storage"
)

func main() {
	var (
		port      = flag.String("port", "5432", "TCP port to listen on")
		socket    = flag.String("socket", "", "Unix socket path to listen on")
		dataDir   = flag.String("data-dir", "./data", "Data directory path")
		initOnly  = flag.Bool("init-only", false, "Initialize database and exit")
	)
	
	flag.Parse()
	
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}
	
	// Initialize database components
	dbPath := filepath.Join(*dataDir, "db")
	
	// Create a pebble KV store
	config := storage.DefaultPebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		log.Fatalf("Failed to create pebble kv: %v", err)
	}
	defer kvStore.Close()
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine and manager
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)
	
	// Load existing schemas
	if err := mgr.LoadSchemas(context.Background()); err != nil {
		log.Printf("Warning: failed to load schemas: %v", err)
	}
	
	// Create executor
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	exec := planner.Executor()
	
	// Create PostgreSQL server
	server := pgserver.NewPostgreSQLServer(exec, planner)
	
	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Println("Shutting down server...")
		server.Close()
		os.Exit(0)
	}()
	
	// Initialize only if requested
	if *initOnly {
		log.Println("Database initialized successfully")
		return
	}
	
	// Start server
	if *socket != "" {
		log.Printf("Starting server on Unix socket: %s", *socket)
		if err := server.StartUnix(*socket); err != nil {
			log.Fatalf("Failed to start server on Unix socket: %v", err)
		}
	} else {
		log.Printf("Starting server on TCP port: %s", *port)
		if err := server.StartTCP(*port); err != nil {
			log.Fatalf("Failed to start server on TCP port: %v", err)
		}
	}
}