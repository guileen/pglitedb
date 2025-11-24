package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/guileen/pglitedb/protocol/api"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/protocol/executor"
	"github.com/guileen/pglitedb/storage"
	
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/protocol/pgserver"
)

func main() {
	dbPath := "/tmp/pglitedb"
	if len(os.Args) > 1 {
		// If first argument is not a flag or command, use it as db path
		if os.Args[1] != "pg" && os.Args[1][0] != '-' {
			dbPath = os.Args[1]
			// Remove the first argument (dbPath) from os.Args
			os.Args = append(os.Args[:1], os.Args[2:]...)
		}
	}
	
	if len(os.Args) > 1 && os.Args[1] == "pg" {
		startPostgreSQLServer(dbPath)
	} else {
		startHTTPServer(dbPath)
	}
}

func startHTTPServer(dbPath string) {
	// Create database components
	// Create a pebble KV store
	config := storage.DefaultPebbleConfig(dbPath + "-http")
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		log.Fatalf("failed to create pebble kv: %v", err)
	}
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine and manager
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManager(eng)
	exec := executor.NewExecutor(mgr, eng)

	// Create REST handler
	restHandler := api.NewRESTHandler(exec)

	// Setup router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Register API routes
	restHandler.RegisterRoutes(r)

	// Start server
	port := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		port = ":" + p
	}

	server := &http.Server{
		Addr:    port,
		Handler: r,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Starting HTTP server on %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Shutdown server gracefully
	log.Println("Shutting down HTTP server...")
	if err := server.Shutdown(context.Background()); err != nil {
		log.Fatalf("HTTP server shutdown failed: %v", err)
	}
	log.Println("HTTP server shutdown complete")
}

func startPostgreSQLServer(dbPath string) {
	// Create database components
	// Create a pebble KV store
	kvStore, err := storage.NewPebbleKV(storage.DefaultPebbleConfig(dbPath + "-postgres"))
	if err != nil {
		log.Fatalf("failed to create pebble kv: %v", err)
	}
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine and manager
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManager(eng)
	exec := executor.NewExecutor(mgr, eng)

	// Create PostgreSQL server
	server := pgserver.NewPostgreSQLServer(exec)

	// Start server in a goroutine
	port := "5432"
	if p := os.Getenv("PG_PORT"); p != "" {
		port = p
	}

	go func() {
		if err := server.Start(port); err != nil {
			log.Fatalf("PostgreSQL server failed: %v", err)
		}
	}()

	log.Printf("PostgreSQL server started on port %s", port)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Shutdown server gracefully
	log.Println("Shutting down PostgreSQL server...")
	if err := server.Close(); err != nil {
		log.Printf("Error shutting down server: %v", err)
	}
	log.Println("PostgreSQL server shutdown complete")
}
