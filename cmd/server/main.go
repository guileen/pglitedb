package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/guileen/pqlitedb/api"
	"github.com/guileen/pqlitedb/executor"
	"github.com/guileen/pqlitedb/manager"
	"github.com/guileen/pqlitedb/engine"
	"github.com/guileen/pqlitedb/pgserver"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "pg" {
		startPostgreSQLServer()
	} else {
		startHTTPServer()
	}
}

func startHTTPServer() {
	// Create database components
	mgr := manager.NewManager()
	eng := engine.NewStorageEngine()
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

func startPostgreSQLServer() {
	// Create database components
	mgr := manager.NewManager()
	eng := engine.NewStorageEngine()
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