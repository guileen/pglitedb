package main

import (
	"context"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/guileen/pglitedb/protocol/api"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
	
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/protocol/pgserver"
	"github.com/guileen/pglitedb/logger"
)

func main() {
	// Initialize logger
	startTime := time.Now()
	logger.Info("Starting PGLiteDB server", "startup_time", startTime.Format(time.RFC3339))
	
	dbPath := "/tmp/pglitedb"
	if len(os.Args) > 1 {
		// If first argument is not a flag or command, use it as db path
		if os.Args[1] != "pg" && os.Args[1][0] != '-' {
			dbPath = os.Args[1]
			// Remove the first argument (dbPath) from os.Args
			os.Args = append(os.Args[:1], os.Args[2:]...)
		}
	}
	
	logger.Info("Database path configured", "dbPath", dbPath)
	
	if len(os.Args) > 1 && os.Args[1] == "pg" {
		startPostgreSQLServer(dbPath)
	} else {
		startHTTPServer(dbPath)
	}
	
	duration := time.Since(startTime)
	logger.Info("PGLiteDB server process completed", "total_duration", duration.String())
}

func startHTTPServer(dbPath string) {
	startTime := time.Now()
	logger.Info("Initializing HTTP server", "dbPath", dbPath, "start_time", startTime.Format(time.RFC3339))
	
	// Create database components
	logger.Info("Creating Pebble KV store", "dbPath", dbPath+"-http")
	config := storage.DefaultPebbleConfig(dbPath + "-http")
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		logger.Error("Failed to create pebble kv", "error", err, "dbPath", dbPath+"-http")
		log.Fatalf("failed to create pebble kv: %v", err)
	}
	logger.Info("Pebble KV store created successfully")
	
	// Create codec
	logger.Info("Creating MemComparableCodec")
	c := codec.NewMemComparableCodec()
	logger.Info("MemComparableCodec created successfully")
	
	// Create engine and manager
	logger.Info("Creating PebbleEngine")
	eng := engine.NewStorageEngine(kvStore, c)
	logger.Info("PebbleEngine created successfully")
	
	logger.Info("Creating TableManager")
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)
	logger.Info("TableManager created successfully")
	
	// Load existing schemas
	logger.Info("Loading existing schemas")
	loadStart := time.Now()
	if err := mgr.LoadSchemas(context.Background()); err != nil {
		logger.Warn("Failed to load schemas", "error", err)
		logger.Info("Warning: failed to load schemas", "error", err)
	} else {
		loadDuration := time.Since(loadStart)
		logger.Info("Schema loading completed", "duration", loadDuration.String())
	}
	
	logger.Info("Creating SQL parser and planner")
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	exec := planner.Executor()
	logger.Info("SQL parser and planner created successfully")

	// Create REST handler
	logger.Info("Creating REST handler")
	restHandler := api.NewRESTHandler(exec, planner)
	logger.Info("REST handler created successfully")

	// Setup router
	logger.Info("Setting up router")
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Register pprof handlers for profiling
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))
	r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))

	// Register API routes
	restHandler.RegisterRoutes(r)

	// Start server
	port := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		port = ":" + p
	}

	logger.Info("Creating HTTP server", "port", port)
	server := &http.Server{
		Addr:    port,
		Handler: r,
	}

	// Start server in a goroutine
	go func() {
		logger.Info("Starting HTTP server", "port", port)
		logger.Info("HTTP server listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed to start", "error", err, "port", port)
			log.Fatalf("HTTP server failed to start: %v", err)
		}
	}()
	
	initDuration := time.Since(startTime)
	logger.Info("HTTP server initialization complete", "init_duration", initDuration.String())
	logger.Info("HTTP server initialized", "init_duration", initDuration.String())

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Shutdown server gracefully
	shutdownStart := time.Now()
	logger.Info("Shutting down HTTP server...", "shutdown_start", shutdownStart.Format(time.RFC3339))
	logger.Info("Shutting down HTTP server...")
	if err := server.Shutdown(context.Background()); err != nil {
		logger.Error("HTTP server shutdown failed", "error", err)
		log.Fatalf("HTTP server shutdown failed: %v", err)
	}
	shutdownDuration := time.Since(shutdownStart)
	logger.Info("HTTP server shutdown complete", "shutdown_duration", shutdownDuration.String())
	logger.Info("HTTP server shutdown complete", "shutdown_duration", shutdownDuration.String())
}

func startPostgreSQLServer(dbPath string) {
	startTime := time.Now()
	logger.Info("Initializing PostgreSQL server", "dbPath", dbPath, "start_time", startTime.Format(time.RFC3339))
	
	// Create database components
	logger.Info("Creating Pebble KV store", "dbPath", dbPath+"-postgres")
	kvConfig := storage.DefaultPebbleConfig(dbPath + "-postgres")
	kvStore, err := storage.NewPebbleKV(kvConfig)
	if err != nil {
		logger.Error("Failed to create pebble kv", "error", err, "dbPath", dbPath+"-postgres")
		log.Fatalf("failed to create pebble kv: %v", err)
	}
	logger.Info("Pebble KV store created successfully")
	
	// Create codec
	logger.Info("Creating MemComparableCodec")
	c := codec.NewMemComparableCodec()
	logger.Info("MemComparableCodec created successfully")
	
	// Create engine and manager
	logger.Info("Creating PebbleEngine")
	eng := engine.NewStorageEngine(kvStore, c)
	logger.Info("PebbleEngine created successfully")
	
	logger.Info("Creating TableManager")
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)
	logger.Info("TableManager created successfully")
	
	// Load existing schemas
	logger.Info("Loading existing schemas")
	loadStart := time.Now()
	if err := mgr.LoadSchemas(context.Background()); err != nil {
		logger.Warn("Failed to load schemas", "error", err)
		logger.Info("Warning: failed to load schemas", "error", err)
	} else {
		loadDuration := time.Since(loadStart)
		logger.Info("Schema loading completed", "duration", loadDuration.String())
	}
	
	logger.Info("Creating SQL parser and planner")
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	exec := planner.Executor()
	logger.Info("SQL parser and planner created successfully")
	
	// Create PostgreSQL server
	logger.Info("Creating PostgreSQL server instance")
	server := pgserver.NewPostgreSQLServer(exec, planner)
	
	// Enable profiling if PROFILING_PORT is set
	if profilingPort := os.Getenv("PROFILING_PORT"); profilingPort != "" {
		logger.Info("Enabling profiling", "port", profilingPort)
		server.WithProfiling(profilingPort)
	} else {
		// Enable profiling on default port 6060 if not disabled
		if os.Getenv("DISABLE_PROFILING") == "" {
			logger.Info("Enabling profiling on default port", "port", "6060")
			server.WithProfiling("6060")
		} else {
			logger.Info("Profiling disabled by DISABLE_PROFILING environment variable")
			server.WithProfiling("") // Disable profiling
		}
	}
	
	initDuration := time.Since(startTime)
	logger.Info("PostgreSQL server instance created", "init_duration", initDuration.String())
	
	// Start server in a goroutine
	port := "5432"
	if p := os.Getenv("PG_PORT"); p != "" {
		port = p
	}
	
	logger.Info("Starting PostgreSQL server", "port", port)
	go func() {
		logger.Info("PostgreSQL server goroutine started", "port", port)
		if err := server.Start(port); err != nil {
			logger.Error("PostgreSQL server failed to start", "error", err, "port", port)
			log.Fatalf("PostgreSQL server failed: %v", err)
		}
	}()
	
	logger.Info("PostgreSQL server started", "port", port, "startup_duration", time.Since(startTime).String())
	logger.Info("PostgreSQL server started", "port", port)
	
	// Wait for interrupt signal
	logger.Info("Waiting for interrupt signal")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	
	// Shutdown server gracefully
	shutdownStart := time.Now()
	logger.Info("Shutting down PostgreSQL server...", "shutdown_start", shutdownStart.Format(time.RFC3339))
	logger.Info("Shutting down PostgreSQL server...")
	if err := server.Close(); err != nil {
		logger.Error("Error shutting down server", "error", err)
		logger.Error("Error shutting down server", "error", err)
	}
	shutdownDuration := time.Since(shutdownStart)
	logger.Info("PostgreSQL server shutdown complete", "shutdown_duration", shutdownDuration.String())
	logger.Info("PostgreSQL server shutdown complete", "shutdown_duration", shutdownDuration.String())
}