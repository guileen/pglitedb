package pgserver

import (
	"context"
	"log"
	"net/http"
	"net/http/pprof"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/guileen/pglitedb/logger"
)

// ProfilingService handles HTTP profiling endpoints
type ProfilingService struct {
	httpServer *http.Server
	port       string
}

// NewProfilingService creates a new profiling service
func NewProfilingService(port string) *ProfilingService {
	return &ProfilingService{
		port: port,
	}
}

// Start starts the profiling HTTP server
func (ps *ProfilingService) Start() error {
	if ps.port == "" {
		logger.Info("Profiling disabled - no port specified")
		return nil
	}
	
	logger.Info("Starting profiling HTTP server", "port", ps.port)
	
	// Setup router
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
	
	// Create HTTP server
	ps.httpServer = &http.Server{
		Addr:    ":" + ps.port,
		Handler: r,
	}
	
	logger.Info("Profiling HTTP server listening", "port", ps.port)
	
	if err := ps.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Profiling HTTP server failed to start", "error", err, "port", ps.port)
		log.Printf("Profiling HTTP server failed to start: %v", err)
		return err
	}
	
	return nil
}

// Stop shuts down the profiling HTTP server
func (ps *ProfilingService) Stop() error {
	if ps.httpServer != nil {
		logger.Info("Shutting down profiling HTTP server...")
		if err := ps.httpServer.Shutdown(context.Background()); err != nil {
			logger.Error("Profiling HTTP server shutdown failed", "error", err)
			return err
		}
		logger.Info("Profiling HTTP server shutdown complete")
	}
	return nil
}