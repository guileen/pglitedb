package profiling

import (
	"context"
	"log"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// Service handles HTTP profiling endpoints
type Service struct {
	httpServer *http.Server
	port       string
	startTime  time.Time
}

// NewService creates a new profiling service
func NewService(port string) interfaces.ProfilingServiceInterface {
	return &Service{
		port:      port,
		startTime: time.Now(),
	}
}

// Start starts the profiling HTTP server
func (ps *Service) Start() error {
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
func (ps *Service) Stop() error {
	if ps.httpServer != nil {
		logger.Info("Shutting down profiling HTTP server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		if err := ps.httpServer.Shutdown(ctx); err != nil {
			logger.Error("Profiling HTTP server shutdown failed", "error", err)
			return err
		}
		logger.Info("Profiling HTTP server shutdown complete")
	}
	return nil
}

// HealthCheck performs a health check on the profiling service
func (ps *Service) HealthCheck() error {
	// Perform any necessary health checks
	return nil
}

// GetStatus returns the current status of the profiling service
func (ps *Service) GetStatus() map[string]interface{} {
	status := make(map[string]interface{})
	status["enabled"] = ps.port != ""
	status["port"] = ps.port
	status["start_time"] = ps.startTime
	status["uptime"] = time.Since(ps.startTime).String()
	return status
}

// CollectData collects profiling data
func (ps *Service) CollectData(ctx context.Context) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	
	// Add basic service information
	data["status"] = ps.GetStatus()
	
	// In a real implementation, this would collect actual profiling data
	// For now, we'll just return basic information
	
	return data, nil
}