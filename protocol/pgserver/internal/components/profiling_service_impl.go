package components

import (
	"context"
	"log"
	"net/http"
	"net/http/pprof"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// Ensure ProfilingService implements ProfilingServiceInterface
var _ interfaces.ProfilingServiceInterface = &ProfilingService{}

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
		logger.Info("Profiling disabled (no port specified)")
		return nil
	}

	logger.Info("Starting profiling HTTP server...", "port", ps.port)
	
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	
	// Register pprof handlers
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	
	ps.httpServer = &http.Server{
		Addr:    ":" + ps.port,
		Handler: r,
	}
	
	go func() {
		if err := ps.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Profiling HTTP server error: %v", err)
		}
	}()
	
	logger.Info("Profiling HTTP server started", "port", ps.port)
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

// HealthCheck performs a health check on the profiling service
func (ps *ProfilingService) HealthCheck() error {
	// Perform any necessary health checks
	return nil
}

// GetStatus returns the status of the profiling service
func (ps *ProfilingService) GetStatus() map[string]interface{} {
	status := make(map[string]interface{})
	status["port"] = ps.port
	status["enabled"] = ps.port != ""
	// Add more status information as needed
	return status
}

// CollectData collects profiling data
func (ps *ProfilingService) CollectData(ctx context.Context) (map[string]interface{}, error) {
	// Collect profiling data
	data := make(map[string]interface{})
	data["status"] = ps.GetStatus()
	// Add more profiling data as needed
	return data, nil
}