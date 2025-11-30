package pgserver

import (
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
)

// ProfilingServiceInterface defines the interface for profiling services
type ProfilingServiceInterface interface {
	Start() error
	Stop() error
}

// NewProfilingService creates a new profiling service
func NewProfilingService(port string) ProfilingServiceInterface {
	return components.NewProfilingService(port)
}