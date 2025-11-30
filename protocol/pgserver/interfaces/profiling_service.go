package interfaces

import (
	"context"
)

// ProfilingServiceInterface defines the interface for profiling services
type ProfilingServiceInterface interface {
	Start() error
	Stop() error
	HealthCheck() error
	GetStatus() map[string]interface{}
	CollectData(ctx context.Context) (map[string]interface{}, error)
}