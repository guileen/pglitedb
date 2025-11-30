package components

// ProfilingServiceInterface defines the interface for profiling services
type ProfilingServiceInterface interface {
	Start() error
	Stop() error
}