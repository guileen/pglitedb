package types

import (
	"os"
	"strconv"
)

// MemoryPoolConfig holds configuration for the memory pool
type MemoryPoolConfig struct {
	// Initial capacities for different pool types
	RecordInitialCap        int
	ResultSetInitialCap     int
	ValueSliceInitialCap    int
	StringSliceInitialCap   int
	InterfaceSliceInitialCap int
	
	// Adaptive sizing parameters
	EnableAdaptiveSizing    bool
	ResizeThreshold         float64 // Percentage of hits that trigger resize
	MaxResizeFactor         float64 // Maximum factor by which to resize
	MinResizeFactor         float64 // Minimum factor by which to resize
	
	// Metrics collection
	EnableMetrics           bool
}

// DefaultMemoryPoolConfig returns the default memory pool configuration
func DefaultMemoryPoolConfig() MemoryPoolConfig {
	return MemoryPoolConfig{
		RecordInitialCap:         16,
		ResultSetInitialCap:      16,
		ValueSliceInitialCap:     16,
		StringSliceInitialCap:    8,
		InterfaceSliceInitialCap: 16,
		EnableAdaptiveSizing:     true,
		ResizeThreshold:          0.8, // Resize when hit rate is above 80%
		MaxResizeFactor:          2.0,
		MinResizeFactor:          0.5,
		EnableMetrics:            true,
	}
}

// LoadMemoryPoolConfig loads configuration from environment variables
func LoadMemoryPoolConfig() MemoryPoolConfig {
	config := DefaultMemoryPoolConfig()
	
	// Load initial capacities
	if recordCap := os.Getenv("MEMORY_POOL_RECORD_CAP"); recordCap != "" {
		if cap, err := strconv.Atoi(recordCap); err == nil && cap > 0 {
			config.RecordInitialCap = cap
		}
	}
	
	if resultSetCap := os.Getenv("MEMORY_POOL_RESULT_SET_CAP"); resultSetCap != "" {
		if cap, err := strconv.Atoi(resultSetCap); err == nil && cap > 0 {
			config.ResultSetInitialCap = cap
		}
	}
	
	if valueSliceCap := os.Getenv("MEMORY_POOL_VALUE_SLICE_CAP"); valueSliceCap != "" {
		if cap, err := strconv.Atoi(valueSliceCap); err == nil && cap > 0 {
			config.ValueSliceInitialCap = cap
		}
	}
	
	if stringSliceCap := os.Getenv("MEMORY_POOL_STRING_SLICE_CAP"); stringSliceCap != "" {
		if cap, err := strconv.Atoi(stringSliceCap); err == nil && cap > 0 {
			config.StringSliceInitialCap = cap
		}
	}
	
	if interfaceSliceCap := os.Getenv("MEMORY_POOL_INTERFACE_SLICE_CAP"); interfaceSliceCap != "" {
		if cap, err := strconv.Atoi(interfaceSliceCap); err == nil && cap > 0 {
			config.InterfaceSliceInitialCap = cap
		}
	}
	
	// Load adaptive sizing config
	if enableAdaptive := os.Getenv("MEMORY_POOL_ENABLE_ADAPTIVE"); enableAdaptive != "" {
		if enable, err := strconv.ParseBool(enableAdaptive); err == nil {
			config.EnableAdaptiveSizing = enable
		}
	}
	
	if resizeThreshold := os.Getenv("MEMORY_POOL_RESIZE_THRESHOLD"); resizeThreshold != "" {
		if threshold, err := strconv.ParseFloat(resizeThreshold, 64); err == nil && threshold > 0 && threshold <= 1 {
			config.ResizeThreshold = threshold
		}
	}
	
	if maxResizeFactor := os.Getenv("MEMORY_POOL_MAX_RESIZE_FACTOR"); maxResizeFactor != "" {
		if factor, err := strconv.ParseFloat(maxResizeFactor, 64); err == nil && factor > 0 {
			config.MaxResizeFactor = factor
		}
	}
	
	if minResizeFactor := os.Getenv("MEMORY_POOL_MIN_RESIZE_FACTOR"); minResizeFactor != "" {
		if factor, err := strconv.ParseFloat(minResizeFactor, 64); err == nil && factor > 0 {
			config.MinResizeFactor = factor
		}
	}
	
	// Load metrics config
	if enableMetrics := os.Getenv("MEMORY_POOL_ENABLE_METRICS"); enableMetrics != "" {
		if enable, err := strconv.ParseBool(enableMetrics); err == nil {
			config.EnableMetrics = enable
		}
	}
	
	return config
}