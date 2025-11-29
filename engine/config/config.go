package config

import (
	"os"
	"strconv"
	"time"
)

// EngineConfig holds configuration for the engine
type EngineConfig struct {
	// Leak detection configuration
	LeakDetectionThreshold time.Duration
	
	// Resource pool configuration
	BufferPoolInitialSize int
	BufferPoolMaxSize     int
	
	// Performance thresholds
	BatchOperationThreshold int
	IteratorBatchSize       int
	
	// Memory management
	EnableAdaptiveMemorySizing bool
	MemoryResizeThreshold     float64
}

// DefaultEngineConfig returns the default engine configuration
func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		LeakDetectionThreshold:     5 * time.Minute,
		BufferPoolInitialSize:      500,
		BufferPoolMaxSize:          50000,
		BatchOperationThreshold:    200,
		IteratorBatchSize:          2000,
		EnableAdaptiveMemorySizing: true,
		MemoryResizeThreshold:      0.7,
	}
}

// LoadEngineConfig loads configuration from environment variables
func LoadEngineConfig() EngineConfig {
	config := DefaultEngineConfig()
	
	// Load leak detection threshold
	if leakThresholdStr := os.Getenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS"); leakThresholdStr != "" {
		if ms, err := strconv.ParseInt(leakThresholdStr, 10, 64); err == nil && ms > 0 {
			config.LeakDetectionThreshold = time.Duration(ms) * time.Millisecond
		}
	}
	
	// Load buffer pool sizes
	if initialSizeStr := os.Getenv("ENGINE_BUFFER_POOL_INITIAL_SIZE"); initialSizeStr != "" {
		if size, err := strconv.Atoi(initialSizeStr); err == nil && size > 0 {
			config.BufferPoolInitialSize = size
		}
	}
	
	if maxSizeStr := os.Getenv("ENGINE_BUFFER_POOL_MAX_SIZE"); maxSizeStr != "" {
		if size, err := strconv.Atoi(maxSizeStr); err == nil && size > 0 {
			config.BufferPoolMaxSize = size
		}
	}
	
	// Load performance thresholds
	if batchThresholdStr := os.Getenv("ENGINE_BATCH_OPERATION_THRESHOLD"); batchThresholdStr != "" {
		if threshold, err := strconv.Atoi(batchThresholdStr); err == nil && threshold > 0 {
			config.BatchOperationThreshold = threshold
		}
	}
	
	if batchSizeStr := os.Getenv("ENGINE_ITERATOR_BATCH_SIZE"); batchSizeStr != "" {
		if size, err := strconv.Atoi(batchSizeStr); err == nil && size > 0 {
			config.IteratorBatchSize = size
		}
	}
	
	// Load memory management config
	if enableAdaptiveStr := os.Getenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING"); enableAdaptiveStr != "" {
		if enable, err := strconv.ParseBool(enableAdaptiveStr); err == nil {
			config.EnableAdaptiveMemorySizing = enable
		}
	}
	
	if resizeThresholdStr := os.Getenv("ENGINE_MEMORY_RESIZE_THRESHOLD"); resizeThresholdStr != "" {
		if threshold, err := strconv.ParseFloat(resizeThresholdStr, 64); err == nil && threshold > 0 && threshold <= 1 {
			config.MemoryResizeThreshold = threshold
		}
	}
	
	return config
}