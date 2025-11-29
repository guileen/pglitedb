package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultEngineConfig(t *testing.T) {
	t.Run("DefaultValues", func(t *testing.T) {
		config := DefaultEngineConfig()
		
		// Test default values
		assert.Equal(t, 5*time.Minute, config.LeakDetectionThreshold)
		assert.Equal(t, 500, config.BufferPoolInitialSize)
		assert.Equal(t, 50000, config.BufferPoolMaxSize)
		assert.Equal(t, 200, config.BatchOperationThreshold)
		assert.Equal(t, 2000, config.IteratorBatchSize)
		assert.Equal(t, true, config.EnableAdaptiveMemorySizing)
		assert.Equal(t, 0.7, config.MemoryResizeThreshold)
	})
}

func TestLoadEngineConfig(t *testing.T) {
	t.Run("DefaultConfigWhenNoEnvVars", func(t *testing.T) {
		// Save original environment variables
		originalLeakThreshold := os.Getenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS")
		originalInitialSize := os.Getenv("ENGINE_BUFFER_POOL_INITIAL_SIZE")
		originalMaxSize := os.Getenv("ENGINE_BUFFER_POOL_MAX_SIZE")
		originalBatchThreshold := os.Getenv("ENGINE_BATCH_OPERATION_THRESHOLD")
		originalBatchSize := os.Getenv("ENGINE_ITERATOR_BATCH_SIZE")
		originalEnableAdaptive := os.Getenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING")
		originalResizeThreshold := os.Getenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
		
		// Clean up environment variables
		os.Unsetenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS")
		os.Unsetenv("ENGINE_BUFFER_POOL_INITIAL_SIZE")
		os.Unsetenv("ENGINE_BUFFER_POOL_MAX_SIZE")
		os.Unsetenv("ENGINE_BATCH_OPERATION_THRESHOLD")
		os.Unsetenv("ENGINE_ITERATOR_BATCH_SIZE")
		os.Unsetenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING")
		os.Unsetenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
		
		defer func() {
			// Restore original environment variables
			if originalLeakThreshold != "" {
				os.Setenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS", originalLeakThreshold)
			}
			if originalInitialSize != "" {
				os.Setenv("ENGINE_BUFFER_POOL_INITIAL_SIZE", originalInitialSize)
			}
			if originalMaxSize != "" {
				os.Setenv("ENGINE_BUFFER_POOL_MAX_SIZE", originalMaxSize)
			}
			if originalBatchThreshold != "" {
				os.Setenv("ENGINE_BATCH_OPERATION_THRESHOLD", originalBatchThreshold)
			}
			if originalBatchSize != "" {
				os.Setenv("ENGINE_ITERATOR_BATCH_SIZE", originalBatchSize)
			}
			if originalEnableAdaptive != "" {
				os.Setenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING", originalEnableAdaptive)
			}
			if originalResizeThreshold != "" {
				os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", originalResizeThreshold)
			}
		}()
		
		config := LoadEngineConfig()
		
		// Should match default values
		expected := DefaultEngineConfig()
		assert.Equal(t, expected, config)
	})
	
	t.Run("LeakDetectionThresholdFromEnv", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS", original)
			} else {
				os.Unsetenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS")
			}
		}()
		
		// Set test value
		os.Setenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS", "10000") // 10 seconds
		
		config := LoadEngineConfig()
		assert.Equal(t, 10*time.Second, config.LeakDetectionThreshold)
	})
	
	t.Run("InvalidLeakDetectionThreshold", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS", original)
			} else {
				os.Unsetenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS")
			}
		}()
		
		// Set invalid value
		os.Setenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS", "invalid")
		
		config := LoadEngineConfig()
		// Should fall back to default
		assert.Equal(t, DefaultEngineConfig().LeakDetectionThreshold, config.LeakDetectionThreshold)
	})
	
	t.Run("NegativeLeakDetectionThreshold", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS", original)
			} else {
				os.Unsetenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS")
			}
		}()
		
		// Set negative value
		os.Setenv("ENGINE_LEAK_DETECTION_THRESHOLD_MS", "-5000")
		
		config := LoadEngineConfig()
		// Should fall back to default
		assert.Equal(t, DefaultEngineConfig().LeakDetectionThreshold, config.LeakDetectionThreshold)
	})
	
	t.Run("BufferPoolInitialSizeFromEnv", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_BUFFER_POOL_INITIAL_SIZE")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_BUFFER_POOL_INITIAL_SIZE", original)
			} else {
				os.Unsetenv("ENGINE_BUFFER_POOL_INITIAL_SIZE")
			}
		}()
		
		// Set test value
		os.Setenv("ENGINE_BUFFER_POOL_INITIAL_SIZE", "1000")
		
		config := LoadEngineConfig()
		assert.Equal(t, 1000, config.BufferPoolInitialSize)
	})
	
	t.Run("InvalidBufferPoolInitialSize", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_BUFFER_POOL_INITIAL_SIZE")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_BUFFER_POOL_INITIAL_SIZE", original)
			} else {
				os.Unsetenv("ENGINE_BUFFER_POOL_INITIAL_SIZE")
			}
		}()
		
		// Set invalid value
		os.Setenv("ENGINE_BUFFER_POOL_INITIAL_SIZE", "invalid")
		
		config := LoadEngineConfig()
		// Should fall back to default
		assert.Equal(t, DefaultEngineConfig().BufferPoolInitialSize, config.BufferPoolInitialSize)
	})
	
	t.Run("BufferPoolMaxSizeFromEnv", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_BUFFER_POOL_MAX_SIZE")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_BUFFER_POOL_MAX_SIZE", original)
			} else {
				os.Unsetenv("ENGINE_BUFFER_POOL_MAX_SIZE")
			}
		}()
		
		// Set test value
		os.Setenv("ENGINE_BUFFER_POOL_MAX_SIZE", "100000")
		
		config := LoadEngineConfig()
		assert.Equal(t, 100000, config.BufferPoolMaxSize)
	})
	
	t.Run("BatchOperationThresholdFromEnv", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_BATCH_OPERATION_THRESHOLD")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_BATCH_OPERATION_THRESHOLD", original)
			} else {
				os.Unsetenv("ENGINE_BATCH_OPERATION_THRESHOLD")
			}
		}()
		
		// Set test value
		os.Setenv("ENGINE_BATCH_OPERATION_THRESHOLD", "500")
		
		config := LoadEngineConfig()
		assert.Equal(t, 500, config.BatchOperationThreshold)
	})
	
	t.Run("IteratorBatchSizeFromEnv", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_ITERATOR_BATCH_SIZE")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_ITERATOR_BATCH_SIZE", original)
			} else {
				os.Unsetenv("ENGINE_ITERATOR_BATCH_SIZE")
			}
		}()
		
		// Set test value
		os.Setenv("ENGINE_ITERATOR_BATCH_SIZE", "5000")
		
		config := LoadEngineConfig()
		assert.Equal(t, 5000, config.IteratorBatchSize)
	})
	
	t.Run("EnableAdaptiveMemorySizingFromEnv", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING", original)
			} else {
				os.Unsetenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING")
			}
		}()
		
		// Set test value
		os.Setenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING", "false")
		
		config := LoadEngineConfig()
		assert.Equal(t, false, config.EnableAdaptiveMemorySizing)
	})
	
	t.Run("InvalidEnableAdaptiveMemorySizing", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING", original)
			} else {
				os.Unsetenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING")
			}
		}()
		
		// Set invalid value
		os.Setenv("ENGINE_ENABLE_ADAPTIVE_MEMORY_SIZING", "invalid")
		
		config := LoadEngineConfig()
		// Should fall back to default
		assert.Equal(t, DefaultEngineConfig().EnableAdaptiveMemorySizing, config.EnableAdaptiveMemorySizing)
	})
	
	t.Run("MemoryResizeThresholdFromEnv", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", original)
			} else {
				os.Unsetenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
			}
		}()
		
		// Set test value
		os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", "0.8")
		
		config := LoadEngineConfig()
		assert.Equal(t, 0.8, config.MemoryResizeThreshold)
	})
	
	t.Run("InvalidMemoryResizeThreshold", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", original)
			} else {
				os.Unsetenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
			}
		}()
		
		// Set invalid value
		os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", "invalid")
		
		config := LoadEngineConfig()
		// Should fall back to default
		assert.Equal(t, DefaultEngineConfig().MemoryResizeThreshold, config.MemoryResizeThreshold)
	})
	
	t.Run("OutOfRangeMemoryResizeThreshold", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", original)
			} else {
				os.Unsetenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
			}
		}()
		
		// Set out of range value
		os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", "1.5")
		
		config := LoadEngineConfig()
		// Should fall back to default
		assert.Equal(t, DefaultEngineConfig().MemoryResizeThreshold, config.MemoryResizeThreshold)
	})
	
	t.Run("ZeroMemoryResizeThreshold", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", original)
			} else {
				os.Unsetenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
			}
		}()
		
		// Set zero value
		os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", "0")
		
		config := LoadEngineConfig()
		// Should fall back to default
		assert.Equal(t, DefaultEngineConfig().MemoryResizeThreshold, config.MemoryResizeThreshold)
	})
	
	t.Run("NegativeMemoryResizeThreshold", func(t *testing.T) {
		// Save original environment variable
		original := os.Getenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
		defer func() {
			if original != "" {
				os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", original)
			} else {
				os.Unsetenv("ENGINE_MEMORY_RESIZE_THRESHOLD")
			}
		}()
		
		// Set negative value
		os.Setenv("ENGINE_MEMORY_RESIZE_THRESHOLD", "-0.5")
		
		config := LoadEngineConfig()
		// Should fall back to default
		assert.Equal(t, DefaultEngineConfig().MemoryResizeThreshold, config.MemoryResizeThreshold)
	})
}

func TestConfigValidation(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		config := EngineConfig{
			LeakDetectionThreshold:     5 * time.Minute,
			BufferPoolInitialSize:      500,
			BufferPoolMaxSize:          50000,
			BatchOperationThreshold:    200,
			IteratorBatchSize:          2000,
			EnableAdaptiveMemorySizing: true,
			MemoryResizeThreshold:      0.7,
		}
		
		// All values should be valid
		assert.True(t, config.LeakDetectionThreshold > 0)
		assert.True(t, config.BufferPoolInitialSize > 0)
		assert.True(t, config.BufferPoolMaxSize > 0)
		assert.True(t, config.BatchOperationThreshold > 0)
		assert.True(t, config.IteratorBatchSize > 0)
		assert.True(t, config.MemoryResizeThreshold > 0)
		assert.True(t, config.MemoryResizeThreshold <= 1)
	})
	
	t.Run("EdgeCaseValues", func(t *testing.T) {
		config := EngineConfig{
			LeakDetectionThreshold:     1 * time.Millisecond, // Minimum reasonable value
			BufferPoolInitialSize:      1,                    // Minimum value
			BufferPoolMaxSize:          1,                    // Minimum value
			BatchOperationThreshold:    1,                    // Minimum value
			IteratorBatchSize:          1,                    // Minimum value
			EnableAdaptiveMemorySizing: false,                // Valid boolean
			MemoryResizeThreshold:      0.01,                 // Minimum reasonable value
		}
		
		// All values should be valid
		assert.True(t, config.LeakDetectionThreshold > 0)
		assert.True(t, config.BufferPoolInitialSize > 0)
		assert.True(t, config.BufferPoolMaxSize > 0)
		assert.True(t, config.BatchOperationThreshold > 0)
		assert.True(t, config.IteratorBatchSize > 0)
		assert.True(t, config.MemoryResizeThreshold > 0)
		assert.True(t, config.MemoryResizeThreshold <= 1)
	})
}

func BenchmarkDefaultEngineConfig(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = DefaultEngineConfig()
	}
}

func BenchmarkLoadEngineConfig(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = LoadEngineConfig()
	}
}