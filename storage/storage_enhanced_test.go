package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pglitedb/storage/internal/kv"
	"github.com/guileen/pglitedb/storage/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageFactoryFunctions(t *testing.T) {
	t.Run("NewPebbleKV", func(t *testing.T) {
		// Create a temporary directory for the test database
		tmpDir, err := os.MkdirTemp("", "pebble-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := filepath.Join(tmpDir, "test-db")
		config := DefaultPebbleConfig(dbPath)
		
		kvStore, err := NewPebbleKV(config)
		assert.NoError(t, err)
		assert.NotNil(t, kvStore)
		
		// Test that we can perform basic operations
		err = kvStore.Set(context.Background(), []byte("test-key"), []byte("test-value"))
		assert.NoError(t, err)
		
		value, err := kvStore.Get(context.Background(), []byte("test-key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("test-value"), value)
		
		// Clean up
		err = kvStore.Close()
		assert.NoError(t, err)
	})

	t.Run("DefaultPebbleConfig", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "pebble-config-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := filepath.Join(tmpDir, "test-db")
		config := DefaultPebbleConfig(dbPath)
		
		assert.NotNil(t, config)
		assert.Equal(t, dbPath, config.Path)
	})

	t.Run("TestOptimizedPebbleConfig", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "pebble-config-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := filepath.Join(tmpDir, "test-db")
		config := TestOptimizedPebbleConfig(dbPath)
		
		assert.NotNil(t, config)
		assert.Equal(t, dbPath, config.Path)
	})

	t.Run("HighPerformancePebbleConfig", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "pebble-config-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := filepath.Join(tmpDir, "test-db")
		config := HighPerformancePebbleConfig(dbPath)
		
		assert.NotNil(t, config)
		assert.Equal(t, dbPath, config.Path)
	})
}

func TestStorageConfigurations(t *testing.T) {
	// Skip this test if running in a CI environment or when specifically requested to skip long tests
	if os.Getenv("SKIP_LONG_TESTS") == "true" {
		t.Skip("Skipping long storage configuration test")
	}

	tmpDir, err := os.MkdirTemp("", "pebble-multi-config-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configs := []struct {
		name   string
		config *kv.PebbleConfig
	}{
		{"Default", DefaultPebbleConfig(filepath.Join(tmpDir, "default-db"))},
		{"TestOptimized", TestOptimizedPebbleConfig(filepath.Join(tmpDir, "test-db"))},
		{"HighPerformance", HighPerformancePebbleConfig(filepath.Join(tmpDir, "perf-db"))},
	}

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			kvStore, err := kv.NewPebbleKV(tc.config)
			assert.NoError(t, err)
			assert.NotNil(t, kvStore)
			
			// Test basic functionality
			testKey := []byte("config-test-key")
			testValue := []byte("config-test-value")
			
			err = kvStore.Set(context.Background(), testKey, testValue)
			assert.NoError(t, err)
			
			value, err := kvStore.Get(context.Background(), testKey)
			assert.NoError(t, err)
			assert.Equal(t, testValue, value)
			
			err = kvStore.Close()
			assert.NoError(t, err)
		})
	}
}

func TestStorageErrorHandling(t *testing.T) {
	t.Run("IsNotFound", func(t *testing.T) {
		// Test the IsNotFound function
		result := IsNotFound(shared.ErrNotFound)
		assert.True(t, result)
		
		result = IsNotFound(nil)
		assert.False(t, result)
		
		result = IsNotFound(assert.AnError)
		assert.False(t, result)
	})
}