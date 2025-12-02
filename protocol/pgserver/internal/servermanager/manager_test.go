package server

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestEnvironment creates a test environment with required components
func setupTestEnvironment(t *testing.T) (*sql.Executor, *sql.Planner, func()) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-servermanager-test-*")
	require.NoError(t, err)

	// Set up the required components
	dbPath := filepath.Join(tmpDir, "test-db")
	pebbleConfig := storage.DefaultPebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(pebbleConfig)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)

	// Load existing schemas
	err = mgr.LoadSchemas(context.Background())
	require.NoError(t, err)

	// Create executor
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	exec := planner.Executor()

	// Cleanup function
	cleanup := func() {
		kvStore.Close()
		os.RemoveAll(tmpDir)
	}

	return exec, planner, cleanup
}

// TestNewServerManager tests the creation and configuration of ServerManager
func TestNewServerManager(t *testing.T) {
	exec, planner, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("CreateWithDefaultConfig", func(t *testing.T) {
		// Test server manager creation with default config
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Test that we can get the configuration
		retrievedConfig := server.GetConfig()
		assert.NotNil(t, retrievedConfig)
		assert.Equal(t, cfg.MaxConnections, retrievedConfig.MaxConnections)
	})

	t.Run("CreateWithCustomConfig", func(t *testing.T) {
		// Test server manager creation with custom config
		cfg := &config.ServerConfig{
			MaxConnections:    200,
			ConnectionTimeout: 60 * time.Second,
			IdleTimeout:       10 * time.Minute,
			MaxLifetime:       2 * time.Hour,
			ProfilingPort:     ":6060",
		}
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Test that we can get the configuration
		retrievedConfig := server.GetConfig()
		assert.NotNil(t, retrievedConfig)
		assert.Equal(t, cfg.MaxConnections, retrievedConfig.MaxConnections)
		assert.Equal(t, cfg.ProfilingPort, retrievedConfig.ProfilingPort)
	})

	t.Run("CreateWithNilConfig", func(t *testing.T) {
		// Test server manager creation with nil config (should panic)
		assert.Panics(t, func() {
			NewServerManager(exec, planner, nil)
		})
	})
}

// TestServerManagerStartAndClose tests the start and close functionality
func TestServerManagerStartAndClose(t *testing.T) {
	exec, planner, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("CloseUnstartedServer", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Test closing an unstarted server
		err := server.Close()
		assert.NoError(t, err)
		assert.True(t, server.IsClosed())
	})

	t.Run("CloseAlreadyClosedServer", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Close the server twice
		err1 := server.Close()
		err2 := server.Close()
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.True(t, server.IsClosed())
	})
}

// TestServerManagerConnectionCount tests the connection counting functionality
func TestServerManagerConnectionCount(t *testing.T) {
	exec, planner, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("InitialConnectionCount", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Initial connection count should be 0
		count := server.GetConnectionCount()
		assert.Equal(t, 0, count)
	})

	t.Run("ConnectionCountAfterOperations", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Connection count should still be 0 (no actual connections made)
		count := server.GetConnectionCount()
		assert.Equal(t, 0, count)

		// Close server
		err := server.Close()
		assert.NoError(t, err)

		// Connection count should still be 0 after closing
		count = server.GetConnectionCount()
		assert.Equal(t, 0, count)
	})
}

// TestServerManagerConfigApplication tests configuration application
func TestServerManagerConfigApplication(t *testing.T) {
	exec, planner, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("ApplyValidConfig", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Apply a new configuration
		newCfg := &config.ServerConfig{
			MaxConnections:    150,
			ConnectionTimeout: 45 * time.Second,
			IdleTimeout:       7 * time.Minute,
			MaxLifetime:       90 * time.Minute,
			ProfilingPort:     ":7070",
		}

		err := server.ApplyConfig(newCfg)
		assert.NoError(t, err)

		// Verify the configuration was applied
		retrievedConfig := server.GetConfig()
		assert.Equal(t, newCfg.MaxConnections, retrievedConfig.MaxConnections)
		assert.Equal(t, newCfg.ProfilingPort, retrievedConfig.ProfilingPort)
	})

	t.Run("ApplyNilConfig", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Applying nil config should not cause an error
		err := server.ApplyConfig(nil)
		assert.NoError(t, err)

		// Original config should remain
		retrievedConfig := server.GetConfig()
		assert.Equal(t, cfg.MaxConnections, retrievedConfig.MaxConnections)
	})

	t.Run("GetConfigWhenNil", func(t *testing.T) {
		// Test that creating server with nil config panics
		assert.Panics(t, func() {
			NewServerManager(exec, planner, nil)
		})
	})
}

// TestServerManagerProfiling tests profiling functionality
func TestServerManagerProfiling(t *testing.T) {
	exec, planner, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("GetProfilingPort", func(t *testing.T) {
		cfg := &config.ServerConfig{
			ProfilingPort: ":8080",
		}
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Should return the configured profiling port
		port := server.GetProfilingPort()
		assert.Equal(t, ":8080", port)
	})

	t.Run("SetProfilingPort", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Set a new profiling port
		err := server.SetProfilingPort(":9090")
		assert.NoError(t, err)

		// Should return the new profiling port
		port := server.GetProfilingPort()
		assert.Equal(t, ":9090", port)
	})

	t.Run("WithProfiling", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Enable profiling with a port
		result := server.WithProfiling(":10000")
		assert.NotNil(t, result)

		// Should be the same server instance
		assert.Equal(t, server, result)

		// Should return the profiling port
		port := server.GetProfilingPort()
		assert.Equal(t, ":10000", port)
	})
}

// TestServerManagerNetworkOperations tests network listening functionality
func TestServerManagerNetworkOperations(t *testing.T) {
	exec, planner, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("GetListenerAddressOnUnstartedServer", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Address should be nil for unstarted server
		addr := server.GetListenerAddress()
		assert.Nil(t, addr)
	})

	t.Run("StartTCPOnClosedServer", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Close the server
		err := server.Close()
		assert.NoError(t, err)

		// Attempt to start TCP on closed server should fail
		err = server.StartTCP(":0") // Use port 0 for automatic assignment
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "server is closed")
	})

	t.Run("StartUnixOnClosedServer", func(t *testing.T) {
		cfg := config.DefaultServerConfig()
		server := NewServerManager(exec, planner, cfg)
		assert.NotNil(t, server)

		// Close the server
		err := server.Close()
		assert.NoError(t, err)

		// Attempt to start Unix socket on closed server should fail
		tempDir := t.TempDir()
		socketPath := filepath.Join(tempDir, "test.sock")
		err = server.StartUnix(socketPath)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "server is closed")
	})
}

// TestServerManagerInterfaceCompliance tests that ServerManager implements ServerInterface
func TestServerManagerInterfaceCompliance(t *testing.T) {
	// This test ensures that ServerManager properly implements all methods of ServerInterface
	var _ interfaces.ServerInterface = &ServerManager{}
}

// TestServerManagerComponentReferences tests that ServerManager implements ServerInterface
func TestServerManagerComponentReferences(t *testing.T) {
	// This test ensures that ServerManager properly implements all methods of ServerInterface
	var _ interfaces.ServerInterface = &ServerManager{}
}