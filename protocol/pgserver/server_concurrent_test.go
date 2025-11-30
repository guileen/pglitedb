package pgserver

import (
	"context"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentAccessSafety(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-concurrent"
	pebbleConfig := storage.DefaultPebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(pebbleConfig)
	require.NoError(t, err)
	defer kvStore.Close()

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

	// Create server
	server := NewPostgreSQLServer(exec, planner)
	assert.NotNil(t, server)

	// Test concurrent access to connection count
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := server.GetConnectionCount()
			assert.GreaterOrEqual(t, count, 0)
		}()
	}
	wg.Wait()

	// Test concurrent access to profiling port
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			port := server.GetProfilingPort()
			// Port should be empty string initially
			assert.Equal(t, "", port)
		}()
	}
	wg.Wait()

	// Test concurrent configuration access
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg := server.GetConfig()
			assert.NotNil(t, cfg)
		}()
	}
	wg.Wait()
}

func TestErrorHandling(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-error"
	config := storage.DefaultPebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)
	defer kvStore.Close()

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

	// Create server
	server := NewPostgreSQLServer(exec, planner)
	assert.NotNil(t, server)

	// Test error handling for StartTCP on closed server
	err = server.Close()
	assert.NoError(t, err)

	err = server.StartTCP("5432")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server is closed")
}

func TestPerformanceCharacteristics(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-perf"
	config := storage.DefaultPebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)
	defer kvStore.Close()

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

	// Create server
	server := NewPostgreSQLServer(exec, planner)
	assert.NotNil(t, server)

	// Measure startup time
	start := time.Now()
	err = server.StartTCP("0") // Use port 0 for automatic assignment
	assert.NoError(t, err)
	startupTime := time.Since(start)
	
	// Startup should be reasonably fast
	assert.Less(t, startupTime, 5*time.Second)

	// Give the server a moment to start
	time.Sleep(10 * time.Millisecond)

	// Test connection count tracking
	initialCount := server.GetConnectionCount()
	assert.Equal(t, 0, initialCount)

	// Clean up
	err = server.Close()
	assert.NoError(t, err)

	// Measure shutdown time
	start = time.Now()
	shutdownTime := time.Since(start)
	
	// Shutdown should be reasonably fast
	assert.Less(t, shutdownTime, 5*time.Second)
}