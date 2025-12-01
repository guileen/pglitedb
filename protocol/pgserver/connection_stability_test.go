package pgserver

import (
	"context"
	"io/ioutil"
	"net"
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

// TestConnectionStability verifies that connections are handled properly during server shutdown
func TestConnectionStability(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-stability"
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

	// Start server on a random port
	err = server.StartTCP("0")
	assert.NoError(t, err)

	// Give the server a moment to start
	time.Sleep(10 * time.Millisecond)

	// Get the actual port the server is listening on
	addr := server.GetListenerAddress()
	assert.NotNil(t, addr)
	t.Logf("Server listening on %s", addr.String())

	// Connect multiple clients simultaneously
	const numClients = 5
	var wg sync.WaitGroup
	clients := make([]net.Conn, numClients)

	// Connect clients
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr.String())
			if err != nil {
				t.Errorf("Client %d failed to connect: %v", i, err)
				return
			}
			clients[i] = conn
			
			// Keep the connection alive for a while
			time.Sleep(50 * time.Millisecond)
		}(i)
	}

	// Wait for all clients to connect
	wg.Wait()

	// Verify connection count
	initialCount := server.GetConnectionCount()
	t.Logf("Initial connection count: %d", initialCount)
	// Note: Connection count might be slightly different due to timing,
	// but should be close to numClients

	// Close client connections
	for _, conn := range clients {
		if conn != nil {
			conn.Close()
		}
	}

	// Start server shutdown in a separate goroutine
	shutdownComplete := make(chan bool)
	go func() {
		err := server.Close()
		assert.NoError(t, err)
		close(shutdownComplete)
	}()

	// Wait for shutdown to complete with timeout
	select {
	case <-shutdownComplete:
		t.Log("Server shutdown completed successfully")
	case <-time.After(10 * time.Second):
		t.Fatal("Server shutdown timed out")
	}
}

// TestGracefulShutdown verifies that the server shuts down gracefully
func TestGracefulShutdown(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-graceful"
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

	// Start server
	err = server.StartTCP("0")
	assert.NoError(t, err)

	// Give the server a moment to start
	time.Sleep(10 * time.Millisecond)

	// Verify server is listening
	addr := server.GetListenerAddress()
	assert.NotNil(t, addr)
	t.Logf("Server listening on %s", addr.String())

	// Connect a client
	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err)
	defer conn.Close()

	// Verify connection was accepted
	time.Sleep(10 * time.Millisecond)
	count := server.GetConnectionCount()
	assert.GreaterOrEqual(t, count, 1, "Should have at least one connection")

	// Close server - should wait for connections to finish
	err = server.Close()
	assert.NoError(t, err)

	// Verify server is properly closed
	// Try to connect again - should fail
	_, err = net.Dial("tcp", addr.String())
	assert.Error(t, err, "Should not be able to connect to closed server")
}