package pgserver

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerConfiguration(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-config"
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

	// Test server creation with config
	cfg := &config.ServerConfig{
		MaxConnections:    100,
		ConnectionTimeout: 30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		MaxLifetime:       1 * time.Hour,
	}
	server := NewPostgreSQLServerWithConfig(exec, planner, cfg)
	assert.NotNil(t, server)

	// Test getting config
	retrievedCfg := server.GetConfig()
	assert.NotNil(t, retrievedCfg)
	assert.Equal(t, cfg.MaxConnections, retrievedCfg.MaxConnections)
	assert.Equal(t, cfg.ConnectionTimeout, retrievedCfg.ConnectionTimeout)
	assert.Equal(t, cfg.IdleTimeout, retrievedCfg.IdleTimeout)
	assert.Equal(t, cfg.MaxLifetime, retrievedCfg.MaxLifetime)
}

func TestServerComponentInitialization(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-components"
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

	// Test server creation
	server := NewPostgreSQLServer(exec, planner)
	assert.NotNil(t, server)

	// Test that all components are initialized
	assert.NotNil(t, server.connectionHandler)
	assert.NotNil(t, server.queryProcessor)
	assert.NotNil(t, server.statementManager)
	assert.NotNil(t, server.bufferPool)

	// Test server integration with components
	err = server.StartTCP("0") // Use port 0 for automatic assignment
	assert.NoError(t, err)

	// Give the server a moment to start
	time.Sleep(10 * time.Millisecond)

	// Test that we can get the listener address
	addr := server.GetListenerAddress()
	assert.NotNil(t, addr)

	// Clean up
	err = server.Close()
	assert.NoError(t, err)
}