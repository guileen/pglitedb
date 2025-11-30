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

func TestNewPostgreSQLServer(t *testing.T) {
	t.Run("ServerCreation", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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

		// Test server creation
		server := NewPostgreSQLServer(exec, planner)
		assert.NotNil(t, server)
		assert.NotNil(t, server.executor)
		assert.NotNil(t, server.parser)
		assert.NotNil(t, server.planner)
	})

	t.Run("ServerWithConfig", func(t *testing.T) {
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
		assert.NotNil(t, server.executor)
		assert.NotNil(t, server.parser)
		assert.NotNil(t, server.planner)
	})
}

func TestPostgreSQLServer_Close(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-close"
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

	// Test closing server
	err = server.Close()
	assert.NoError(t, err)

	// Test closing already closed server
	err = server.Close()
	assert.NoError(t, err)
}