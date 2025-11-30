package pgserver

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLServer_WithProfiling(t *testing.T) {
	// Create a temporary database for testing
	tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Set up the required components
	dbPath := tmpDir + "/test-db-profiling"
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

	// Test server creation with profiling
	server := NewPostgreSQLServer(exec, planner)
	server = server.WithProfiling("6060")

	assert.Equal(t, "6060", server.GetProfilingPort())

	// Test setting profiling port
	err = server.SetProfilingPort("6061")
	assert.NoError(t, err)
	assert.Equal(t, "6061", server.GetProfilingPort())
}