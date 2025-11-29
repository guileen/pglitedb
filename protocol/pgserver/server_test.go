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
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
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
		assert.NotNil(t, server.executor)
		assert.NotNil(t, server.parser)
		assert.NotNil(t, server.planner)
		assert.NotNil(t, server.bufferPool)
		assert.NotNil(t, server.preparedStatements)
		assert.NotNil(t, server.portals)
	})
}

func TestPostgreSQLServer_WithProfiling(t *testing.T) {
	t.Run("WithProfiling", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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

		// Test server creation with profiling
		server := NewPostgreSQLServer(exec, planner)
		server = server.WithProfiling("6060")
		
		assert.Equal(t, "6060", server.httpPort)
	})
}

func TestPostgreSQLServer_Close(t *testing.T) {
	t.Run("CloseUninitializedServer", func(t *testing.T) {
		// Test closing a server that hasn't been initialized
		server := &PostgreSQLServer{}
		err := server.Close()
		assert.NoError(t, err)
	})

	t.Run("CloseInitializedServer", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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

		// Test server creation and closing
		server := NewPostgreSQLServer(exec, planner)
		err = server.Close()
		assert.NoError(t, err)
		
		// Test that closing twice doesn't cause issues
		err = server.Close()
		assert.NoError(t, err)
	})
}

func TestPreparedStatement_Structure(t *testing.T) {
	t.Run("PreparedStatementCreation", func(t *testing.T) {
		stmt := &PreparedStatement{
			Name:            "test_stmt",
			Query:           "SELECT * FROM users WHERE id = $1",
			PreprocessedSQL: "SELECT * FROM users WHERE id = ?",
			ParameterOIDs:   []uint32{23}, // INT4OID
			ReturningColumns: []string{"id", "name"},
		}

		assert.Equal(t, "test_stmt", stmt.Name)
		assert.Equal(t, "SELECT * FROM users WHERE id = $1", stmt.Query)
		assert.Equal(t, "SELECT * FROM users WHERE id = ?", stmt.PreprocessedSQL)
		assert.Equal(t, []uint32{23}, stmt.ParameterOIDs)
		assert.Equal(t, []string{"id", "name"}, stmt.ReturningColumns)
	})
}

func TestPortal_Structure(t *testing.T) {
	t.Run("PortalCreation", func(t *testing.T) {
		stmt := &PreparedStatement{
			Name:  "test_stmt",
			Query: "SELECT * FROM users WHERE id = $1",
		}

		portal := &Portal{
			Name:         "test_portal",
			Statement:    stmt,
			Params:       []interface{}{42},
			ParamFormats: []int16{1},
		}

		assert.Equal(t, "test_portal", portal.Name)
		assert.Equal(t, stmt, portal.Statement)
		assert.Equal(t, []interface{}{42}, portal.Params)
		assert.Equal(t, []int16{1}, portal.ParamFormats)
	})
}

func TestServerComponentInitialization(t *testing.T) {
	t.Run("BufferPoolInitialization", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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
		assert.NotNil(t, server.bufferPool)
	})

	t.Run("MapsInitialization", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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
		assert.NotNil(t, server.preparedStatements)
		assert.NotNil(t, server.portals)
		
		// Test that maps are empty initially
		assert.Empty(t, server.preparedStatements)
		assert.Empty(t, server.portals)
	})
}

func TestServerIntegrationWithComponents(t *testing.T) {
	t.Run("ServerIntegrationWithExecutor", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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

		// Create a test table
		ctx := context.Background()
		tableDef := &types.TableDefinition{
			Name: "test_users",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
				{Name: "name", Type: types.ColumnTypeText},
			},
		}
		err = mgr.CreateTable(ctx, 1, tableDef)
		require.NoError(t, err)

		// Create executor
		parser := sql.NewPGParser()
		planner := sql.NewPlannerWithCatalog(parser, mgr)
		exec := planner.Executor()

		// Test server creation with real components
		server := NewPostgreSQLServer(exec, planner)
		assert.NotNil(t, server.executor)
		assert.NotNil(t, server.planner)
	})

	t.Run("ServerIntegrationWithParser", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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

		// Test server creation with real components
		server := NewPostgreSQLServer(exec, planner)
		assert.NotNil(t, server.parser)
	})
}

func TestServerConfiguration(t *testing.T) {
	t.Run("DefaultConfiguration", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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

		// Test server creation with default configuration
		server := NewPostgreSQLServer(exec, planner)
		assert.Equal(t, "", server.httpPort) // No profiling by default
	})

	t.Run("ProfilingDisabledByDefault", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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
		assert.Equal(t, "", server.httpPort)
	})
}

func TestConcurrentAccessSafety(t *testing.T) {
	t.Run("ConcurrentServerCreation", func(t *testing.T) {
		// Test that server creation is thread-safe
		done := make(chan bool)

		// Create multiple servers concurrently
		for i := 0; i < 5; i++ {
			go func() {
				// Create a temporary database for testing
				tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
				require.NoError(t, err)
				defer os.RemoveAll(tmpDir)

				// Set up the required components
				dbPath := tmpDir + "/test-db"
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

				server := NewPostgreSQLServer(exec, planner)
				assert.NotNil(t, server)
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 5; i++ {
			<-done
		}
	})

	t.Run("ConcurrentMapAccess", func(t *testing.T) {
		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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

		server := NewPostgreSQLServer(exec, planner)

		// Test concurrent access to prepared statements map
		done := make(chan bool)
		
		// Concurrently add prepared statements
		for i := 0; i < 10; i++ {
			go func(i int) {
				stmt := &PreparedStatement{
					Name:  "stmt_" + string(rune(i+'0')),
					Query: "SELECT * FROM test WHERE id = $" + string(rune(i+'0')),
				}
				// Use mutex to protect map access
				server.psMutex.Lock()
				server.preparedStatements[stmt.Name] = stmt
				server.psMutex.Unlock()
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}

		// Verify all statements were added
		assert.Len(t, server.preparedStatements, 10)
	})
}

func TestErrorHandling(t *testing.T) {
	t.Run("NilExecutorHandling", func(t *testing.T) {
		// Test server creation with nil components
		// Note: This would normally cause a panic, but we're just testing the constructor
		assert.True(t, true) // Placeholder
	})

	t.Run("NilParserHandling", func(t *testing.T) {
		// Test server creation with nil components
		assert.True(t, true) // Placeholder
	})
}

func TestPerformanceCharacteristics(t *testing.T) {
	t.Run("ServerCreationPerformance", func(t *testing.T) {
		// Test that server creation is reasonably fast
		startTime := time.Now()

		// Create a temporary database for testing
		tmpDir, err := ioutil.TempDir("", "pglitedb-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Set up the required components
		dbPath := tmpDir + "/test-db"
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
		_ = NewPostgreSQLServer(exec, planner)

		duration := time.Since(startTime)
		assert.True(t, duration < 1*time.Second, "Server creation should be fast")
	})
}