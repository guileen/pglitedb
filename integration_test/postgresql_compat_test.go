//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/guileen/pglitedb/protocol/pgserver"
	pgsql "github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/catalog"
	_ "github.com/lib/pq"
)

// TestPostgreSQLCompatibility tests basic PostgreSQL compatibility
func TestPostgreSQLCompatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create a temporary directory for the test database
	tmpDir := "/tmp/pglitedb-pg-compat-test"
	os.RemoveAll(tmpDir) // Clean up any previous test data
	defer os.RemoveAll(tmpDir)

	// Start a PostgreSQL server for testing
	port := "5433" // Use a different port to avoid conflicts
	server, err := startTestPostgreSQLServer(tmpDir, port)
	if err != nil {
		t.Fatalf("Failed to start PostgreSQL server: %v", err)
	}
	defer server.Close()

	// Wait for server to be ready
	if err := waitForServerReady("localhost:"+port, 10*time.Second); err != nil {
		t.Fatalf("Server failed to become ready: %v", err)
	}

	// Give the server a moment to fully initialize
	time.Sleep(100 * time.Millisecond)

	// Connect to the test server
	connStr := fmt.Sprintf("host=127.0.0.1 port=%s dbname=pglitedb sslmode=disable", port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}
	t.Log("Successfully connected to PGLiteDB PostgreSQL server!")

	// Run basic PostgreSQL compatibility tests
	tests := []struct {
		name string
		sql  string
	}{
		{"Create table", "CREATE TABLE test_users (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))"},
		{"Insert data", "INSERT INTO test_users (name, email) VALUES ('John Doe', 'john@example.com'), ('Jane Smith', 'jane@example.com')"},
		{"Select data", "SELECT * FROM test_users"},
		{"Update data", "UPDATE test_users SET email = 'john.doe@example.com' WHERE name = 'John Doe'"},
		{"Count rows", "SELECT COUNT(*) FROM test_users"},
		{"Delete data", "DELETE FROM test_users WHERE name = 'Jane Smith'"},
		{"Drop table", "DROP TABLE test_users"},
	}

	passed := 0
	failed := 0

	for _, test := range tests {
		start := time.Now()
		_, err := db.Exec(test.sql)
		duration := time.Since(start)
		
		if err != nil {
			t.Errorf("❌ FAILED: %s - %s (took %v)", test.name, err, duration)
			failed++
		} else {
			t.Logf("✅ PASSED: %s (took %v)", test.name, duration)
			passed++
		}
	}

	// Test some PostgreSQL-specific features that should work
	pgTests := []struct {
		name string
		sql  string
	}{
		{"Current timestamp", "SELECT CURRENT_TIMESTAMP"},
		{"String concatenation", "SELECT 'Hello' || ' ' || 'World' AS greeting"},
		{"Simple math", "SELECT 1 + 1 AS result"},
		{"Basic SELECT", "SELECT 'test' AS value"},
	}

	t.Log("\n--- PostgreSQL-like Features ---")
	for _, test := range pgTests {
		start := time.Now()
		rows, err := db.Query(test.sql)
		duration := time.Since(start)
		
		if err != nil {
			t.Errorf("❌ FAILED: %s - %s (took %v)", test.name, err, duration)
			failed++
		} else {
			t.Logf("✅ PASSED: %s (took %v)", test.name, duration)
			rows.Close()
			passed++
		}
	}

	t.Logf("\n--- Test Results ---")
	t.Logf("Passed: %d", passed)
	t.Logf("Failed: %d", failed)
	t.Logf("Total: %d", passed+failed)
	
	if failed == 0 {
		t.Log("🎉 All tests passed! Good PostgreSQL compatibility!")
	} else {
		t.Logf("⚠️  %d tests failed. Some PostgreSQL features may not be fully supported.", failed)
	}
}

// startTestPostgreSQLServer starts a PostgreSQL server for testing
func startTestPostgreSQLServer(dbPath, port string) (*pgserver.PostgreSQLServer, error) {
	// Create database components
	kvConfig := storage.HighPerformancePebbleConfig(dbPath + "-postgres")
	kvStore, err := storage.NewPebbleKV(kvConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create pebble kv: %v", err)
	}

	// Create codec
	c := codec.NewMemComparableCodec()

	// Create engine and manager
	eng := engine.NewStorageEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)

	// Load existing schemas
	if err := mgr.LoadSchemas(context.Background()); err != nil {
		// This is not fatal, just log a warning
		fmt.Printf("Warning: failed to load schemas: %v\n", err)
	}

	// Create SQL parser and planner
	parser := pgsql.NewPGParser()
	planner := pgsql.NewPlannerWithCatalog(parser, mgr)
	exec := planner.Executor()

	// Create PostgreSQL server
	server := pgserver.NewPostgreSQLServer(exec, planner)

	// Start server in a goroutine
	go func() {
		if err := server.Start(port); err != nil {
			fmt.Printf("PostgreSQL server failed: %v\n", err)
		}
	}()

	return server, nil
}

// waitForServerReady waits for the server to be ready to accept connections
func waitForServerReady(addr string, timeout time.Duration) error {
	timeoutChan := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutChan:
			return fmt.Errorf("timeout waiting for server to be ready")
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err == nil {
				conn.Close()
				return nil
			}
		}
	}
}