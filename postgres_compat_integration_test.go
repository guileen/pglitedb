//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestPostgresCompatibilityWithServer starts a PostgreSQL server and tests compatibility
func TestPostgresCompatibilityWithServer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create a temporary directory for the test database
	tmpDir := "/tmp/pglitedb-compat-test"
	os.RemoveAll(tmpDir) // Clean up any previous test data
	defer os.RemoveAll(tmpDir)

	// Channel to signal when server is ready
	serverReady := make(chan bool, 1)
	serverError := make(chan error, 1)
	
	// Start the PostgreSQL server in a separate goroutine
	go func() {
		// Override the port to avoid conflicts
		os.Setenv("PG_PORT", "5433")
		defer os.Unsetenv("PG_PORT")
		
		// Create a context that can be cancelled
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		
		// Start the PostgreSQL server
		originalArgs := os.Args
		os.Args = []string{"main", "pg", tmpDir}
		defer func() { os.Args = originalArgs }()
		
		// We need to modify the startPostgreSQLServer function to accept a context
		// For now, let's run it in a goroutine and use signals to control it
		startPostgreSQLServer(tmpDir)
		
		serverReady <- true
	}()

	// Wait for the server to start or timeout
	select {
	case <-serverReady:
		// Server started
	case err := <-serverError:
		t.Fatalf("Server failed to start: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("Server startup timeout")
	}

	// Wait for the server to be ready to accept connections
	if err := waitForServerReady("localhost:5433", 10*time.Second); err != nil {
		t.Fatalf("Server failed to become ready: %v", err)
	}

	// Give the server a moment to fully initialize
	time.Sleep(100 * time.Millisecond)

	// Now run the compatibility test
	runPostgresCompatibilityTest(t, "5433")

	// Cleanup - send interrupt signal to stop the server
	// In a real implementation, we would have a better way to stop the server
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

// runPostgresCompatibilityTest runs the actual PostgreSQL compatibility tests
func runPostgresCompatibilityTest(t *testing.T, port string) {
	// Connect to the PGLiteDB server
	connStr := "host=127.0.0.1 port=" + port + " dbname=pglitedb sslmode=disable"
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
	t.Log("Successfully connected to PGLiteDB!")

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

	// Test some PostgreSQL-specific features that might work
	pgTests := []struct {
		name string
		sql  string
	}{
		{"Current timestamp", "SELECT CURRENT_TIMESTAMP"},
		{"String concatenation", "SELECT 'Hello' || ' ' || 'World' AS greeting"},
		{"Simple math", "SELECT 1 + 1 AS result"},
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