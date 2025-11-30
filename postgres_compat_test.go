package main

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestPostgresCompatibility(t *testing.T) {
	// Connect to the PGLiteDB server
	// Use localhost instead of ::1 to avoid IPv6 issues
	connStr := "host=127.0.0.1 port=5432 dbname=pglitedb sslmode=disable"
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