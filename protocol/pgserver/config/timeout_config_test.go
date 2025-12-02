package config

import (
	"testing"
	"time"
)

func TestDefaultTimeoutConfig(t *testing.T) {
	config := DefaultTimeoutConfig()
	
	if config.DefaultQueryTimeout != 30*time.Second {
		t.Errorf("Expected DefaultQueryTimeout to be 30s, got %v", config.DefaultQueryTimeout)
	}
	
	if config.SimpleQueryTimeout != 5*time.Second {
		t.Errorf("Expected SimpleQueryTimeout to be 5s, got %v", config.SimpleQueryTimeout)
	}
	
	if config.ComplexQueryTimeout != 15*time.Second {
		t.Errorf("Expected ComplexQueryTimeout to be 15s, got %v", config.ComplexQueryTimeout)
	}
	
	if config.DDLTimeout != 60*time.Second {
		t.Errorf("Expected DDLTimeout to be 60s, got %v", config.DDLTimeout)
	}
	
	if config.AdminOperationTimeout != 300*time.Second {
		t.Errorf("Expected AdminOperationTimeout to be 300s, got %v", config.AdminOperationTimeout)
	}
	
	if config.TransactionTimeout != 30*time.Second {
		t.Errorf("Expected TransactionTimeout to be 30s, got %v", config.TransactionTimeout)
	}
}

func TestGetTimeoutForQuery_DDL(t *testing.T) {
	tc := DefaultTimeoutConfig()
	
	// Test CREATE TABLE
	timeout := tc.GetTimeoutForQuery("CREATE TABLE users (id INT)")
	if timeout != tc.DDLTimeout {
		t.Errorf("Expected DDL timeout for CREATE TABLE, got %v", timeout)
	}
	
	// Test ALTER TABLE
	timeout = tc.GetTimeoutForQuery("ALTER TABLE users ADD COLUMN name VARCHAR(50)")
	if timeout != tc.DDLTimeout {
		t.Errorf("Expected DDL timeout for ALTER TABLE, got %v", timeout)
	}
	
	// Test DROP TABLE
	timeout = tc.GetTimeoutForQuery("DROP TABLE users")
	if timeout != tc.DDLTimeout {
		t.Errorf("Expected DDL timeout for DROP TABLE, got %v", timeout)
	}
}

func TestGetTimeoutForQuery_AdminOperations(t *testing.T) {
	tc := DefaultTimeoutConfig()
	
	// Test ANALYZE
	timeout := tc.GetTimeoutForQuery("ANALYZE users")
	if timeout != tc.AdminOperationTimeout {
		t.Errorf("Expected admin operation timeout for ANALYZE, got %v", timeout)
	}
	
	// Test VACUUM
	timeout = tc.GetTimeoutForQuery("VACUUM users")
	if timeout != tc.AdminOperationTimeout {
		t.Errorf("Expected admin operation timeout for VACUUM, got %v", timeout)
	}
	
	// Test REINDEX
	timeout = tc.GetTimeoutForQuery("REINDEX TABLE users")
	if timeout != tc.AdminOperationTimeout {
		t.Errorf("Expected admin operation timeout for REINDEX, got %v", timeout)
	}
}

func TestGetTimeoutForQuery_ComplexQueries(t *testing.T) {
	tc := DefaultTimeoutConfig()
	
	// Test JOIN queries
	timeout := tc.GetTimeoutForQuery("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id")
	if timeout != tc.ComplexQueryTimeout {
		t.Errorf("Expected complex query timeout for JOIN, got %v", timeout)
	}
	
	// Test GROUP BY queries
	timeout = tc.GetTimeoutForQuery("SELECT department, COUNT(*) FROM employees GROUP BY department")
	if timeout != tc.ComplexQueryTimeout {
		t.Errorf("Expected complex query timeout for GROUP BY, got %v", timeout)
	}
	
	// Test subqueries (multiple SELECT)
	timeout = tc.GetTimeoutForQuery("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)")
	if timeout != tc.ComplexQueryTimeout {
		t.Errorf("Expected complex query timeout for subquery, got %v", timeout)
	}
}

func TestGetTimeoutForQuery_TransactionOperations(t *testing.T) {
	tc := DefaultTimeoutConfig()
	
	// Test BEGIN
	timeout := tc.GetTimeoutForQuery("BEGIN")
	if timeout != tc.TransactionTimeout {
		t.Errorf("Expected transaction timeout for BEGIN, got %v", timeout)
	}
	
	// Test COMMIT
	timeout = tc.GetTimeoutForQuery("COMMIT")
	if timeout != tc.TransactionTimeout {
		t.Errorf("Expected transaction timeout for COMMIT, got %v", timeout)
	}
	
	// Test ROLLBACK
	timeout = tc.GetTimeoutForQuery("ROLLBACK")
	if timeout != tc.TransactionTimeout {
		t.Errorf("Expected transaction timeout for ROLLBACK, got %v", timeout)
	}
}

func TestGetTimeoutForQuery_SimpleQueries(t *testing.T) {
	tc := DefaultTimeoutConfig()
	
	// Test simple SELECT
	timeout := tc.GetTimeoutForQuery("SELECT * FROM users WHERE id = 1")
	if timeout != tc.SimpleQueryTimeout {
		t.Errorf("Expected simple query timeout for SELECT, got %v", timeout)
	}
	
	// Test simple INSERT
	timeout = tc.GetTimeoutForQuery("INSERT INTO users (name) VALUES ('John')")
	if timeout != tc.SimpleQueryTimeout {
		t.Errorf("Expected simple query timeout for INSERT, got %v", timeout)
	}
	
	// Test simple UPDATE
	timeout = tc.GetTimeoutForQuery("UPDATE users SET name = 'Jane' WHERE id = 1")
	if timeout != tc.SimpleQueryTimeout {
		t.Errorf("Expected simple query timeout for UPDATE, got %v", timeout)
	}
	
	// Test simple DELETE
	timeout = tc.GetTimeoutForQuery("DELETE FROM users WHERE id = 1")
	if timeout != tc.SimpleQueryTimeout {
		t.Errorf("Expected simple query timeout for DELETE, got %v", timeout)
	}
}

func TestContainsAnyPrefix(t *testing.T) {
	// Test positive cases
	if !containsAnyPrefix("CREATE TABLE test", []string{"CREATE TABLE", "ALTER TABLE"}) {
		t.Error("Expected true for matching prefix")
	}
	
	// Test negative cases
	if containsAnyPrefix("SELECT * FROM test", []string{"CREATE TABLE", "ALTER TABLE"}) {
		t.Error("Expected false for non-matching prefix")
	}
	
	// Test empty string
	if containsAnyPrefix("", []string{"CREATE TABLE"}) {
		t.Error("Expected false for empty string")
	}
	
	// Test empty prefixes
	if containsAnyPrefix("CREATE TABLE test", []string{}) {
		t.Error("Expected false for empty prefixes")
	}
}

func TestContainsAny(t *testing.T) {
	// Test positive cases
	if !containsAny("SELECT u.name FROM users u JOIN orders o", []string{" JOIN "}) {
		t.Error("Expected true for containing JOIN")
	}
	
	// Test negative cases
	if containsAny("SELECT * FROM users", []string{" JOIN "}) {
		t.Error("Expected false for not containing JOIN")
	}
	
	// Test empty string
	if containsAny("", []string{" JOIN "}) {
		t.Error("Expected false for empty string")
	}
	
	// Test empty substrings
	if containsAny("SELECT * FROM users", []string{}) {
		t.Error("Expected false for empty substrings")
	}
}

func TestContainsSubstringCount(t *testing.T) {
	// Test counting occurrences
	count := containsSubstringCount("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", "SELECT")
	if count != 2 {
		t.Errorf("Expected 2 SELECT occurrences, got %d", count)
	}
	
	// Test zero occurrences
	count = containsSubstringCount("SELECT * FROM users", "INSERT")
	if count != 0 {
		t.Errorf("Expected 0 INSERT occurrences, got %d", count)
	}
	
	// Test empty substring
	count = containsSubstringCount("SELECT * FROM users", "")
	if count != 0 {
		t.Errorf("Expected 0 occurrences for empty substring, got %d", count)
	}
	
	// Test empty string
	count = containsSubstringCount("", "SELECT")
	if count != 0 {
		t.Errorf("Expected 0 occurrences in empty string, got %d", count)
	}
}