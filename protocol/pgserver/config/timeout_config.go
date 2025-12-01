package config

import "time"

// TimeoutConfig defines timeout values for different types of operations
type TimeoutConfig struct {
	// Default timeout for general queries
	DefaultQueryTimeout time.Duration
	
	// Short timeout for simple queries (SELECT, INSERT, UPDATE, DELETE with few conditions)
	SimpleQueryTimeout time.Duration
	
	// Medium timeout for complex queries (JOINs, subqueries, aggregations)
	ComplexQueryTimeout time.Duration
	
	// Long timeout for DDL operations (CREATE, ALTER, DROP)
	DDLTimeout time.Duration
	
	// Very long timeout for administrative operations (ANALYZE, VACUUM)
	AdminOperationTimeout time.Duration
	
	// Timeout for transaction operations
	TransactionTimeout time.Duration
}

// DefaultTimeoutConfig returns the default timeout configuration
func DefaultTimeoutConfig() *TimeoutConfig {
	return &TimeoutConfig{
		DefaultQueryTimeout:     30 * time.Second,
		SimpleQueryTimeout:      5 * time.Second,
		ComplexQueryTimeout:     15 * time.Second,
		DDLTimeout:              60 * time.Second,
		AdminOperationTimeout:   300 * time.Second, // 5 minutes
		TransactionTimeout:      30 * time.Second,
	}
}

// GetTimeoutForQuery determines the appropriate timeout based on the query type
func (tc *TimeoutConfig) GetTimeoutForQuery(query string) time.Duration {
	// Convert query to uppercase for easier matching
	upperQuery := query
	if len(query) > 100 {
		upperQuery = query[:100]
	}
	
	// Check for DDL operations
	if isDDLQuery(upperQuery) {
		return tc.DDLTimeout
	}
	
	// Check for administrative operations
	if isAdminOperation(upperQuery) {
		return tc.AdminOperationTimeout
	}
	
	// Check for complex queries
	if isComplexQuery(upperQuery) {
		return tc.ComplexQueryTimeout
	}
	
	// Check for transaction operations
	if isTransactionOperation(upperQuery) {
		return tc.TransactionTimeout
	}
	
	// Default to simple query timeout
	return tc.SimpleQueryTimeout
}

// isDDLQuery checks if the query is a DDL operation
func isDDLQuery(query string) bool {
	return containsAnyPrefix(query, []string{
		"CREATE TABLE", "ALTER TABLE", "DROP TABLE",
		"CREATE INDEX", "DROP INDEX",
		"CREATE VIEW", "DROP VIEW",
		"CREATE DATABASE", "DROP DATABASE", "ALTER DATABASE",
	})
}

// isAdminOperation checks if the query is an administrative operation
func isAdminOperation(query string) bool {
	return containsAnyPrefix(query, []string{
		"ANALYZE", "VACUUM", "REINDEX",
	})
}

// isComplexQuery checks if the query is complex (JOINs, subqueries, aggregations)
func isComplexQuery(query string) bool {
	return containsAny(query, []string{
		" JOIN ", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN",
		"GROUP BY", "HAVING", "DISTINCT", "UNION", "INTERSECT", "EXCEPT",
	}) || containsSubstringCount(query, "SELECT") > 1 // Subqueries
}

// isTransactionOperation checks if the query is a transaction operation
func isTransactionOperation(query string) bool {
	return containsAnyPrefix(query, []string{
		"BEGIN", "START TRANSACTION", "COMMIT", "ROLLBACK",
	})
}

// Helper functions
func containsAnyPrefix(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

func containsAny(s string, substrings []string) bool {
	for _, substr := range substrings {
		if len(s) >= len(substr) && containsSubstring(s, substr) {
			return true
		}
	}
	return false
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && index(s, substr) != -1
}

// Simple implementation of string index search
func index(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}
	
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// containsSubstringCount counts occurrences of a substring in a string
func containsSubstringCount(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	
	count := 0
	start := 0
	for {
		pos := index(s[start:], substr)
		if pos == -1 {
			break
		}
		count++
		start += pos + len(substr)
		if start >= len(s) {
			break
		}
	}
	return count
}