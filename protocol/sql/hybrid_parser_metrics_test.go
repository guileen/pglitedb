package sql

import (
	"testing"
)

func TestHybridParserDetailedMetrics(t *testing.T) {
	parser := NewHybridPGParser()

	// Test simple queries
	simpleQueries := []string{
		"SELECT id, name FROM users WHERE age > 25",
		"INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
		"UPDATE users SET name = 'Bob' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
	}

	// Test complex queries
	complexQueries := []string{
		"SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
		"SELECT department, COUNT(*) FROM employees GROUP BY department",
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
	}

	// Parse simple queries
	for _, query := range simpleQueries {
		_, err := parser.Parse(query)
		if err != nil {
			t.Fatalf("Failed to parse simple query: %v", err)
		}
	}

	// Parse complex queries
	for _, query := range complexQueries {
		_, err := parser.Parse(query)
		if err != nil {
			t.Fatalf("Failed to parse complex query: %v", err)
		}
	}

	// Check basic stats
	stats := parser.GetStats()
	if stats["parse_attempts"] != 7 {
		t.Errorf("Expected 7 parse attempts, got %d", stats["parse_attempts"])
	}
	
	// Note: The actual counts may vary based on the parser's complexity analysis
	// We're mainly testing that the metrics are being tracked
}

func TestHybridParserPerformanceMetrics(t *testing.T) {
	parser := NewHybridPGParser()

	// Parse a few queries to generate timing data
	queries := []string{
		"SELECT id, name FROM users",
		"SELECT * FROM users WHERE age > 25",
		"INSERT INTO users (name) VALUES ('Alice')",
		"UPDATE users SET name = 'Bob'",
		"SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id", // This should use full parser
	}

	for _, query := range queries {
		_, err := parser.Parse(query)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}
	}

	// Get detailed stats
	detailedStats := parser.GetDetailedStats()
	
	// Verify timing metrics exist and are reasonable
	if detailedStats["total_parse_time_ns"].(int64) <= 0 {
		t.Error("Expected positive total parse time")
	}
	
	if detailedStats["simple_parse_time_ns"].(int64) < 0 {
		t.Error("Expected non-negative simple parse time")
	}
	
	if detailedStats["full_parse_time_ns"].(int64) < 0 {
		t.Error("Expected non-negative full parse time")
	}
	
	// Verify average times make sense
	if avgTime, ok := detailedStats["avg_parse_time_ns"].(int64); ok {
		if avgTime <= 0 {
			t.Error("Expected positive average parse time")
		}
	} else {
		t.Error("Expected avg_parse_time_ns to be int64")
	}
}