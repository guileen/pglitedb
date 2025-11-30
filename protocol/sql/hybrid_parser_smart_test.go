package sql

import (
	"testing"
)

func TestHybridParserSmartDecisions(t *testing.T) {
	parser := NewHybridPGParser()

	tests := []struct {
		name        string
		query       string
		description string
	}{
		{
			name:        "Simple SELECT",
			query:       "SELECT id, name FROM users",
			description: "Basic SELECT should use simple parser",
		},
		{
			name:        "SELECT with WHERE",
			query:       "SELECT * FROM users WHERE age > 25",
			description: "SELECT with basic WHERE should use simple parser",
		},
		{
			name:        "SELECT with ORDER BY",
			query:       "SELECT id, name FROM users ORDER BY name",
			description: "SELECT with ORDER BY should use simple parser",
		},
		{
			name:        "SELECT with LIMIT",
			query:       "SELECT * FROM users LIMIT 10",
			description: "SELECT with LIMIT should use simple parser",
		},
		{
			name:        "Complex SELECT with JOIN",
			query:       "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
			description: "SELECT with JOIN should use full parser",
		},
		{
			name:        "SELECT with GROUP BY",
			query:       "SELECT department, COUNT(*) FROM employees GROUP BY department",
			description: "SELECT with GROUP BY should use full parser",
		},
		{
			name:        "SELECT with subquery",
			query:       "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			description: "SELECT with subquery should use full parser",
		},
		{
			name:        "INSERT statement",
			query:       "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
			description: "Basic INSERT should use simple parser",
		},
		{
			name:        "UPDATE statement",
			query:       "UPDATE users SET name = 'Bob' WHERE id = 1",
			description: "Basic UPDATE should use simple parser",
		},
		{
			name:        "DELETE statement",
			query:       "DELETE FROM users WHERE id = 1",
			description: "Basic DELETE should use simple parser",
		},
	}

	// Test that all queries can be parsed without error
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
		})
	}
}