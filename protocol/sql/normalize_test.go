package sql

import (
	"testing"
)

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Basic select query",
			input:    "SELECT * FROM users WHERE id = 1",
			expected: "select * from users where id = ?",
		},
		{
			name:     "Query with different case",
			input:    "select * from users where id = 1",
			expected: "select * from users where id = ?",
		},
		{
			name:     "Query with extra spaces",
			input:    "  SELECT   *   FROM   users   WHERE   id   =   1  ",
			expected: "select * from users where id = ?",
		},
		{
			name:     "Query with string literals",
			input:    "SELECT * FROM users WHERE name = 'John'",
			expected: "select * from users where name = '?'",
		},
		{
			name:     "Query with comments",
			input:    "SELECT * FROM users WHERE id = 1 /* comment */",
			expected: "select * from users where id = ?",
		},
		{
			name:     "Query with line comments",
			input:    "SELECT * FROM users WHERE id = 1 -- comment",
			expected: "select * from users where id = ?",
		},
		{
			name:     "Complex query with multiple conditions",
			input:    "SELECT id, name FROM users WHERE age > 25 AND status = 'active' ORDER BY created_at DESC LIMIT 10",
			expected: "select id, name from users where age > ? and status = '?' order by created_at desc limit ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeQuery(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeQuery(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}