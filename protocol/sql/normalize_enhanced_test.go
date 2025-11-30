package sql

import (
	"testing"
)

func TestEnhancedNormalizeQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Basic query",
			input:    "SELECT id, name FROM users WHERE age > 25",
			expected: "select id, name from users where age > ?",
		},
		{
			name:     "Query with extra spaces",
			input:    "  SELECT   id,   name   FROM   users   WHERE   age   >   25  ",
			expected: "select id, name from users where age > ?",
		},
		{
			name:     "Query with different case",
			input:    "select ID, NAME from USERS where AGE > 25",
			expected: "select id, name from users where age > ?",
		},
		{
			name:     "Query with comments",
			input:    "SELECT id, name FROM users WHERE age > 25 /* comment */",
			expected: "select id, name from users where age > ?",
		},
		{
			name:     "Query with line comments",
			input:    "SELECT id, name FROM users WHERE age > 25 -- comment",
			expected: "select id, name from users where age > ?",
		},
		{
			name:     "Query with string literals",
			input:    "SELECT id, name FROM users WHERE name = 'John'",
			expected: "select id, name from users where name = '?'",
		},
		{
			name:     "Query with boolean values",
			input:    "SELECT id, name FROM users WHERE active = TRUE",
			expected: "select id, name from users where active = true",
		},
		{
			name:     "Query with NULL values",
			input:    "SELECT id, name FROM users WHERE name IS NULL",
			expected: "select id, name from users where name is null",
		},
		{
			name:     "Query with function calls",
			input:    "SELECT COUNT(*) FROM users WHERE created_at > NOW()",
			expected: "select count(*) from users where created_at > now()",
		},
		{
			name:     "Query with IN clause",
			input:    "SELECT id, name FROM users WHERE id IN (1, 2, 3)",
			expected: "select id, name from users where id in (?, ?, ?)",
		},
		{
			name:     "Query with BETWEEN clause",
			input:    "SELECT id, name FROM users WHERE age BETWEEN 18 AND 65",
			expected: "select id, name from users where age between ? and ?",
		},
		{
			name:     "Query with trailing semicolon",
			input:    "SELECT id, name FROM users WHERE age > 25;",
			expected: "select id, name from users where age > ?",
		},
		{
			name:     "Complex query with multiple conditions",
			input:    "SELECT u.id, u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.age > 25 AND u.active = TRUE ORDER BY p.created_at DESC LIMIT 10",
			expected: "select u.id, u.name, p.title from users u join posts p on u.id = p.user_id where u.age > ? and u.active = true order by p.created_at desc limit ?",
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

func TestCacheHitImprovement(t *testing.T) {
	// Test that different formatting of the same query produces the same normalized result
	queryVariants := []string{
		"SELECT id, name FROM users WHERE age > 25",
		"select id, name from users where age > 25",
		"  SELECT   id,   name   FROM   users   WHERE   age   >   25  ",
		"SELECT id,name FROM users WHERE age>25",
		"SELECT id, name FROM users WHERE age > 25;",
		"select ID, NAME from USERS where AGE > 25",
		"SELECT  id,  name  FROM  users  WHERE  age  >  25",
	}

	// All variants should normalize to the same result
	expected := NormalizeQuery(queryVariants[0])
	for i, query := range queryVariants {
		result := NormalizeQuery(query)
		if result != expected {
			t.Errorf("Query variant %d normalized to %q, expected %q", i, result, expected)
		}
	}
}