package sql

import (
	"testing"
)

func TestSimpleParserEnhancedExtraction(t *testing.T) {
	parser := NewSimplePGParser()

	tests := []struct {
		name             string
		query            string
		expectedType     StatementType
		expectedTable    string
		expectedFields   []string
		expectedLimit    *int64
		hasConditions    bool
		hasOrderBy       bool
	}{
		{
			name:          "SELECT with table and fields",
			query:         "SELECT id, name FROM users",
			expectedType:  SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"id", "name"},
		},
		{
			name:          "SELECT with WHERE clause",
			query:         "SELECT * FROM users WHERE age > 25",
			expectedType:  SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"*"},
			hasConditions: true,
		},
		{
			name:          "SELECT with ORDER BY",
			query:         "SELECT id, name FROM users ORDER BY name DESC",
			expectedType:  SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"id", "name"},
			hasOrderBy:    true,
		},
		{
			name:          "SELECT with LIMIT",
			query:         "SELECT * FROM users LIMIT 10",
			expectedType:  SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"*"},
			expectedLimit: int64Ptr(10),
		},
		{
			name:          "Complex SELECT query",
			query:         "SELECT id, name, email FROM users WHERE age > 25 AND active = true ORDER BY created_at DESC LIMIT 100",
			expectedType:  SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"id", "name", "email"},
			expectedLimit: int64Ptr(100),
			hasConditions: true,
			hasOrderBy:    true,
		},
		{
			name:          "INSERT query",
			query:         "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
			expectedType:  InsertStatement,
			expectedTable: "users",
		},
		{
			name:          "UPDATE query",
			query:         "UPDATE users SET name = 'Bob' WHERE id = 1",
			expectedType:  UpdateStatement,
			expectedTable: "users",
		},
		{
			name:          "DELETE query",
			query:         "DELETE FROM users WHERE id = 1",
			expectedType:  DeleteStatement,
			expectedTable: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			if parsed.Type != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, parsed.Type)
			}

			if parsed.Table != tt.expectedTable {
				t.Errorf("Expected table %v, got %v", tt.expectedTable, parsed.Table)
			}

			if len(tt.expectedFields) > 0 {
				if len(parsed.Fields) != len(tt.expectedFields) {
					t.Errorf("Expected %d fields, got %d", len(tt.expectedFields), len(parsed.Fields))
					return
				}
				for i, expected := range tt.expectedFields {
					if parsed.Fields[i] != expected {
						t.Errorf("Expected field %s at position %d, got %s", expected, i, parsed.Fields[i])
					}
				}
			}

			if tt.expectedLimit != nil {
				if parsed.Limit == nil {
					t.Error("Expected limit, got nil")
				} else if *parsed.Limit != *tt.expectedLimit {
					t.Errorf("Expected limit %d, got %d", *tt.expectedLimit, *parsed.Limit)
				}
			}

			if tt.hasConditions && len(parsed.Conditions) == 0 {
				t.Error("Expected conditions, got none")
			}

			if tt.hasOrderBy && len(parsed.OrderBy) == 0 {
				t.Error("Expected ORDER BY, got none")
			}
		})
	}
}

func int64Ptr(i int64) *int64 {
	return &i
}