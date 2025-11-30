package sql

import (
	"testing"
	"github.com/guileen/pglitedb/protocol/sql/parser"
)

func TestSimpleParserEnhancedExtraction(t *testing.T) {
	sqlParser := NewSimplePGParser()
	planner := NewPlanner(sqlParser)

	tests := []struct {
		name             string
		query            string
		expectedType     parser.StatementType
		expectedTable    string
		expectedFields   []string
		expectedLimit    *int64
		hasConditions    bool
		hasOrderBy       bool
	}{
		{
			name:          "SELECT with table and fields",
			query:         "SELECT id, name FROM users",
			expectedType:  parser.SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"id", "name"},
		},
		{
			name:          "SELECT with WHERE clause",
			query:         "SELECT * FROM users WHERE age > 25",
			expectedType:  parser.SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"*"},
			hasConditions: true,
		},
		{
			name:          "SELECT with ORDER BY",
			query:         "SELECT id, name FROM users ORDER BY name DESC",
			expectedType:  parser.SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"id", "name"},
			hasOrderBy:    true,
		},
		{
			name:          "SELECT with LIMIT",
			query:         "SELECT * FROM users LIMIT 10",
			expectedType:  parser.SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"*"},
			expectedLimit: int64Ptr(10),
		},
		{
			name:          "Complex SELECT query",
			query:         "SELECT id, name, email FROM users WHERE age > 25 AND active = true ORDER BY created_at DESC LIMIT 100",
			expectedType:  parser.SelectStatement,
			expectedTable: "users",
			expectedFields: []string{"id", "name", "email"},
			expectedLimit: int64Ptr(100),
			hasConditions: true,
			hasOrderBy:    true,
		},
		{
			name:          "INSERT query",
			query:         "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
			expectedType:  parser.InsertStatement,
			expectedTable: "users",
		},
		{
			name:          "UPDATE query",
			query:         "UPDATE users SET name = 'Bob' WHERE id = 1",
			expectedType:  parser.UpdateStatement,
			expectedTable: "users",
		},
		{
			name:          "DELETE query",
			query:         "DELETE FROM users WHERE id = 1",
			expectedType:  parser.DeleteStatement,
			expectedTable: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a plan from the parsed query
			plan, err := planner.CreatePlan(tt.query)
			if err != nil {
				t.Fatalf("CreatePlan failed: %v", err)
			}

			if plan.Type != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, plan.Type)
			}

			if plan.Table != tt.expectedTable {
				t.Errorf("Expected table %v, got %v", tt.expectedTable, plan.Table)
			}

			if len(tt.expectedFields) > 0 {
				if len(plan.Fields) != len(tt.expectedFields) {
					t.Errorf("Expected %d fields, got %d", len(tt.expectedFields), len(plan.Fields))
					return
				}
				for i, expected := range tt.expectedFields {
					if plan.Fields[i] != expected {
						t.Errorf("Expected field %s at position %d, got %s", expected, i, plan.Fields[i])
					}
				}
			}

			if tt.expectedLimit != nil {
				if plan.Limit == nil {
					t.Error("Expected limit, got nil")
				} else if *plan.Limit != *tt.expectedLimit {
					t.Errorf("Expected limit %d, got %d", *tt.expectedLimit, *plan.Limit)
				}
			}

			if tt.hasConditions && len(plan.Conditions) == 0 {
				t.Error("Expected conditions, got none")
			}

			if tt.hasOrderBy && len(plan.OrderBy) == 0 {
				t.Error("Expected ORDER BY, got none")
			}
		})
	}
}

func int64Ptr(i int64) *int64 {
	return &i
}