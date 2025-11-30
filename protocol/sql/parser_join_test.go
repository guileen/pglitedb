package sql

import (
	"testing"
)

func TestSimpleParserJoinParsing(t *testing.T) {
	parser := NewSimplePGParser()

	tests := []struct {
		name           string
		query          string
		expectJoins    bool
		expectedJoins  int
		expectedTable  string
		expectedFields int
	}{
		{
			name:          "Simple SELECT without JOIN",
			query:         "SELECT id, name FROM users",
			expectJoins:   false,
			expectedTable: "users",
			expectedFields: 2,
		},
		{
			name:          "SELECT with INNER JOIN",
			query:         "SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id",
			expectJoins:   true,
			expectedJoins: 1,
			expectedTable: "users",
			expectedFields: 2,
		},
		{
			name:          "SELECT with JOIN (implicit INNER)",
			query:         "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
			expectJoins:   true,
			expectedJoins: 1,
			expectedTable: "users",
			expectedFields: 2,
		},
		{
			name:          "SELECT with LEFT JOIN",
			query:         "SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id",
			expectJoins:   true,
			expectedJoins: 1,
			expectedTable: "users",
			expectedFields: 2,
		},
		{
			name:          "SELECT with multiple JOINs",
			query:         "SELECT u.name, p.title, c.name FROM users u JOIN posts p ON u.id = p.user_id JOIN categories c ON p.category_id = c.id",
			expectJoins:   true,
			expectedJoins: 2,
			expectedTable: "users",
			expectedFields: 3,
		},
		{
			name:          "SELECT with JOIN without ON clause",
			query:         "SELECT u.name FROM users u JOIN posts p",
			expectJoins:   true,
			expectedJoins: 1,
			expectedTable: "users",
			expectedFields: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			if tt.expectJoins {
				if len(parsed.Joins) != tt.expectedJoins {
					t.Errorf("Expected %d JOINs, got %d", tt.expectedJoins, len(parsed.Joins))
				}
				
				if len(parsed.Joins) > 0 {
					firstJoin := parsed.Joins[0]
					if firstJoin.LeftTable != tt.expectedTable {
						t.Errorf("Expected left table %s, got %s", tt.expectedTable, firstJoin.LeftTable)
					}
				}
			} else {
				if len(parsed.Joins) != 0 {
					t.Errorf("Expected no JOINs, got %d", len(parsed.Joins))
				}
			}

			if parsed.Table != tt.expectedTable {
				t.Errorf("Expected table %s, got %s", tt.expectedTable, parsed.Table)
			}

			if len(parsed.Fields) != tt.expectedFields {
				t.Errorf("Expected %d fields, got %d", tt.expectedFields, len(parsed.Fields))
			}
		})
	}
}

func TestSimpleParserJoinDetails(t *testing.T) {
	parser := NewSimplePGParser()

	query := "SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id WHERE u.age > 25"
	parsed, err := parser.Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(parsed.Joins) != 1 {
		t.Fatalf("Expected 1 JOIN, got %d", len(parsed.Joins))
	}

	join := parsed.Joins[0]
	if join.Type != "INNER" {
		t.Errorf("Expected JOIN type INNER, got %s", join.Type)
	}

	if join.LeftTable != "users" {
		t.Errorf("Expected left table users, got %s", join.LeftTable)
	}

	if join.RightTable != "posts" {
		t.Errorf("Expected right table posts, got %s", join.RightTable)
	}

	if join.Condition != "u.id = p.user_id" {
		t.Errorf("Expected condition 'u.id = p.user_id', got '%s'", join.Condition)
	}

	// Check that WHERE clause is still parsed correctly
	if len(parsed.Conditions) != 1 {
		t.Fatalf("Expected 1 condition, got %d", len(parsed.Conditions))
	}

	condition := parsed.Conditions[0]
	if condition.Field != "u.age" {
		t.Errorf("Expected condition field 'u.age', got '%s'", condition.Field)
	}

	if condition.Operator != ">" {
		t.Errorf("Expected operator '>', got '%s'", condition.Operator)
	}

	if condition.Value != int64(25) {
		t.Errorf("Expected value 25, got %v", condition.Value)
	}
}

func TestSimpleParserLeftJoin(t *testing.T) {
	parser := NewSimplePGParser()

	query := "SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id"
	parsed, err := parser.Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(parsed.Joins) != 1 {
		t.Fatalf("Expected 1 JOIN, got %d", len(parsed.Joins))
	}

	join := parsed.Joins[0]
	if join.Type != "LEFT" {
		t.Errorf("Expected JOIN type LEFT, got %s", join.Type)
	}

	if join.LeftTable != "users" {
		t.Errorf("Expected left table users, got %s", join.LeftTable)
	}

	if join.RightTable != "posts" {
		t.Errorf("Expected right table posts, got %s", join.RightTable)
	}

	if join.Condition != "u.id = p.user_id" {
		t.Errorf("Expected condition 'u.id = p.user_id', got '%s'", join.Condition)
	}
}