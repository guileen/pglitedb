package sql

import (
	"testing"
	"time"
)

func TestHybridParser_Parse(t *testing.T) {
	parser := NewHybridPGParser()
	
	tests := []struct {
		name        string
		query       string
		expectedType StatementType
	}{
		{
			name:        "Simple SELECT",
			query:       "SELECT id, name FROM users",
			expectedType: SelectStatement,
		},
		{
			name:        "Simple INSERT",
			query:       "INSERT INTO users (name) VALUES ('Alice')",
			expectedType: InsertStatement,
		},
		{
			name:        "Simple UPDATE",
			query:       "UPDATE users SET name = 'Bob' WHERE id = 1",
			expectedType: UpdateStatement,
		},
		{
			name:        "Simple DELETE",
			query:       "DELETE FROM users WHERE id = 1",
			expectedType: DeleteStatement,
		},
		{
			name:        "BEGIN transaction",
			query:       "BEGIN",
			expectedType: BeginStatement,
		},
		{
			name:        "COMMIT transaction",
			query:       "COMMIT",
			expectedType: CommitStatement,
		},
		{
			name:        "ROLLBACK transaction",
			query:       "ROLLBACK",
			expectedType: RollbackStatement,
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
		})
	}
}

func TestHybridParser_Caching(t *testing.T) {
	parser := NewHybridPGParser()
	query := "SELECT id, name FROM users"
	
	// Parse the same query multiple times
	var firstParseTime, secondParseTime time.Duration
	
	// First parse
	start := time.Now()
	_, err1 := parser.Parse(query)
	firstParseTime = time.Since(start)
	
	if err1 != nil {
		t.Fatalf("First parse failed: %v", err1)
	}
	
	// Second parse (should be cached)
	start = time.Now()
	_, err2 := parser.Parse(query)
	secondParseTime = time.Since(start)
	
	if err2 != nil {
		t.Fatalf("Second parse failed: %v", err2)
	}
	
	// Second parse should be faster due to caching
	if secondParseTime >= firstParseTime {
		t.Logf("Warning: Second parse (%v) was not faster than first parse (%v)", secondParseTime, firstParseTime)
	}
	
	// Check stats
	stats := parser.GetStats()
	if stats["cache_hits"] != 1 {
		t.Errorf("Expected 1 cache hit, got %d", stats["cache_hits"])
	}
}

func TestHybridParser_ReturningColumns(t *testing.T) {
	parser := NewHybridPGParser()
	
	tests := []struct {
		name             string
		query            string
		expectedColumns  []string
	}{
		{
			name:            "INSERT with RETURNING *",
			query:           "INSERT INTO users (name) VALUES ('Alice') RETURNING *",
			expectedColumns: []string{"*"},
		},
		{
			name:            "INSERT with RETURNING specific columns",
			query:           "INSERT INTO users (name) VALUES ('Alice') RETURNING id, name",
			expectedColumns: []string{"id", "name"},
		},
		{
			name:            "UPDATE with RETURNING",
			query:           "UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING id, updated_at",
			expectedColumns: []string{"id", "updated_at"},
		},
		{
			name:            "DELETE with RETURNING",
			query:           "DELETE FROM users WHERE id = 1 RETURNING id, name, deleted_at",
			expectedColumns: []string{"id", "name", "deleted_at"},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			
			if len(parsed.ReturningColumns) != len(tt.expectedColumns) {
				t.Errorf("Expected %d returning columns, got %d", len(tt.expectedColumns), len(parsed.ReturningColumns))
				return
			}
			
			for i, expected := range tt.expectedColumns {
				if parsed.ReturningColumns[i] != expected {
					t.Errorf("Expected column %s at position %d, got %s", expected, i, parsed.ReturningColumns[i])
				}
			}
		})
	}
}