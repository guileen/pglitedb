package sql

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
)

// TestExecutor_DropDatabaseStatement verifies that DROP DATABASE statements are properly handled
func TestExecutor_DropDatabaseStatement(t *testing.T) {
	// Create a planner and executor
	planner := NewPlanner(nil) // Use default parser
	executor := NewExecutor(planner)

	// Test DROP DATABASE statement type recognition
	tests := []struct {
		query    string
		expected parser.StatementType
	}{
		{"DROP DATABASE testdb", parser.DropDatabaseStatement},
		{"DROP DATABASE IF EXISTS testdb", parser.DropDatabaseStatement},
		{"drop database testdb", parser.DropDatabaseStatement},
		{"drop database if exists testdb", parser.DropDatabaseStatement},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			// Test that the parser correctly identifies DROP DATABASE statements
			// First parse the query to get the statement type
			stmtType := planner.parser.getStatementType(tt.query)
			// Debug print
			t.Logf("Query: %s, Expected: %d, Got: %d", tt.query, tt.expected, stmtType)
			assert.Equal(t, tt.expected, stmtType, "Query: %s", tt.query)

			// Test that the executor can handle the statement without returning "unsupported statement type: UNKNOWN"
			// Note: We don't expect the statement to actually execute successfully since we don't have a real database,
			// but we do expect it to be recognized as a valid statement type
			ctx := context.Background()
			result, err := executor.Execute(ctx, tt.query)
			// The error should not be "unsupported statement type: UNKNOWN"
			if err != nil {
				assert.NotContains(t, err.Error(), "unsupported statement type: UNKNOWN", "Query: %s", tt.query)
			} else {
				// Verify that we get a result with at least one column (our improvement)
				assert.NotNil(t, result)
				assert.GreaterOrEqual(t, len(result.Columns), 1, "Should have at least one column in result")
			}
		})
	}
}