package sql

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
)

// TestExecutor_AlterDatabaseStatement verifies that ALTER DATABASE statements are properly handled
func TestExecutor_AlterDatabaseStatement(t *testing.T) {
	// Create a planner and executor
	planner := NewPlanner(nil) // Use default parser
	executor := NewExecutor(planner)

	// Test ALTER DATABASE statement type recognition
	tests := []struct {
		query    string
		expected parser.StatementType
	}{
		{"ALTER DATABASE testdb SET timezone TO 'UTC'", parser.AlterDatabaseStatement},
		{"ALTER DATABASE testdb OWNER TO newowner", parser.AlterDatabaseStatement},
		{"ALTER DATABASE testdb REFRESH COLLATION VERSION", parser.AlterDatabaseStatement},
		{"alter database testdb set timezone to 'UTC'", parser.AlterDatabaseStatement},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			// Test that the parser correctly identifies ALTER DATABASE statements
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
				// Also verify that we get a proper error message
				t.Logf("Error for query '%s': %v", tt.query, err)
			} else {
				// Verify that we get a result
				assert.NotNil(t, result)
				t.Logf("Success for query '%s': %+v", tt.query, result)
			}
		})
	}
}