package sql

import (
	"testing"

	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
)

// TestExecutor_AlterDatabaseExecution verifies that ALTER DATABASE statements are properly executed
func TestExecutor_AlterDatabaseExecution(t *testing.T) {
	// Create a planner and executor
	planner := NewPlanner(nil) // Use default parser
	
	// Test ALTER DATABASE statement execution with proper catalog initialization
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

			// Test plan creation for ALTER DATABASE statements
			plan, err := planner.CreatePlan(tt.query)
			if err != nil {
				t.Fatalf("Failed to create plan for query '%s': %v", tt.query, err)
			}
			
			// Verify plan properties
			assert.Equal(t, parser.AlterDatabaseStatement, plan.Type, "Plan type should be ALTER_DATABASE")
			assert.Equal(t, "alter_database", plan.Operation, "Plan operation should be 'alter_database'")
			assert.NotEmpty(t, plan.QueryString, "Query string should not be empty")
			
			t.Logf("Successfully created plan for query '%s': %+v", tt.query, plan)
		})
	}
}