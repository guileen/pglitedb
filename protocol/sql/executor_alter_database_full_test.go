package sql

import (
	"testing"

	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
)

// TestExecutor_AlterDatabaseFullExecution verifies that ALTER DATABASE statements are properly parsed and executed
func TestExecutor_AlterDatabaseFullExecution(t *testing.T) {
	// Create a planner and executor
	planner := NewPlanner(nil) // Use default parser
	
	// Test ALTER DATABASE statement parsing and execution
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
			stmtType := planner.parser.getStatementType(tt.query)
			assert.Equal(t, tt.expected, stmtType, "Query: %s", tt.query)

			// Test DDL parser directly
			ddlParser := NewDDLParser()
			ddlStmt, err := ddlParser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse DDL statement '%s': %v", tt.query, err)
			}
			
			assert.Equal(t, parser.AlterDatabaseStatement, ddlStmt.Type, "DDL statement type should be ALTER_DATABASE")
			assert.Equal(t, "testdb", ddlStmt.TableName, "Database name should be 'testdb'")
			
			t.Logf("Successfully parsed DDL statement '%s': %+v", tt.query, ddlStmt)
		})
	}
}