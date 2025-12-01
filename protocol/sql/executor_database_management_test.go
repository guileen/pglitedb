package sql

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
)

// TestExecutor_DatabaseManagementStatements verifies that CREATE DATABASE and DROP DATABASE statements are properly handled
func TestExecutor_DatabaseManagementStatements(t *testing.T) {
	// Create a planner and executor
	planner := NewPlanner(nil) // Use default parser
	executor := NewExecutor(planner)

	// Test CREATE DATABASE statement type recognition
	createTests := []struct {
		query    string
		expected parser.StatementType
	}{
		{"CREATE DATABASE testdb", parser.CreateDatabaseStatement},
		{"CREATE DATABASE IF NOT EXISTS testdb", parser.CreateDatabaseStatement},
		{"create database testdb", parser.CreateDatabaseStatement},
		{"create database if not exists testdb", parser.CreateDatabaseStatement},
	}

	for _, tt := range createTests {
		t.Run("Create_"+tt.query, func(t *testing.T) {
			// Test that the parser correctly identifies CREATE DATABASE statements
			stmtType := planner.parser.getStatementType(tt.query)
			assert.Equal(t, tt.expected, stmtType, "Query: %s", tt.query)

			// Test that the executor can handle the statement without returning "unsupported statement type: UNKNOWN"
			ctx := context.Background()
			result, err := executor.Execute(ctx, tt.query)
			// The error should not be "unsupported statement type: UNKNOWN"
			if err != nil {
				assert.NotContains(t, err.Error(), "unsupported statement type: UNKNOWN", "Query: %s", tt.query)
			} else {
				// Verify that we get a result
				assert.NotNil(t, result)
			}
		})
	}

	// Test DROP DATABASE statement type recognition
	dropTests := []struct {
		query    string
		expected parser.StatementType
	}{
		{"DROP DATABASE testdb", parser.DropDatabaseStatement},
		{"DROP DATABASE IF EXISTS testdb", parser.DropDatabaseStatement},
		{"drop database testdb", parser.DropDatabaseStatement},
		{"drop database if exists testdb", parser.DropDatabaseStatement},
	}

	for _, tt := range dropTests {
		t.Run("Drop_"+tt.query, func(t *testing.T) {
			// Test that the parser correctly identifies DROP DATABASE statements
			stmtType := planner.parser.getStatementType(tt.query)
			assert.Equal(t, tt.expected, stmtType, "Query: %s", tt.query)

			// Test that the executor can handle the statement without returning "unsupported statement type: UNKNOWN"
			ctx := context.Background()
			result, err := executor.Execute(ctx, tt.query)
			// The error should not be "unsupported statement type: UNKNOWN"
			if err != nil {
				assert.NotContains(t, err.Error(), "unsupported statement type: UNKNOWN", "Query: %s", tt.query)
			} else {
				// Verify that we get a result
				assert.NotNil(t, result)
			}
		})
	}
}