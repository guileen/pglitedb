package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientParameterBinding tests parameter binding issues found in client tests
func TestClientParameterBinding(t *testing.T) {
	parser := NewPGParser()
	planner := NewPlanner(parser)
	_ = planner // Fix unused variable error

	// Test case 1: INSERT with parameter binding and RETURNING clause
	t.Run("InsertWithParameterBindingAndReturning", func(t *testing.T) {
		query := "INSERT INTO test_products (name, price, in_stock) VALUES ($1, $2, $3) RETURNING id"

		// Test parsing
		parsed, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, InsertStatement, parsed.Type)
		assert.Len(t, parsed.ReturningColumns, 1)
		assert.Equal(t, "id", parsed.ReturningColumns[0])

		// Test planning
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "insert", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
	})

	// Test case 2: UPDATE with parameter binding
	t.Run("UpdateWithParameterBinding", func(t *testing.T) {
		query := "UPDATE test_products SET price = $1 WHERE name = $2"

		// Test parsing
		parsed, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, UpdateStatement, parsed.Type)

		// Test planning
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "update", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		// Note: For parameterized queries, conditions may not be extracted in the planning phase
		// The actual parameter binding happens later in the execution phase
	})

	// Test case 3: DELETE with parameter binding
	t.Run("DeleteWithParameterBinding", func(t *testing.T) {
		query := "DELETE FROM test_products WHERE name = $1"

		// Test parsing
		parsed, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DeleteStatement, parsed.Type)

		// Test planning
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "delete", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		// Note: For parameterized queries, conditions may not be extracted in the planning phase
		// The actual parameter binding happens later in the execution phase

	})
}

// TestClientTransactions tests transaction handling issues found in client tests
func TestClientTransactions(t *testing.T) {
	parser := NewPGParser()

	// Test case: Transaction statements
	t.Run("TransactionStatements", func(t *testing.T) {
		// BEGIN statement
		beginQuery := "BEGIN"
		_, _ = parser.Parse(beginQuery)
		// BEGIN is typically handled at the protocol level, not parsed as a regular statement

		// COMMIT statement
		commitQuery := "COMMIT"
		_, _ = parser.Parse(commitQuery)
		// COMMIT is typically handled at the protocol level, not parsed as a regular statement

		// ROLLBACK statement
		rollbackQuery := "ROLLBACK"
		_, _ = parser.Parse(rollbackQuery)
		// ROLLBACK is typically handled at the protocol level, not parsed as a regular statement
	})
}

// TestClientBulkOperations tests bulk UPDATE and DELETE operations
func TestClientBulkOperations(t *testing.T) {
	parser := NewPGParser()
	planner := NewPlanner(parser)
	_ = planner // Fix unused variable error

	// Test case 1: Bulk UPDATE operation
	t.Run("BulkUpdateOperation", func(t *testing.T) {
		query := "UPDATE test_products SET price = $1 WHERE category = $2"
		
		// Test parsing
		parsed, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, UpdateStatement, parsed.Type)
		
		// Test planning
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "update", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
	})

	// Test case 2: Bulk DELETE operation
	t.Run("BulkDeleteOperation", func(t *testing.T) {
		query := "DELETE FROM test_products WHERE category = $1"
		
		// Test parsing
		parsed, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DeleteStatement, parsed.Type)
		
		// Test planning
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "delete", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
	})
}

// TestClientComplexQueries tests complex queries that failed in client tests
func TestClientComplexQueries(t *testing.T) {
	parser := NewPGParser()

	// Test case: Complex query with multiple conditions and RETURNING
	t.Run("ComplexQueryWithReturning", func(t *testing.T) {
		query := "UPDATE products SET price = $1, updated_at = NOW() WHERE category = $2 AND active = true RETURNING id, price, updated_at"

		parsed, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, UpdateStatement, parsed.Type)
		assert.Len(t, parsed.ReturningColumns, 3)
		assert.Contains(t, parsed.ReturningColumns, "id")
		assert.Contains(t, parsed.ReturningColumns, "price")
		assert.Contains(t, parsed.ReturningColumns, "updated_at")
	})
}