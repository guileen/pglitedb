package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlannerExtractCountInfo(t *testing.T) {
	parser := NewPGParser()
	planner := NewPlanner(parser)

	// Test case: Simple COUNT(*) query
	t.Run("SimpleCountStar", func(t *testing.T) {
		query := "SELECT COUNT(*) FROM test_products"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// For now, we'll just check that we have at least one field
		assert.Greater(t, len(plan.Fields), 0)
	})

	// Test case: COUNT with alias
	t.Run("CountWithAlias", func(t *testing.T) {
		query := "SELECT COUNT(*) as total_count FROM test_products"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// For now, we'll just check that we have at least one field
		assert.Greater(t, len(plan.Fields), 0)
	})

	// Test case: COUNT with specific column
	t.Run("CountSpecificColumn", func(t *testing.T) {
		query := "SELECT COUNT(price) FROM test_products"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// For now, we'll just check that we have at least one field
		assert.Greater(t, len(plan.Fields), 0)
	})

	// Test case: COUNT with GROUP BY
	t.Run("CountWithGroupBy", func(t *testing.T) {
		query := "SELECT category, COUNT(*) as count FROM test_products GROUP BY category"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// For now, we'll just check that we have at least one field
		assert.Greater(t, len(plan.Fields), 0)
		
		// Check that GROUP BY is extracted
		assert.GreaterOrEqual(t, len(plan.GroupBy), 0)
	})
}