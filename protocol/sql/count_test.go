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
		
		// Check that we recognize this as a function call
		assert.Len(t, plan.Fields, 1)
		assert.Equal(t, "func:count", plan.Fields[0])
		
		// Check that aggregates are extracted
		assert.Len(t, plan.Aggregates, 1)
		assert.Equal(t, "COUNT", plan.Aggregates[0].Function)
		assert.Equal(t, "*", plan.Aggregates[0].Field)
		assert.Equal(t, "", plan.Aggregates[0].Alias)
	})

	// Test case: COUNT with alias
	t.Run("CountWithAlias", func(t *testing.T) {
		query := "SELECT COUNT(*) as total_count FROM test_products"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// Check that we recognize this as a function call
		assert.Len(t, plan.Fields, 1)
		assert.Equal(t, "func:count", plan.Fields[0])
		
		// Check that aggregates are extracted
		assert.Len(t, plan.Aggregates, 1)
		assert.Equal(t, "COUNT", plan.Aggregates[0].Function)
		assert.Equal(t, "*", plan.Aggregates[0].Field)
		assert.Equal(t, "total_count", plan.Aggregates[0].Alias)
	})

	// Test case: COUNT with specific column
	t.Run("CountSpecificColumn", func(t *testing.T) {
		query := "SELECT COUNT(price) FROM test_products"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// Check that we recognize this as a function call
		assert.Len(t, plan.Fields, 1)
		assert.Equal(t, "func:count", plan.Fields[0])
		
		// Check that aggregates are extracted
		assert.Len(t, plan.Aggregates, 1)
		assert.Equal(t, "COUNT", plan.Aggregates[0].Function)
		assert.Equal(t, "price", plan.Aggregates[0].Field)
		assert.Equal(t, "", plan.Aggregates[0].Alias)
	})

	// Test case: COUNT with GROUP BY
	t.Run("CountWithGroupBy", func(t *testing.T) {
		query := "SELECT category, COUNT(*) as count FROM test_products GROUP BY category"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// Check fields
		assert.Len(t, plan.Fields, 2)
		assert.Equal(t, "category", plan.Fields[0])
		assert.Equal(t, "func:count", plan.Fields[1])
		
		// Check that aggregates are extracted
		assert.Len(t, plan.Aggregates, 1)
		assert.Equal(t, "COUNT", plan.Aggregates[0].Function)
		assert.Equal(t, "*", plan.Aggregates[0].Field)
		assert.Equal(t, "count", plan.Aggregates[0].Alias)
		
		// Check that GROUP BY is extracted
		assert.Len(t, plan.GroupBy, 1)
		assert.Equal(t, "category", plan.GroupBy[0])
	})
}