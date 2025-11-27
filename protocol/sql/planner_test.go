package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlannerExtractUpdateInfo(t *testing.T) {
	parser := NewPGParser()
	planner := NewPlanner(parser)

	// Test case: UPDATE with constant values
	t.Run("UpdateWithConstants", func(t *testing.T) {
		query := "UPDATE test_products SET price = 999.99, active = true WHERE id = 1"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "update", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// Check extracted values
		assert.Len(t, plan.Updates, 2)
		assert.Equal(t, 999.99, plan.Updates["price"])
		assert.Equal(t, true, plan.Updates["active"])
		
		// Check extracted conditions
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "id", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		assert.Equal(t, int32(1), plan.Conditions[0].Value)
	})

	// Test case: UPDATE with parameter placeholders
	t.Run("UpdateWithParameters", func(t *testing.T) {
		query := "UPDATE test_products SET price = $1, name = $2 WHERE id = $3"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "update", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// Check extracted parameter placeholders
		assert.Len(t, plan.Updates, 2)
		assert.Equal(t, "$1", plan.Updates["price"])
		assert.Equal(t, "$2", plan.Updates["name"])
		
		// Check extracted conditions
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "id", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		assert.Equal(t, "$3", plan.Conditions[0].Value)
	})
}

func TestPlannerExtractDeleteInfo(t *testing.T) {
	parser := NewPGParser()
	planner := NewPlanner(parser)

	// Test case: DELETE with constant conditions
	t.Run("DeleteWithConstants", func(t *testing.T) {
		query := "DELETE FROM test_products WHERE category = 'electronics' AND price > 100"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "delete", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// Check extracted conditions
		assert.Len(t, plan.Conditions, 2)
		assert.Equal(t, "category", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		assert.Equal(t, "electronics", plan.Conditions[0].Value)
		assert.Equal(t, "price", plan.Conditions[1].Field)
		assert.Equal(t, ">", plan.Conditions[1].Operator)
		assert.Equal(t, int32(100), plan.Conditions[1].Value)
	})

	// Test case: DELETE with parameter placeholders
	t.Run("DeleteWithParameters", func(t *testing.T) {
		query := "DELETE FROM test_products WHERE id = $1 AND category = $2"
		
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, "delete", plan.Operation)
		assert.Equal(t, "test_products", plan.Table)
		
		// Check extracted conditions
		assert.Len(t, plan.Conditions, 2)
		assert.Equal(t, "id", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		assert.Equal(t, "$1", plan.Conditions[0].Value)
		assert.Equal(t, "category", plan.Conditions[1].Field)
		assert.Equal(t, "=", plan.Conditions[1].Operator)
		assert.Equal(t, "$2", plan.Conditions[1].Value)
	})
}