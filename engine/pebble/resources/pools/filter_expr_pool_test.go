package pools

import (
	"testing"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/resources/leak"
	"github.com/stretchr/testify/assert"
)

func TestFilterExprPool_CachingEffectiveness(t *testing.T) {
	leakDetector := leak.NewDetector()
	poolManager := NewManager(leakDetector, nil)
	
	// Create a filter expression
	expr1 := poolManager.AcquireFilterExpr()
	assert.NotNil(t, expr1)
	
	// Set some values
	expr1.Type = "simple"
	expr1.Column = "test_column"
	expr1.Operator = "="
	expr1.Value = "test_value"
	
	// Release it back to the pool
	poolManager.ReleaseFilterExpr(expr1)
	
	// Acquire another one
	expr2 := poolManager.AcquireFilterExpr()
	assert.NotNil(t, expr2)
	
	// Should be reset to zero values
	assert.Equal(t, "", expr2.Type)
	assert.Equal(t, "", expr2.Column)
	assert.Equal(t, "", expr2.Operator)
	assert.Nil(t, expr2.Value)
}

func TestFilterExprPool_ComplexFilters(t *testing.T) {
	leakDetector := leak.NewDetector()
	poolManager := NewManager(leakDetector, nil)
	
	// Create a complex filter expression with children
	expr := poolManager.AcquireFilterExpr()
	expr.Type = "and"
	expr.Children = make([]*engineTypes.FilterExpression, 2)
	
	// Add child expressions
	child1 := poolManager.AcquireFilterExpr()
	child1.Type = "simple"
	child1.Column = "col1"
	child1.Operator = "="
	child1.Value = "value1"
	expr.Children[0] = child1
	
	child2 := poolManager.AcquireFilterExpr()
	child2.Type = "simple"
	child2.Column = "col2"
	child2.Operator = ">"
	child2.Value = 100
	expr.Children[1] = child2
	
	// Release the entire tree
	poolManager.ReleaseFilterExpr(expr)
	
	// Acquire a new expression - should be properly reset
	newExpr := poolManager.AcquireFilterExpr()
	assert.Equal(t, "", newExpr.Type)
	assert.Nil(t, newExpr.Children)
}