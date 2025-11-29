package pools

import (
	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// FilterExprPool manages filter expression resources
type FilterExprPool struct {
	BasePool
}

// NewFilterExprPool creates a new filter expression pool
func NewFilterExprPool() *FilterExprPool {
	return &FilterExprPool{
		BasePool: *NewBasePool("filterExpr", func() interface{} {
			return &engineTypes.FilterExpression{}
		}),
	}
}

// Acquire gets a filter expression from the pool
func (fep *FilterExprPool) Acquire() *engineTypes.FilterExpression {
	expr := fep.BasePool.pool.Get()
	fromPool := expr != nil

	if !fromPool {
		expr = &engineTypes.FilterExpression{}
	}

	return expr.(*engineTypes.FilterExpression)
}

// Release returns a filter expression to the pool
func (fep *FilterExprPool) Release(expr *engineTypes.FilterExpression) {
	*expr = engineTypes.FilterExpression{} // Reset to zero value
	fep.BasePool.Put(expr)
}