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

// FilterExprSlicePool manages slices of filter expressions
type FilterExprSlicePool struct {
	BasePool
}

// NewFilterExprSlicePool creates a new filter expression slice pool
func NewFilterExprSlicePool() *FilterExprSlicePool {
	return &FilterExprSlicePool{
		BasePool: *NewBasePool("filterExprSlice", func() interface{} {
			return make([]*engineTypes.FilterExpression, 0, 4) // Small capacity for typical use cases
		}),
	}
}

// AcquireFilterExprSlice gets a filter expression slice from the pool
func (fesp *FilterExprSlicePool) AcquireFilterExprSlice() []*engineTypes.FilterExpression {
	slice := fesp.BasePool.pool.Get()
	fromPool := slice != nil

	if !fromPool {
		return make([]*engineTypes.FilterExpression, 0, 4)
	}

	return slice.([]*engineTypes.FilterExpression)
}

// ReleaseFilterExprSlice returns a filter expression slice to the pool
func (fesp *FilterExprSlicePool) ReleaseFilterExprSlice(slice []*engineTypes.FilterExpression) {
	// Clear the slice without reallocating
	for i := range slice {
		slice[i] = nil
	}
	slice = slice[:0]

	fesp.BasePool.Put(slice)
}