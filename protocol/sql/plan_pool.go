// Package sql provides SQL parsing and execution functionality for PGLiteDB
package sql

import (
	"sync"
	"sync/atomic"
)

// PlanPool is a specialized pool for Plan objects and their associated slices
type PlanPool struct {
	planPool       *sync.Pool
	stringSlicePool *sync.Pool
	conditionSlicePool *sync.Pool
	orderBySlicePool *sync.Pool
	metrics        atomicMetrics
}

// NewPlanPool creates a new PlanPool
func NewPlanPool() *PlanPool {
	return &PlanPool{
		planPool: &sync.Pool{
			New: func() interface{} {
				return &Plan{}
			},
		},
		stringSlicePool: &sync.Pool{
			New: func() interface{} {
				slice := make([]string, 0, 8)
				return &slice
			},
		},
		conditionSlicePool: &sync.Pool{
			New: func() interface{} {
				slice := make([]Condition, 0, 4)
				return &slice
			},
		},
		orderBySlicePool: &sync.Pool{
			New: func() interface{} {
				slice := make([]OrderBy, 0, 4)
				return &slice
			},
		},
	}
}

// GetPlan retrieves a Plan from the pool
func (pp *PlanPool) GetPlan() *Plan {
	atomic.AddInt64(&pp.metrics.Gets, 1)
	plan := pp.planPool.Get().(*Plan)
	if plan == nil {
		atomic.AddInt64(&pp.metrics.Misses, 1)
		return &Plan{}
	}
	atomic.AddInt64(&pp.metrics.Hits, 1)
	// Reset the plan for reuse
	pp.ResetPlan(plan)
	return plan
}

// PutPlan returns a Plan to the pool
func (pp *PlanPool) PutPlan(plan *Plan) {
	if plan == nil {
		return
	}
	atomic.AddInt64(&pp.metrics.Puts, 1)
	// Only put back plans that aren't too large to avoid memory bloat
	if len(plan.Fields) <= 32 && len(plan.Conditions) <= 16 {
		pp.planPool.Put(plan)
	}
}

// GetStringSlice retrieves a string slice from the pool
func (pp *PlanPool) GetStringSlice(capacity int) *[]string {
	atomic.AddInt64(&pp.metrics.Gets, 1)
	slicePtr := pp.stringSlicePool.Get().(*[]string)
	if slicePtr == nil {
		atomic.AddInt64(&pp.metrics.Misses, 1)
		slice := make([]string, 0, capacity)
		return &slice
	}
	atomic.AddInt64(&pp.metrics.Hits, 1)
	slice := *slicePtr
	// Ensure capacity is sufficient
	if cap(slice) < capacity {
		slice = make([]string, 0, capacity)
	} else {
		slice = slice[:0] // Reset length but keep capacity
	}
	return &slice
}

// PutStringSlice returns a string slice to the pool
func (pp *PlanPool) PutStringSlice(slice *[]string) {
	if slice == nil {
		return
	}
	atomic.AddInt64(&pp.metrics.Puts, 1)
	// Only put back slices that aren't too large to avoid memory bloat
	if cap(*slice) <= 64 {
		*slice = (*slice)[:0] // Reset length
		pp.stringSlicePool.Put(slice)
	}
}

// GetConditionSlice retrieves a Condition slice from the pool
func (pp *PlanPool) GetConditionSlice(capacity int) *[]Condition {
	atomic.AddInt64(&pp.metrics.Gets, 1)
	slicePtr := pp.conditionSlicePool.Get().(*[]Condition)
	if slicePtr == nil {
		atomic.AddInt64(&pp.metrics.Misses, 1)
		slice := make([]Condition, 0, capacity)
		return &slice
	}
	atomic.AddInt64(&pp.metrics.Hits, 1)
	slice := *slicePtr
	// Ensure capacity is sufficient
	if cap(slice) < capacity {
		slice = make([]Condition, 0, capacity)
	} else {
		slice = slice[:0] // Reset length but keep capacity
	}
	return &slice
}

// PutConditionSlice returns a Condition slice to the pool
func (pp *PlanPool) PutConditionSlice(slice *[]Condition) {
	if slice == nil {
		return
	}
	atomic.AddInt64(&pp.metrics.Puts, 1)
	// Only put back slices that aren't too large to avoid memory bloat
	if cap(*slice) <= 32 {
		*slice = (*slice)[:0] // Reset length
		pp.conditionSlicePool.Put(slice)
	}
}

// GetOrderBySlice retrieves an OrderBy slice from the pool
func (pp *PlanPool) GetOrderBySlice(capacity int) *[]OrderBy {
	atomic.AddInt64(&pp.metrics.Gets, 1)
	slicePtr := pp.orderBySlicePool.Get().(*[]OrderBy)
	if slicePtr == nil {
		atomic.AddInt64(&pp.metrics.Misses, 1)
		slice := make([]OrderBy, 0, capacity)
		return &slice
	}
	atomic.AddInt64(&pp.metrics.Hits, 1)
	slice := *slicePtr
	// Ensure capacity is sufficient
	if cap(slice) < capacity {
		slice = make([]OrderBy, 0, capacity)
	} else {
		slice = slice[:0] // Reset length but keep capacity
	}
	return &slice
}

// PutOrderBySlice returns an OrderBy slice to the pool
func (pp *PlanPool) PutOrderBySlice(slice *[]OrderBy) {
	if slice == nil {
		return
	}
	atomic.AddInt64(&pp.metrics.Puts, 1)
	// Only put back slices that aren't too large to avoid memory bloat
	if cap(*slice) <= 16 {
		*slice = (*slice)[:0] // Reset length
		pp.orderBySlicePool.Put(slice)
	}
}

// ResetPlan resets a Plan for reuse
func (pp *PlanPool) ResetPlan(plan *Plan) {
	// Reset all fields to zero values
	plan.Type = 0
	plan.Operation = ""
	plan.Table = ""
	plan.Fields = plan.Fields[:0]
	plan.Conditions = plan.Conditions[:0]
	plan.Limit = nil
	plan.Offset = nil
	plan.OrderBy = plan.OrderBy[:0]
	plan.GroupBy = plan.GroupBy[:0]
	plan.Aggregates = plan.Aggregates[:0]
	plan.QueryString = ""
	// Reset maps
	for k := range plan.Values {
		delete(plan.Values, k)
	}
	for k := range plan.Updates {
		delete(plan.Updates, k)
	}
}

// Metrics returns the current pool metrics
func (pp *PlanPool) Metrics() PoolMetrics {
	return PoolMetrics{
		Gets:   atomic.LoadInt64(&pp.metrics.Gets),
		Puts:   atomic.LoadInt64(&pp.metrics.Puts),
		Hits:   atomic.LoadInt64(&pp.metrics.Hits),
		Misses: atomic.LoadInt64(&pp.metrics.Misses),
		Size:   int64(0), // sync.Pool doesn't expose size
	}
}

// PoolMetrics tracks pool performance and health
type PoolMetrics struct {
	Gets      int64 // Number of Get() operations
	Puts      int64 // Number of Put() operations
	Hits      int64 // Number of successful reuse operations
	Misses    int64 // Number of allocations due to pool miss
	Size      int64 // Current pool size
}

// atomicMetrics provides atomic operations for metrics
type atomicMetrics struct {
	Gets   int64
	Puts   int64
	Hits   int64
	Misses int64
}