// Enhanced memory pooling for various database objects with adaptive sizing
package types

import (
	"sync"
	"sync/atomic"
)

// ResultSet is the internal result set type used by the executor
type ResultSet struct {
	Columns      []string
	Rows         [][]interface{}
	Count        int
	LastInsertID int64
}

// PoolMetrics tracks usage statistics for a memory pool
type PoolMetrics struct {
	Gets   uint64 // Number of times objects were acquired
	Puts   uint64 // Number of times objects were released
	Hits   uint64 // Number of times a pooled object was available
	Misses uint64 // Number of times a new object had to be created
}

// AdaptiveMemoryPool extends sync.Pool with adaptive sizing capabilities
type AdaptiveMemoryPool struct {
	pool     sync.Pool
	metrics  PoolMetrics
	config   MemoryPoolConfig
	poolType string
}

// GlobalMemoryPool manages pools of various database objects with adaptive sizing
type GlobalMemoryPool struct {
	config MemoryPoolConfig
	
	recordPool            *AdaptiveMemoryPool
	resultSetPool         *AdaptiveMemoryPool
	executorResultSetPool *AdaptiveMemoryPool
	valueSlicePool        *AdaptiveMemoryPool
	stringSlicePool       *AdaptiveMemoryPool
	interfaceSlicePool    *AdaptiveMemoryPool
	
	// Mutex to protect pool resizing operations
	mu sync.RWMutex
}

// Global memory pool instance
var globalMemoryPool *GlobalMemoryPool

// Initialize the global memory pool with default configuration
func init() {
	config := LoadMemoryPoolConfig()
	InitializeMemoryPool(config)
}

// InitializeMemoryPool initializes the global memory pool with the given configuration
func InitializeMemoryPool(config MemoryPoolConfig) {
	globalMemoryPool = &GlobalMemoryPool{
		config: config,
		recordPool: &AdaptiveMemoryPool{
			pool: sync.Pool{
				New: func() interface{} {
					return &Record{
						Data: make(map[string]*Value, config.RecordInitialCap),
					}
				},
			},
			config:   config,
			poolType: "record",
		},
		resultSetPool: &AdaptiveMemoryPool{
			pool: sync.Pool{
				New: func() interface{} {
					return &QueryResult{
						Columns: make([]ColumnInfo, 0, config.ResultSetInitialCap),
						Rows:    make([][]interface{}, 0, config.ResultSetInitialCap*2),
					}
				},
			},
			config:   config,
			poolType: "result_set",
		},
		executorResultSetPool: &AdaptiveMemoryPool{
			pool: sync.Pool{
				New: func() interface{} {
					return &ResultSet{
						Columns: make([]string, 0, config.ResultSetInitialCap),
						Rows:    make([][]interface{}, 0, config.ResultSetInitialCap*2),
					}
				},
			},
			config:   config,
			poolType: "executor_result_set",
		},
		valueSlicePool: &AdaptiveMemoryPool{
			pool: sync.Pool{
				New: func() interface{} {
					slice := make([]*Value, 0, config.ValueSliceInitialCap)
					return &slice
				},
			},
			config:   config,
			poolType: "value_slice",
		},
		stringSlicePool: &AdaptiveMemoryPool{
			pool: sync.Pool{
				New: func() interface{} {
					slice := make([]string, 0, config.StringSliceInitialCap)
					return &slice
				},
			},
			config:   config,
			poolType: "string_slice",
		},
		interfaceSlicePool: &AdaptiveMemoryPool{
			pool: sync.Pool{
				New: func() interface{} {
					slice := make([]interface{}, 0, config.InterfaceSliceInitialCap)
					return &slice
				},
			},
			config:   config,
			poolType: "interface_slice",
		},
	}
}

// GetMetrics returns the current metrics for all pools
func (gmp *GlobalMemoryPool) GetMetrics() map[string]PoolMetrics {
	gmp.mu.RLock()
	defer gmp.mu.RUnlock()
	
	metrics := make(map[string]PoolMetrics)
	metrics["record"] = gmp.recordPool.metrics
	metrics["result_set"] = gmp.resultSetPool.metrics
	metrics["executor_result_set"] = gmp.executorResultSetPool.metrics
	metrics["value_slice"] = gmp.valueSlicePool.metrics
	metrics["string_slice"] = gmp.stringSlicePool.metrics
	metrics["interface_slice"] = gmp.interfaceSlicePool.metrics
	
	return metrics
}

// checkAndResize checks if a pool needs resizing based on usage metrics
func (amp *AdaptiveMemoryPool) checkAndResize() {
	if !amp.config.EnableAdaptiveSizing {
		return
	}
	
	// Get current metrics atomically
	gets := atomic.LoadUint64(&amp.metrics.Gets)
	hits := atomic.LoadUint64(&amp.metrics.Hits)
	
	// Only resize if we have sufficient data
	if gets < 100 {
		return
	}
	
	// Calculate hit rate
	hitRate := float64(hits) / float64(gets)
	
	// If hit rate is below threshold, consider resizing
	if hitRate < amp.config.ResizeThreshold {
		// Calculate resize factor based on miss rate
		missRate := 1.0 - hitRate
		resizeFactor := 1.0 + missRate
		
		// Clamp resize factor to configured limits
		if resizeFactor > amp.config.MaxResizeFactor {
			resizeFactor = amp.config.MaxResizeFactor
		}
		if resizeFactor < amp.config.MinResizeFactor {
			resizeFactor = amp.config.MinResizeFactor
		}
		
		// TODO: Implement actual pool resizing logic
		// This would involve creating a new pool with adjusted initial capacities
		// For now, we just log the need to resize
		_ = resizeFactor
	}
}

// AcquireRecord gets a Record from the pool
func AcquireRecord() *Record {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	amp := globalMemoryPool.recordPool
	atomic.AddUint64(&amp.metrics.Gets, 1)
	
	obj := amp.pool.Get()
	if obj != nil {
		atomic.AddUint64(&amp.metrics.Hits, 1)
		r := obj.(*Record)
		r.ID = ""
		r.Table = ""
		return r
	}
	
	atomic.AddUint64(&amp.metrics.Misses, 1)
	// Create a new record with the configured initial capacity
	r := &Record{
		Data: make(map[string]*Value, globalMemoryPool.config.RecordInitialCap),
	}
	return r
}

// ReleaseRecord returns a Record to the pool
func ReleaseRecord(r *Record) {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	// Clear the map without reallocating
	for k := range r.Data {
		delete(r.Data, k)
	}
	
	amp := globalMemoryPool.recordPool
	atomic.AddUint64(&amp.metrics.Puts, 1)
	amp.pool.Put(r)
	
	// Check if we need to resize the pool
	amp.checkAndResize()
}

// AcquireResultSet gets a QueryResult from the pool
func AcquireResultSet() *QueryResult {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	amp := globalMemoryPool.resultSetPool
	atomic.AddUint64(&amp.metrics.Gets, 1)
	
	obj := amp.pool.Get()
	if obj != nil {
		atomic.AddUint64(&amp.metrics.Hits, 1)
		rs := obj.(*QueryResult)
		// Reset fields but keep underlying slices
		rs.Columns = rs.Columns[:0]
		rs.Rows = rs.Rows[:0]
		rs.Count = 0
		rs.LastInsertID = 0
		return rs
	}
	
	atomic.AddUint64(&amp.metrics.Misses, 1)
	// Create a new result set with configured initial capacities
	rs := &QueryResult{
		Columns: make([]ColumnInfo, 0, globalMemoryPool.config.ResultSetInitialCap),
		Rows:    make([][]interface{}, 0, globalMemoryPool.config.ResultSetInitialCap*2),
	}
	return rs
}

// ReleaseResultSet returns a QueryResult to the pool
func ReleaseResultSet(rs *QueryResult) {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	// Clear slices without reallocating
	for i := range rs.Rows {
		// Release individual row elements if they are poolable
		rs.Rows[i] = nil
	}
	rs.Rows = rs.Rows[:0]
	rs.Columns = rs.Columns[:0]
	
	amp := globalMemoryPool.resultSetPool
	atomic.AddUint64(&amp.metrics.Puts, 1)
	amp.pool.Put(rs)
	
	// Check if we need to resize the pool
	amp.checkAndResize()
}

// AcquireExecutorResultSet gets a ResultSet from the pool for use by the executor
func AcquireExecutorResultSet() *ResultSet {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	amp := globalMemoryPool.executorResultSetPool
	atomic.AddUint64(&amp.metrics.Gets, 1)
	
	obj := amp.pool.Get()
	if obj != nil {
		atomic.AddUint64(&amp.metrics.Hits, 1)
		rs := obj.(*ResultSet)
		// Reset fields but keep underlying slices
		rs.Columns = rs.Columns[:0]
		rs.Rows = rs.Rows[:0]
		rs.Count = 0
		rs.LastInsertID = 0
		return rs
	}
	
	atomic.AddUint64(&amp.metrics.Misses, 1)
	// Create a new result set with configured initial capacities
	rs := &ResultSet{
		Columns: make([]string, 0, globalMemoryPool.config.ResultSetInitialCap),
		Rows:    make([][]interface{}, 0, globalMemoryPool.config.ResultSetInitialCap*2),
	}
	return rs
}

// ReleaseExecutorResultSet returns a ResultSet to the pool
func ReleaseExecutorResultSet(rs *ResultSet) {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	// Clear slices without reallocating
	for i := range rs.Rows {
		// Release individual row elements if they are poolable
		rs.Rows[i] = nil
	}
	rs.Rows = rs.Rows[:0]
	rs.Columns = rs.Columns[:0]
	
	amp := globalMemoryPool.executorResultSetPool
	atomic.AddUint64(&amp.metrics.Puts, 1)
	amp.pool.Put(rs)
	
	// Check if we need to resize the pool
	amp.checkAndResize()
}

// AcquireValueSlice gets a []*Value slice from the pool
func AcquireValueSlice(capacity int) []*Value {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	amp := globalMemoryPool.valueSlicePool
	atomic.AddUint64(&amp.metrics.Gets, 1)
	
	obj := amp.pool.Get()
	if obj != nil {
		atomic.AddUint64(&amp.metrics.Hits, 1)
		slicePtr := obj.(*[]*Value)
		slice := *slicePtr
		if cap(slice) < capacity {
			slice = make([]*Value, 0, capacity)
		} else {
			slice = slice[:0]
		}
		return slice
	}
	
	atomic.AddUint64(&amp.metrics.Misses, 1)
	// Create a new slice with the requested capacity
	slice := make([]*Value, 0, capacity)
	return slice
}

// ReleaseValueSlice returns a []*Value slice to the pool
func ReleaseValueSlice(slice []*Value) {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	// Clear elements without reallocating
	for i := range slice {
		slice[i] = nil
	}
	slice = slice[:0]
	
	amp := globalMemoryPool.valueSlicePool
	atomic.AddUint64(&amp.metrics.Puts, 1)
	amp.pool.Put(&slice)
	
	// Check if we need to resize the pool
	amp.checkAndResize()
}

// AcquireStringSlice gets a []string slice from the pool
func AcquireStringSlice(capacity int) []string {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	amp := globalMemoryPool.stringSlicePool
	atomic.AddUint64(&amp.metrics.Gets, 1)
	
	obj := amp.pool.Get()
	if obj != nil {
		atomic.AddUint64(&amp.metrics.Hits, 1)
		slicePtr := obj.(*[]string)
		slice := *slicePtr
		if cap(slice) < capacity {
			slice = make([]string, 0, capacity)
		} else {
			slice = slice[:0]
		}
		return slice
	}
	
	atomic.AddUint64(&amp.metrics.Misses, 1)
	// Create a new slice with the requested capacity
	slice := make([]string, 0, capacity)
	return slice
}

// ReleaseStringSlice returns a []string slice to the pool
func ReleaseStringSlice(slice []string) {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	// Clear elements without reallocating
	for i := range slice {
		slice[i] = ""
	}
	slice = slice[:0]
	
	amp := globalMemoryPool.stringSlicePool
	atomic.AddUint64(&amp.metrics.Puts, 1)
	amp.pool.Put(&slice)
	
	// Check if we need to resize the pool
	amp.checkAndResize()
}

// AcquireInterfaceSlice gets a []interface{} slice from the pool
func AcquireInterfaceSlice(capacity int) []interface{} {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	amp := globalMemoryPool.interfaceSlicePool
	atomic.AddUint64(&amp.metrics.Gets, 1)
	
	obj := amp.pool.Get()
	if obj != nil {
		atomic.AddUint64(&amp.metrics.Hits, 1)
		slicePtr := obj.(*[]interface{})
		slice := *slicePtr
		if cap(slice) < capacity {
			slice = make([]interface{}, 0, capacity)
		} else {
			slice = slice[:0]
		}
		return slice
	}
	
	atomic.AddUint64(&amp.metrics.Misses, 1)
	// Create a new slice with the requested capacity
	slice := make([]interface{}, 0, capacity)
	return slice
}

// ReleaseInterfaceSlice returns a []interface{} slice to the pool
func ReleaseInterfaceSlice(slice []interface{}) {
	globalMemoryPool.mu.RLock()
	defer globalMemoryPool.mu.RUnlock()
	
	// Clear elements without reallocating
	for i := range slice {
		slice[i] = nil
	}
	slice = slice[:0]
	
	amp := globalMemoryPool.interfaceSlicePool
	atomic.AddUint64(&amp.metrics.Puts, 1)
	amp.pool.Put(&slice)
	
	// Check if we need to resize the pool
	amp.checkAndResize()
}

// GetGlobalMemoryPoolMetrics returns metrics for the global memory pool
func GetGlobalMemoryPoolMetrics() map[string]PoolMetrics {
	return globalMemoryPool.GetMetrics()
}

// ReinitializeMemoryPool reinitializes the global memory pool with new configuration
// This should be used with caution as it will discard existing pooled objects
func ReinitializeMemoryPool(config MemoryPoolConfig) {
	globalMemoryPool.mu.Lock()
	defer globalMemoryPool.mu.Unlock()
	
	InitializeMemoryPool(config)
}