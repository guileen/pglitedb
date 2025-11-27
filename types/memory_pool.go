// Enhanced memory pooling for various database objects
package types

import (
	"sync"
)

// ResultSet is the internal result set type used by the executor
type ResultSet struct {
	Columns      []string
	Rows         [][]interface{}
	Count        int
	LastInsertID int64
}

// MemoryPool manages pools of various database objects
type MemoryPool struct {
	recordPool        sync.Pool
	resultSetPool     sync.Pool
	executorResultSetPool sync.Pool
	valueSlicePool    sync.Pool
	stringSlicePool   sync.Pool
	interfaceSlicePool sync.Pool
}

// Global memory pool instance
var globalMemoryPool = &MemoryPool{
	recordPool: sync.Pool{
		New: func() interface{} {
			return &Record{
				Data: make(map[string]*Value, 16),
			}
		},
	},
	resultSetPool: sync.Pool{
		New: func() interface{} {
			return &QueryResult{
				Columns: make([]ColumnInfo, 0, 8),
				Rows:    make([][]interface{}, 0, 16),
			}
		},
	},
	executorResultSetPool: sync.Pool{
		New: func() interface{} {
			return &ResultSet{
				Columns: make([]string, 0, 8),
				Rows:    make([][]interface{}, 0, 16),
			}
		},
	},
	valueSlicePool: sync.Pool{
		New: func() interface{} {
			slice := make([]*Value, 0, 16)
			return &slice
		},
	},
	stringSlicePool: sync.Pool{
		New: func() interface{} {
			slice := make([]string, 0, 8)
			return &slice
		},
	},
	interfaceSlicePool: sync.Pool{
		New: func() interface{} {
			slice := make([]interface{}, 0, 16)
			return &slice
		},
	},
}

// AcquireRecord gets a Record from the pool
func AcquireRecord() *Record {
	r := globalMemoryPool.recordPool.Get().(*Record)
	r.ID = ""
	r.Table = ""
	return r
}

// ReleaseRecord returns a Record to the pool
func ReleaseRecord(r *Record) {
	// Clear the map without reallocating
	for k := range r.Data {
		delete(r.Data, k)
	}
	globalMemoryPool.recordPool.Put(r)
}

// AcquireResultSet gets a QueryResult from the pool
func AcquireResultSet() *QueryResult {
	rs := globalMemoryPool.resultSetPool.Get().(*QueryResult)
	// Reset fields but keep underlying slices
	rs.Columns = rs.Columns[:0]
	rs.Rows = rs.Rows[:0]
	rs.Count = 0
	rs.LastInsertID = 0
	return rs
}

// ReleaseResultSet returns a QueryResult to the pool
func ReleaseResultSet(rs *QueryResult) {
	// Clear slices without reallocating
	for i := range rs.Rows {
		// Release individual row elements if they are poolable
		rs.Rows[i] = nil
	}
	rs.Rows = rs.Rows[:0]
	rs.Columns = rs.Columns[:0]
	globalMemoryPool.resultSetPool.Put(rs)
}

// AcquireExecutorResultSet gets a ResultSet from the pool for use by the executor
func AcquireExecutorResultSet() *ResultSet {
	rs := globalMemoryPool.executorResultSetPool.Get().(*ResultSet)
	// Reset fields but keep underlying slices
	rs.Columns = rs.Columns[:0]
	rs.Rows = rs.Rows[:0]
	rs.Count = 0
	rs.LastInsertID = 0
	return rs
}

// ReleaseExecutorResultSet returns a ResultSet to the pool
func ReleaseExecutorResultSet(rs *ResultSet) {
	// Clear slices without reallocating
	for i := range rs.Rows {
		// Release individual row elements if they are poolable
		rs.Rows[i] = nil
	}
	rs.Rows = rs.Rows[:0]
	rs.Columns = rs.Columns[:0]
	globalMemoryPool.executorResultSetPool.Put(rs)
}

// AcquireValueSlice gets a []*Value slice from the pool
func AcquireValueSlice(capacity int) []*Value {
	slicePtr := globalMemoryPool.valueSlicePool.Get().(*[]*Value)
	slice := *slicePtr
	if cap(slice) < capacity {
		slice = make([]*Value, 0, capacity)
	} else {
		slice = slice[:0]
	}
	return slice
}

// ReleaseValueSlice returns a []*Value slice to the pool
func ReleaseValueSlice(slice []*Value) {
	// Clear elements without reallocating
	for i := range slice {
		slice[i] = nil
	}
	slice = slice[:0]
	globalMemoryPool.valueSlicePool.Put(&slice)
}

// AcquireStringSlice gets a []string slice from the pool
func AcquireStringSlice(capacity int) []string {
	slicePtr := globalMemoryPool.stringSlicePool.Get().(*[]string)
	slice := *slicePtr
	if cap(slice) < capacity {
		slice = make([]string, 0, capacity)
	} else {
		slice = slice[:0]
	}
	return slice
}

// ReleaseStringSlice returns a []string slice to the pool
func ReleaseStringSlice(slice []string) {
	// Clear elements without reallocating
	for i := range slice {
		slice[i] = ""
	}
	slice = slice[:0]
	globalMemoryPool.stringSlicePool.Put(&slice)
}

// AcquireInterfaceSlice gets a []interface{} slice from the pool
func AcquireInterfaceSlice(capacity int) []interface{} {
	slicePtr := globalMemoryPool.interfaceSlicePool.Get().(*[]interface{})
	slice := *slicePtr
	if cap(slice) < capacity {
		slice = make([]interface{}, 0, capacity)
	} else {
		slice = slice[:0]
	}
	return slice
}

// ReleaseInterfaceSlice returns a []interface{} slice to the pool
func ReleaseInterfaceSlice(slice []interface{}) {
	// Clear elements without reallocating
	for i := range slice {
		slice[i] = nil
	}
	slice = slice[:0]
	globalMemoryPool.interfaceSlicePool.Put(&slice)
}