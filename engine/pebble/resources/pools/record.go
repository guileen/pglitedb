package pools

import (
	"time"

	"github.com/guileen/pglitedb/types"
)

// RecordPool manages record resources
type RecordPool struct {
	BasePool
}

// NewRecordPool creates a new record pool
func NewRecordPool() *RecordPool {
	return &RecordPool{
		BasePool: *NewBasePool("record", func() interface{} {
			return &types.Record{
				Data: make(map[string]*types.Value),
			}
		}),
	}
}

// Acquire gets a record from the pool
func (rp *RecordPool) Acquire() *types.Record {
	record := rp.BasePool.pool.Get()
	fromPool := record != nil

	if !fromPool {
		record = &types.Record{
			Data: make(map[string]*types.Value),
		}
		return record.(*types.Record)
	}

	return record.(*types.Record)
}

// Release returns a record to the pool
func (rp *RecordPool) Release(record *types.Record) {
	// Clear the map without reallocating
	for k := range record.Data {
		delete(record.Data, k)
	}
	record.ID = ""
	record.Table = ""
	record.Metadata = nil
	record.CreatedAt = time.Time{}
	record.UpdatedAt = time.Time{}
	record.Version = 0

	rp.BasePool.Put(record)
}

// RecordSlicePool manages slices of records
type RecordSlicePool struct {
	BasePool
}

// NewRecordSlicePool creates a new record slice pool
func NewRecordSlicePool() *RecordSlicePool {
	return &RecordSlicePool{
		BasePool: *NewBasePool("recordSlice", func() interface{} {
			return make([]*types.Record, 0, 32) // Start with reasonable capacity
		}),
	}
}

// AcquireRecordSlice gets a record slice from the pool
func (rsp *RecordSlicePool) AcquireRecordSlice() []*types.Record {
	slice := rsp.BasePool.pool.Get()
	fromPool := slice != nil

	if !fromPool {
		return make([]*types.Record, 0, 32)
	}

	return slice.([]*types.Record)
}

// ReleaseRecordSlice returns a record slice to the pool
func (rsp *RecordSlicePool) ReleaseRecordSlice(slice []*types.Record) {
	// Clear the slice without reallocating
	for i := range slice {
		slice[i] = nil
	}
	slice = slice[:0]
	rsp.BasePool.Put(slice)
}