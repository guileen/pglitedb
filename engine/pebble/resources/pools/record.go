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