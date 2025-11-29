package pools

import (
	"github.com/guileen/pglitedb/types"
)

// ScanResultPool manages scan result resources
type ScanResultPool struct {
	BasePool
}

// NewScanResultPool creates a new scan result pool
func NewScanResultPool() *ScanResultPool {
	return &ScanResultPool{
		BasePool: *NewBasePool("scanResult", func() interface{} {
			return make([]*types.Record, 0, 10) // Pre-allocate slice with capacity
		}),
	}
}

// Acquire gets a scan result slice from the pool
func (srp *ScanResultPool) Acquire() []*types.Record {
	result := srp.BasePool.pool.Get()
	fromPool := result != nil

	if !fromPool {
		result = make([]*types.Record, 0, 10)
		return result.([]*types.Record)
	}

	return result.([]*types.Record)
}

// Release returns a scan result slice to the pool
func (srp *ScanResultPool) Release(result []*types.Record) {
	// Clear the slice without reallocating
	for i := range result {
		result[i] = nil
	}
	result = result[:0]

	srp.BasePool.Put(result)
}