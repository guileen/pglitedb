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

// ScanResultMapPool manages maps of scan results
type ScanResultMapPool struct {
	BasePool
}

// NewScanResultMapPool creates a new scan result map pool
func NewScanResultMapPool() *ScanResultMapPool {
	return &ScanResultMapPool{
		BasePool: *NewBasePool("scanResultMap", func() interface{} {
			return make(map[int64]*types.Record) // Map for batch operations
		}),
	}
}

// AcquireScanResultMap gets a scan result map from the pool
func (srmp *ScanResultMapPool) AcquireScanResultMap() map[int64]*types.Record {
	result := srmp.BasePool.pool.Get()
	fromPool := result != nil

	if !fromPool {
		return make(map[int64]*types.Record)
	}

	return result.(map[int64]*types.Record)
}

// ReleaseScanResultMap returns a scan result map to the pool
func (srmp *ScanResultMapPool) ReleaseScanResultMap(result map[int64]*types.Record) {
	// Clear the map without reallocating
	for k := range result {
		delete(result, k)
	}

	srmp.BasePool.Put(result)
}