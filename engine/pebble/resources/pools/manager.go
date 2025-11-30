package pools

import (
	"github.com/guileen/pglitedb/engine/pebble/resources/leak"
	"github.com/guileen/pglitedb/engine/pebble/resources/metrics"
	"github.com/guileen/pglitedb/types"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	scan "github.com/guileen/pglitedb/engine/pebble/operations/scan"
)

// Manager coordinates all resource pools
type Manager struct {
	batchPool           *BatchPool
	batchOperationPool  *BatchOperationPool
	txnPool             *TxnPool
	recordPool          *RecordPool
	bufferPool          *BufferPool
	keyEncoderPool      *KeyEncoderPool
	filterExprPool      *FilterExprPool
	scanResultPool      *ScanResultPool
	keyPools            *KeyPools
	tieredBufferPool    *TieredBufferPool
	
	// Iterator pools
	indexIteratorPool   *IndexIteratorPool
	rowIteratorPool     *RowIteratorPool
	indexOnlyIteratorPool *IndexOnlyIteratorPool
	
	// Leak detector
	leakDetector *leak.Detector
	
	// Metrics recorder
	metrics *metrics.Collector
}

// NewManager creates a new pool manager
func NewManager(
	leakDetector *leak.Detector,
	metrics *metrics.Collector,
) *Manager {
	return &Manager{
		batchPool:           NewBatchPool(),
		batchOperationPool:  NewBatchOperationPool(),
		txnPool:             NewTxnPool(),
		recordPool:          NewRecordPool(),
		bufferPool:          NewBufferPool(),
		keyEncoderPool:      NewKeyEncoderPool(),
		filterExprPool:      NewFilterExprPool(),
		scanResultPool:      NewScanResultPool(),
		keyPools:            NewKeyPools(),
		tieredBufferPool:    NewTieredBufferPool(),
		indexIteratorPool:   NewIndexIteratorPool(),
		rowIteratorPool:     NewRowIteratorPool(),
		indexOnlyIteratorPool: NewIndexOnlyIteratorPool(),
		leakDetector:        leakDetector,
		metrics:             metrics,
	}
}

// AcquireBatch gets a batch from the pool
func (pm *Manager) AcquireBatch() *BatchWrapper {
	return pm.batchPool.Acquire()
}

// ReleaseBatch returns a batch to the pool
func (pm *Manager) ReleaseBatch(batch *BatchWrapper) {
	pm.batchPool.Release(batch)
}

// AcquireTransaction gets a transaction from the pool
func (pm *Manager) AcquireTransaction() *TransactionWrapper {
	return pm.txnPool.Acquire()
}

// ReleaseTransaction returns a transaction to the pool
func (pm *Manager) ReleaseTransaction(txn *TransactionWrapper) {
	pm.txnPool.Release(txn)
}

// AcquireRecord gets a record from the pool
func (pm *Manager) AcquireRecord() *types.Record {
	return pm.recordPool.Acquire()
}

// ReleaseRecord returns a record to the pool
func (pm *Manager) ReleaseRecord(record *types.Record) {
	pm.recordPool.Release(record)
}

// AcquireBuffer gets a byte buffer from the pool with the specified size
func (pm *Manager) AcquireBuffer(size int) []byte {
	return pm.bufferPool.Acquire(size)
}

// ReleaseBuffer returns a byte buffer to the pool
func (pm *Manager) ReleaseBuffer(buf []byte) {
	pm.bufferPool.Release(buf)
}

// AcquireScanResult gets a scan result slice from the pool
func (pm *Manager) AcquireScanResult() []*types.Record {
	return pm.scanResultPool.Acquire()
}

// ReleaseScanResult returns a scan result slice to the pool
func (pm *Manager) ReleaseScanResult(result []*types.Record) {
	pm.scanResultPool.Release(result)
}

// AcquireIndexKeyBuffer gets a buffer optimized for index keys
func (pm *Manager) AcquireIndexKeyBuffer(size int) []byte {
	return pm.keyPools.AcquireIndexKeyBuffer(size)
}

// ReleaseIndexKeyBuffer returns an index key buffer to the pool
func (pm *Manager) ReleaseIndexKeyBuffer(buf []byte) {
	pm.keyPools.ReleaseIndexKeyBuffer(buf)
}

// AcquireTableKeyBuffer gets a buffer optimized for table keys
func (pm *Manager) AcquireTableKeyBuffer(size int) []byte {
	return pm.keyPools.AcquireTableKeyBuffer(size)
}

// ReleaseTableKeyBuffer returns a table key buffer to the pool
func (pm *Manager) ReleaseTableKeyBuffer(buf []byte) {
	pm.keyPools.ReleaseTableKeyBuffer(buf)
}

// AcquireMetaKeyBuffer gets a buffer optimized for metadata keys
func (pm *Manager) AcquireMetaKeyBuffer(size int) []byte {
	return pm.keyPools.AcquireMetaKeyBuffer(size)
}

// ReleaseMetaKeyBuffer returns a metadata key buffer to the pool
func (pm *Manager) ReleaseMetaKeyBuffer(buf []byte) {
	pm.keyPools.ReleaseMetaKeyBuffer(buf)
}

// AcquireCompositeKeyBuffer gets a buffer optimized for composite keys
func (pm *Manager) AcquireCompositeKeyBuffer(size int) []byte {
	return pm.keyPools.AcquireCompositeKeyBuffer(size)
}

// ReleaseCompositeKeyBuffer returns a composite key buffer to the pool
func (pm *Manager) ReleaseCompositeKeyBuffer(buf []byte) {
	pm.keyPools.ReleaseCompositeKeyBuffer(buf)
}

// AcquireTieredBuffer gets a buffer from the appropriate size-tiered pool
func (pm *Manager) AcquireTieredBuffer(size int) []byte {
	return pm.tieredBufferPool.Acquire(size)
}

// ReleaseTieredBuffer returns a buffer back to its appropriate size-tiered pool
func (pm *Manager) ReleaseTieredBuffer(buf []byte) {
	pm.tieredBufferPool.Release(buf)
}

// AcquireBatchOperation gets a batch operation from the pool
func (pm *Manager) AcquireBatchOperation() *BatchOperationWrapper {
	return pm.batchOperationPool.Acquire()
}

// ReleaseBatchOperation returns a batch operation to the pool
func (pm *Manager) ReleaseBatchOperation(op *BatchOperationWrapper) {
	pm.batchOperationPool.Release(op)
}

// AcquireKeyEncoder gets a key encoder from the pool
func (pm *Manager) AcquireKeyEncoder() interface{} {
	encoder := pm.keyEncoderPool.Acquire()
	return encoder
}

// AcquireIndexIterator gets an index iterator from the pool
func (pm *Manager) AcquireIndexIterator() *scan.IndexIterator {
	return pm.indexIteratorPool.Acquire()
}

// ReleaseIndexIterator returns an index iterator to the pool
func (pm *Manager) ReleaseIndexIterator(iter *scan.IndexIterator) {
	pm.indexIteratorPool.Release(iter)
}

// AcquireRowIterator gets a row iterator from the pool
func (pm *Manager) AcquireRowIterator() *scan.RowIterator {
	return pm.rowIteratorPool.Acquire()
}

// ReleaseRowIterator returns a row iterator to the pool
func (pm *Manager) ReleaseRowIterator(iter *scan.RowIterator) {
	pm.rowIteratorPool.Release(iter)
}

// AcquireIndexOnlyIterator gets an index-only iterator from the pool
func (pm *Manager) AcquireIndexOnlyIterator() *scan.IndexOnlyIterator {
	return pm.indexOnlyIteratorPool.Acquire()
}

// ReleaseIndexOnlyIterator returns an index-only iterator to the pool
func (pm *Manager) ReleaseIndexOnlyIterator(iter *scan.IndexOnlyIterator) {
	pm.indexOnlyIteratorPool.Release(iter)
}

// ReleaseKeyEncoder returns a key encoder to the pool
func (pm *Manager) ReleaseKeyEncoder(encoder interface{}) {
	pm.keyEncoderPool.Release(encoder)
}

// AcquireFilterExpr gets a filter expression from the pool
func (pm *Manager) AcquireFilterExpr() *engineTypes.FilterExpression {
	expr := pm.filterExprPool.Acquire()
	return expr
}

// ReleaseFilterExpr returns a filter expression to the pool
func (pm *Manager) ReleaseFilterExpr(expr *engineTypes.FilterExpression) {
	pm.filterExprPool.Release(expr)
}