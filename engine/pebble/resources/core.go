package resources

import (
	"context"
	"sync"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/leak_detection"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

// ResourceManager manages resource pooling for the pebble engine
type ResourceManager struct {
	iteratorPool     sync.Pool
	batchPool        sync.Pool
	txnPool          sync.Pool
	recordPool       sync.Pool  // Pool for Record objects
	valuePool        sync.Pool  // Pool for Value objects
	bufferPool       sync.Pool  // Pool for byte buffers
	keyEncoderPool   sync.Pool  // Pool for key encoders
	filterExprPool   sync.Pool  // Pool for filter expressions
	
	// Enhanced pools for specific use cases
	scanResultPool   sync.Pool  // Pool for scan results
	indexKeyPool     sync.Pool  // Pool for index keys
	tableKeyPool     sync.Pool  // Pool for table keys
	metaKeyPool      sync.Pool  // Pool for metadata keys
	compositeKeyPool sync.Pool  // Pool for composite keys
	
	// Size-tiered buffer pools for better memory utilization
	smallBufferPool  sync.Pool  // 64-byte buffers
	mediumBufferPool sync.Pool  // 256-byte buffers
	largeBufferPool  sync.Pool  // 1024-byte buffers
	hugeBufferPool   sync.Pool  // 4096-byte buffers
	
	// Adaptive pool sizing fields
	poolSizes        map[string]int // Track current pool sizes
	poolHitRates     map[string]float64 // Track pool hit rates
	poolAdjustmentMu sync.RWMutex   // Mutex for pool adjustment operations
	
	metrics          *ResourceMetricsCollector
	
	// Leak detection
	leakDetector     engineTypes.LeakDetector
}

// NewResourceManager creates a new resource manager
func NewResourceManager() *ResourceManager {
	rm := &ResourceManager{
		iteratorPool: sync.Pool{
			New: func() interface{} {
				return &scan.RowIterator{}
			},
		},
		batchPool: sync.Pool{
			New: func() interface{} {
				return &batchWrapper{}
			},
		},
		txnPool: sync.Pool{
			New: func() interface{} {
				return &transactionWrapper{}
			},
		},
		recordPool: sync.Pool{
			New: func() interface{} {
				return &types.Record{
					Data: make(map[string]*types.Value),
				}
			},
		},
		valuePool: sync.Pool{
			New: func() interface{} {
				return &types.Value{}
			},
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256) // Start with reasonable buffer size
			},
		},
		keyEncoderPool: sync.Pool{
			New: func() interface{} {
				return codec.NewMemComparableCodec()
			},
		},
		filterExprPool: sync.Pool{
			New: func() interface{} {
				return &engineTypes.FilterExpression{}
			},
		},
		// Enhanced pools
		scanResultPool: sync.Pool{
			New: func() interface{} {
				return make([]*types.Record, 0, 10) // Pre-allocate slice with capacity
			},
		},
		indexKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64) // Index keys are typically smaller
			},
		},
		tableKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 32) // Table keys are typically small
			},
		},
		metaKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128) // Meta keys can be larger
			},
		},
		compositeKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128) // Composite keys vary in size
			},
		},
		// Size-tiered buffer pools
		smallBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64)
			},
		},
		mediumBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256)
			},
		},
		largeBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 1024)
			},
		},
		hugeBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 4096)
			},
		},
		poolSizes: make(map[string]int),
		poolHitRates: make(map[string]float64),
		metrics: NewResourceMetricsCollector(),
		leakDetector: leak_detection.NewLeakDetector(),
	}
	
	// Initialize default pool sizes
	rm.poolSizes["iterator"] = 100
	rm.poolSizes["batch"] = 100
	rm.poolSizes["txn"] = 100
	rm.poolSizes["record"] = 1000
	rm.poolSizes["value"] = 1000
	rm.poolSizes["buffer"] = 1000
	rm.poolSizes["keyEncoder"] = 100
	rm.poolSizes["filterExpr"] = 100
	rm.poolSizes["scanResult"] = 100
	rm.poolSizes["indexKey"] = 100
	rm.poolSizes["tableKey"] = 100
	rm.poolSizes["metaKey"] = 100
	rm.poolSizes["compositeKey"] = 100
	
	// Start leak detection monitoring
	rm.leakDetector.StartMonitoring(context.Background())
	
	return rm
}

// batchWrapper wraps a storage batch for pooling
type batchWrapper struct {
	batch storage.Batch
}

// transactionWrapper wraps a transaction for pooling
type transactionWrapper struct {
	txn engineTypes.Transaction
}

// GetResourceManager returns the default resource manager
func GetResourceManager() *ResourceManager {
	return defaultResourceManager
}

// Initialize resource manager instance
var defaultResourceManager = NewResourceManager()