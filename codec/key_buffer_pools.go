package codec

import (
	"sync"
)

// KeyBufferPools manages specialized buffer pools for key encoding operations
type KeyBufferPools struct {
	// Pools for specific key encoding operations
	tableKeyBufferPool       sync.Pool // Pool for table key encoding buffers
	indexKeyBufferPool       sync.Pool // Pool for index key encoding buffers
	compositeIndexKeyBufferPool sync.Pool // Pool for composite index key encoding buffers
	pkKeyBufferPool          sync.Pool // Pool for primary key encoding buffers
	metaKeyBufferPool        sync.Pool // Pool for metadata key encoding buffers
	sequenceKeyBufferPool    sync.Pool // Pool for sequence key encoding buffers
	indexScanKeyBufferPool   sync.Pool // Pool for index scan key encoding buffers
	
	// Size-tiered general purpose pools for key buffers
	smallKeyBufferPool  sync.Pool // 32-byte key buffers
	mediumKeyBufferPool sync.Pool // 64-byte key buffers
	largeKeyBufferPool  sync.Pool // 128-byte key buffers
	hugeKeyBufferPool   sync.Pool // 256-byte key buffers
}

// NewKeyBufferPools creates new key buffer pools
func NewKeyBufferPools() *KeyBufferPools {
	return &KeyBufferPools{
		tableKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 32)
			},
		},
		indexKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64)
			},
		},
		compositeIndexKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128)
			},
		},
		pkKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64)
			},
		},
		metaKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64)
			},
		},
		sequenceKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 32)
			},
		},
		indexScanKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 33)
			},
		},
		
		// Size-tiered general purpose pools
		smallKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 32)
			},
		},
		mediumKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64)
			},
		},
		largeKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128)
			},
		},
		hugeKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256)
			},
		},
	}
}

// AcquireTableKeyBuffer gets a buffer for table key encoding
func (kbp *KeyBufferPools) AcquireTableKeyBuffer() []byte {
	buf := kbp.tableKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 32)
	}
	return buf.([]byte)[:0]
}

// ReleaseTableKeyBuffer returns a table key buffer to the pool
func (kbp *KeyBufferPools) ReleaseTableKeyBuffer(buf []byte) {
	if cap(buf) == 32 {
		kbp.tableKeyBufferPool.Put(buf[:0])
	} else {
		// Return to appropriate size-tiered pool
		kbp.releaseToSizeTieredPool(buf)
	}
}

// AcquireIndexKeyBuffer gets a buffer for index key encoding
func (kbp *KeyBufferPools) AcquireIndexKeyBuffer() []byte {
	buf := kbp.indexKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 64)
	}
	return buf.([]byte)[:0]
}

// ReleaseIndexKeyBuffer returns an index key buffer to the pool
func (kbp *KeyBufferPools) ReleaseIndexKeyBuffer(buf []byte) {
	if cap(buf) == 64 {
		kbp.indexKeyBufferPool.Put(buf[:0])
	} else {
		// Return to appropriate size-tiered pool
		kbp.releaseToSizeTieredPool(buf)
	}
}

// AcquireCompositeIndexKeyBuffer gets a buffer for composite index key encoding
func (kbp *KeyBufferPools) AcquireCompositeIndexKeyBuffer() []byte {
	buf := kbp.compositeIndexKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 128)
	}
	return buf.([]byte)[:0]
}

// ReleaseCompositeIndexKeyBuffer returns a composite index key buffer to the pool
func (kbp *KeyBufferPools) ReleaseCompositeIndexKeyBuffer(buf []byte) {
	if cap(buf) == 128 {
		kbp.compositeIndexKeyBufferPool.Put(buf[:0])
	} else {
		// Return to appropriate size-tiered pool
		kbp.releaseToSizeTieredPool(buf)
	}
}

// AcquirePKKeyBuffer gets a buffer for primary key encoding
func (kbp *KeyBufferPools) AcquirePKKeyBuffer() []byte {
	buf := kbp.pkKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 64)
	}
	return buf.([]byte)[:0]
}

// ReleasePKKeyBuffer returns a primary key buffer to the pool
func (kbp *KeyBufferPools) ReleasePKKeyBuffer(buf []byte) {
	if cap(buf) == 64 {
		kbp.pkKeyBufferPool.Put(buf[:0])
	} else {
		// Return to appropriate size-tiered pool
		kbp.releaseToSizeTieredPool(buf)
	}
}

// AcquireMetaKeyBuffer gets a buffer for metadata key encoding
func (kbp *KeyBufferPools) AcquireMetaKeyBuffer() []byte {
	buf := kbp.metaKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 64)
	}
	return buf.([]byte)[:0]
}

// ReleaseMetaKeyBuffer returns a metadata key buffer to the pool
func (kbp *KeyBufferPools) ReleaseMetaKeyBuffer(buf []byte) {
	if cap(buf) == 64 {
		kbp.metaKeyBufferPool.Put(buf[:0])
	} else {
		// Return to appropriate size-tiered pool
		kbp.releaseToSizeTieredPool(buf)
	}
}

// AcquireSequenceKeyBuffer gets a buffer for sequence key encoding
func (kbp *KeyBufferPools) AcquireSequenceKeyBuffer() []byte {
	buf := kbp.sequenceKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 32)
	}
	return buf.([]byte)[:0]
}

// ReleaseSequenceKeyBuffer returns a sequence key buffer to the pool
func (kbp *KeyBufferPools) ReleaseSequenceKeyBuffer(buf []byte) {
	if cap(buf) == 32 {
		kbp.sequenceKeyBufferPool.Put(buf[:0])
	} else {
		// Return to appropriate size-tiered pool
		kbp.releaseToSizeTieredPool(buf)
	}
}

// AcquireIndexScanKeyBuffer gets a buffer for index scan key encoding
func (kbp *KeyBufferPools) AcquireIndexScanKeyBuffer() []byte {
	buf := kbp.indexScanKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 33)
	}
	return buf.([]byte)[:0]
}

// ReleaseIndexScanKeyBuffer returns an index scan key buffer to the pool
func (kbp *KeyBufferPools) ReleaseIndexScanKeyBuffer(buf []byte) {
	if cap(buf) == 33 {
		kbp.indexScanKeyBufferPool.Put(buf[:0])
	} else {
		// Return to appropriate size-tiered pool
		kbp.releaseToSizeTieredPool(buf)
	}
}

// releaseToSizeTieredPool returns a buffer to the appropriate size-tiered pool
func (kbp *KeyBufferPools) releaseToSizeTieredPool(buf []byte) {
	buf = buf[:0]
	size := cap(buf)
	
	switch {
	case size <= 32:
		kbp.smallKeyBufferPool.Put(buf)
	case size <= 64:
		kbp.mediumKeyBufferPool.Put(buf)
	case size <= 128:
		kbp.largeKeyBufferPool.Put(buf)
	case size <= 256:
		kbp.hugeKeyBufferPool.Put(buf)
	}
}

// AcquireFromSizeTieredPool gets a buffer from the appropriate size-tiered pool
func (kbp *KeyBufferPools) AcquireFromSizeTieredPool(sizeHint int) []byte {
	switch {
	case sizeHint <= 32:
		buf := kbp.smallKeyBufferPool.Get()
		if buf == nil {
			return make([]byte, 0, 32)
		}
		return buf.([]byte)[:0]
	case sizeHint <= 64:
		buf := kbp.mediumKeyBufferPool.Get()
		if buf == nil {
			return make([]byte, 0, 64)
		}
		return buf.([]byte)[:0]
	case sizeHint <= 128:
		buf := kbp.largeKeyBufferPool.Get()
		if buf == nil {
			return make([]byte, 0, 128)
		}
		return buf.([]byte)[:0]
	case sizeHint <= 256:
		buf := kbp.hugeKeyBufferPool.Get()
		if buf == nil {
			return make([]byte, 0, 256)
		}
		return buf.([]byte)[:0]
	default:
		// For larger buffers, allocate directly
		return make([]byte, 0, sizeHint)
	}
}

// ReleaseToSizeTieredPool returns a buffer to the appropriate size-tiered pool
func (kbp *KeyBufferPools) ReleaseToSizeTieredPool(buf []byte) {
	kbp.releaseToSizeTieredPool(buf)
}