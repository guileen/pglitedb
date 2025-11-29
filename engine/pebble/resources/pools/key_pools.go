package pools

import (
	"sync"
)

// KeyPools manages various key buffer resources
type KeyPools struct {
	indexKeyPool     BasePool // Pool for index keys
	tableKeyPool     BasePool // Pool for table keys
	metaKeyPool      BasePool // Pool for metadata keys
	compositeKeyPool BasePool // Pool for composite keys
	
	// Size-tiered pools for better memory utilization
	smallKeyPool  sync.Pool // 32-byte keys
	mediumKeyPool sync.Pool // 64-byte keys
	largeKeyPool  sync.Pool // 128-byte keys
	hugeKeyPool   sync.Pool // 256-byte keys
}

// NewKeyPools creates new key pools
func NewKeyPools() *KeyPools {
	return &KeyPools{
		indexKeyPool:     *NewBasePool("indexKey", func() interface{} { return make([]byte, 0, 64) }),
		tableKeyPool:     *NewBasePool("tableKey", func() interface{} { return make([]byte, 0, 32) }),
		metaKeyPool:      *NewBasePool("metaKey", func() interface{} { return make([]byte, 0, 128) }),
		compositeKeyPool: *NewBasePool("compositeKey", func() interface{} { return make([]byte, 0, 128) }),
		
		// Initialize size-tiered pools
		smallKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 32)
			},
		},
		mediumKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64)
			},
		},
		largeKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128)
			},
		},
		hugeKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256)
			},
		},
	}
}

// AcquireIndexKeyBuffer gets a buffer optimized for index keys
func (kp *KeyPools) AcquireIndexKeyBuffer(size int) []byte {
	// Use size-tiered pools for better memory utilization
	if size <= 32 {
		buf := kp.smallKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 32)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 64 {
		buf := kp.mediumKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 64)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 128 {
		buf := kp.largeKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 128)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 256 {
		buf := kp.hugeKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 256)
		}
		b := buf.([]byte)
		return b[:size]
	}
	
	// For larger sizes, fall back to the original pool
	buf := kp.indexKeyPool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		buf = make([]byte, 0, size)
		return buf.([]byte)[:size]
	}

	b := buf.([]byte)
	if cap(b) >= size {
		return b[:size]
	}

	// Buffer too small, create a new one
	return make([]byte, size)
}

// ReleaseIndexKeyBuffer returns an index key buffer to the pool
func (kp *KeyPools) ReleaseIndexKeyBuffer(buf []byte) {
	buf = buf[:0]
	size := cap(buf)
	
	// Return to appropriate size-tiered pool
	if size <= 32 {
		kp.smallKeyPool.Put(buf)
	} else if size <= 64 {
		kp.mediumKeyPool.Put(buf)
	} else if size <= 128 {
		kp.largeKeyPool.Put(buf)
	} else if size <= 256 {
		kp.hugeKeyPool.Put(buf)
	} else {
		// For larger buffers, return to original pool
		kp.indexKeyPool.Put(buf)
	}
}

// AcquireTableKeyBuffer gets a buffer optimized for table keys
func (kp *KeyPools) AcquireTableKeyBuffer(size int) []byte {
	// Table keys are typically small (32 bytes), use size-tiered pools
	if size <= 32 {
		buf := kp.smallKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 32)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 64 {
		buf := kp.mediumKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 64)
		}
		b := buf.([]byte)
		return b[:size]
	}
	
	// For larger sizes, fall back to the original pool
	buf := kp.tableKeyPool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		buf = make([]byte, 0, size)
		return buf.([]byte)[:size]
	}

	b := buf.([]byte)
	if cap(b) >= size {
		return b[:size]
	}

	return make([]byte, size)
}

// ReleaseTableKeyBuffer returns a table key buffer to the pool
func (kp *KeyPools) ReleaseTableKeyBuffer(buf []byte) {
	buf = buf[:0]
	size := cap(buf)
	
	// Return to appropriate size-tiered pool
	if size <= 32 {
		kp.smallKeyPool.Put(buf)
	} else if size <= 64 {
		kp.mediumKeyPool.Put(buf)
	} else {
		// For larger buffers, return to original pool
		kp.tableKeyPool.Put(buf)
	}
}

// AcquireMetaKeyBuffer gets a buffer optimized for metadata keys
func (kp *KeyPools) AcquireMetaKeyBuffer(size int) []byte {
	// Meta keys can vary in size, use size-tiered pools when possible
	if size <= 32 {
		buf := kp.smallKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 32)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 64 {
		buf := kp.mediumKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 64)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 128 {
		buf := kp.largeKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 128)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 256 {
		buf := kp.hugeKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 256)
		}
		b := buf.([]byte)
		return b[:size]
	}
	
	// For larger sizes, fall back to the original pool
	buf := kp.metaKeyPool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		buf = make([]byte, 0, size)
		return buf.([]byte)[:size]
	}

	b := buf.([]byte)
	if cap(b) >= size {
		return b[:size]
	}

	return make([]byte, size)
}

// ReleaseMetaKeyBuffer returns a metadata key buffer to the pool
func (kp *KeyPools) ReleaseMetaKeyBuffer(buf []byte) {
	buf = buf[:0]
	size := cap(buf)
	
	// Return to appropriate size-tiered pool
	if size <= 32 {
		kp.smallKeyPool.Put(buf)
	} else if size <= 64 {
		kp.mediumKeyPool.Put(buf)
	} else if size <= 128 {
		kp.largeKeyPool.Put(buf)
	} else if size <= 256 {
		kp.hugeKeyPool.Put(buf)
	} else {
		// For larger buffers, return to original pool
		kp.metaKeyPool.Put(buf)
	}
}

// AcquireCompositeKeyBuffer gets a buffer optimized for composite keys
func (kp *KeyPools) AcquireCompositeKeyBuffer(size int) []byte {
	// Composite keys can be larger, use size-tiered pools when possible
	if size <= 32 {
		buf := kp.smallKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 32)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 64 {
		buf := kp.mediumKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 64)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 128 {
		buf := kp.largeKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 128)
		}
		b := buf.([]byte)
		return b[:size]
	} else if size <= 256 {
		buf := kp.hugeKeyPool.Get()
		if buf == nil {
			buf = make([]byte, 0, 256)
		}
		b := buf.([]byte)
		return b[:size]
	}
	
	// For larger sizes, fall back to the original pool
	buf := kp.compositeKeyPool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		buf = make([]byte, 0, size)
		return buf.([]byte)[:size]
	}

	b := buf.([]byte)
	if cap(b) >= size {
		return b[:size]
	}

	return make([]byte, size)
}

// ReleaseCompositeKeyBuffer returns a composite key buffer to the pool
func (kp *KeyPools) ReleaseCompositeKeyBuffer(buf []byte) {
	buf = buf[:0]
	size := cap(buf)
	
	// Return to appropriate size-tiered pool
	if size <= 32 {
		kp.smallKeyPool.Put(buf)
	} else if size <= 64 {
		kp.mediumKeyPool.Put(buf)
	} else if size <= 128 {
		kp.largeKeyPool.Put(buf)
	} else if size <= 256 {
		kp.hugeKeyPool.Put(buf)
	} else {
		// For larger buffers, return to original pool
		kp.compositeKeyPool.Put(buf)
	}
}