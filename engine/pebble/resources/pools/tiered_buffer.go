package pools

import "sync"

// TieredBufferPool manages size-tiered buffer resources
type TieredBufferPool struct {
	smallBufferPool  sync.Pool // 64-byte buffers
	mediumBufferPool sync.Pool // 256-byte buffers
	largeBufferPool  sync.Pool // 1024-byte buffers
	hugeBufferPool   sync.Pool // 4096-byte buffers
	bufferPool       sync.Pool // General buffer pool
}

// NewTieredBufferPool creates a new tiered buffer pool
func NewTieredBufferPool() *TieredBufferPool {
	return &TieredBufferPool{
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
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256) // Start with reasonable buffer size
			},
		},
	}
}

// Acquire gets a buffer from the appropriate size-tiered pool
func (tbp *TieredBufferPool) Acquire(size int) []byte {
	// Select the appropriate pool based on size
	var pool *sync.Pool
	var buf interface{}

	switch {
	case size <= 64:
		pool = &tbp.smallBufferPool
		buf = pool.Get()
		if buf == nil {
			buf = make([]byte, 0, 64)
		}
	case size <= 256:
		pool = &tbp.mediumBufferPool
		buf = pool.Get()
		if buf == nil {
			buf = make([]byte, 0, 256)
		}
	case size <= 1024:
		pool = &tbp.largeBufferPool
		buf = pool.Get()
		if buf == nil {
			buf = make([]byte, 0, 1024)
		}
	case size <= 4096:
		pool = &tbp.hugeBufferPool
		buf = pool.Get()
		if buf == nil {
			buf = make([]byte, 0, 4096)
		}
	default:
		// For very large buffers, use the general buffer pool
		pool = &tbp.bufferPool
		buf = pool.Get()
		if buf == nil {
			buf = make([]byte, 0, size)
		}
	}

	b := buf.([]byte)
	if cap(b) >= size {
		return b[:size]
	}

	return make([]byte, size)
}

// Release returns a buffer back to its appropriate size-tiered pool
func (tbp *TieredBufferPool) Release(buf []byte) {
	buf = buf[:0]
	size := cap(buf)

	// Return to the appropriate pool based on size
	switch {
	case size <= 64:
		tbp.smallBufferPool.Put(buf)
	case size <= 256:
		tbp.mediumBufferPool.Put(buf)
	case size <= 1024:
		tbp.largeBufferPool.Put(buf)
	case size <= 4096:
		tbp.hugeBufferPool.Put(buf)
	default:
		tbp.bufferPool.Put(buf)
	}
}