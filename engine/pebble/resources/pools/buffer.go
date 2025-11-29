package pools

// BufferPool manages byte buffer resources
type BufferPool struct {
	BasePool
}

// NewBufferPool creates a new buffer pool
func NewBufferPool() *BufferPool {
	return &BufferPool{
		BasePool: *NewBasePool("buffer", func() interface{} {
			return make([]byte, 0, 256) // Start with reasonable buffer size
		}),
	}
}

// Acquire gets a byte buffer from the pool with the specified size
func (bp *BufferPool) Acquire(size int) []byte {
	buf := bp.BasePool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		// Create a new buffer with the requested size
		buf = make([]byte, 0, size)
		return buf.([]byte)[:size]
	}

	b := buf.([]byte)
	if cap(b) >= size {
		// Reuse existing buffer
		return b[:size]
	}

	// Buffer too small, create a new one
	return make([]byte, size)
}

// Release returns a byte buffer to the pool
func (bp *BufferPool) Release(buf []byte) {
	// Reset buffer without reallocating
	buf = buf[:0]
	bp.BasePool.Put(buf)
}

// AcquireWithMinSize gets a byte buffer with a minimum capacity
func (bp *BufferPool) AcquireWithMinSize(minSize int) []byte {
	buf := bp.BasePool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		// Create a new buffer with the requested size
		buf = make([]byte, 0, minSize)
		return buf.([]byte)
	}

	b := buf.([]byte)
	if cap(b) >= minSize {
		// Reuse existing buffer
		return b
	}

	// Buffer too small, create a new one
	return make([]byte, 0, minSize)
}