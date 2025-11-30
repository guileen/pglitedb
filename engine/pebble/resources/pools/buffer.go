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

// KeyBufferPool manages key buffer resources
type KeyBufferPool struct {
	BasePool
}

// NewKeyBufferPool creates a new key buffer pool
func NewKeyBufferPool() *KeyBufferPool {
	return &KeyBufferPool{
		BasePool: *NewBasePool("keyBuffer", func() interface{} {
			return make([]byte, 0, 128) // Keys are typically smaller
		}),
	}
}

// AcquireKeyBuffer gets a key buffer from the pool
func (kbp *KeyBufferPool) AcquireKeyBuffer() []byte {
	buf := kbp.BasePool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		return make([]byte, 0, 128)
	}

	b := buf.([]byte)
	return b[:cap(b)]
}

// ReleaseKeyBuffer returns a key buffer to the pool
func (kbp *KeyBufferPool) ReleaseKeyBuffer(buf []byte) {
	buf = buf[:0]
	kbp.BasePool.Put(buf)
}

// ValueBufferPool manages value buffer resources
type ValueBufferPool struct {
	BasePool
}

// NewValueBufferPool creates a new value buffer pool
func NewValueBufferPool() *ValueBufferPool {
	return &ValueBufferPool{
		BasePool: *NewBasePool("valueBuffer", func() interface{} {
			return make([]byte, 0, 512) // Values are typically larger
		}),
	}
}

// AcquireValueBuffer gets a value buffer from the pool
func (vbp *ValueBufferPool) AcquireValueBuffer() []byte {
	buf := vbp.BasePool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		return make([]byte, 0, 512)
	}

	b := buf.([]byte)
	return b[:cap(b)]
}

// ReleaseValueBuffer returns a value buffer to the pool
func (vbp *ValueBufferPool) ReleaseValueBuffer(buf []byte) {
	buf = buf[:0]
	vbp.BasePool.Put(buf)
}

// RowIDBufferPool manages row ID buffer resources
type RowIDBufferPool struct {
	BasePool
}

// NewRowIDBufferPool creates a new row ID buffer pool
func NewRowIDBufferPool() *RowIDBufferPool {
	return &RowIDBufferPool{
		BasePool: *NewBasePool("rowIDBuffer", func() interface{} {
			return make([]byte, 0, 64) // Row IDs are small integers
		}),
	}
}

// AcquireRowIDBuffer gets a row ID buffer from the pool
func (ribp *RowIDBufferPool) AcquireRowIDBuffer() []byte {
	buf := ribp.BasePool.pool.Get()
	fromPool := buf != nil

	if !fromPool {
		return make([]byte, 0, 64)
	}

	b := buf.([]byte)
	return b[:cap(b)]
}

// ReleaseRowIDBuffer returns a row ID buffer to the pool
func (ribp *RowIDBufferPool) ReleaseRowIDBuffer(buf []byte) {
	buf = buf[:0]
	ribp.BasePool.Put(buf)
}

// AcquireKeyBuffer gets a key buffer from the pool
func (bp *BufferPool) AcquireKeyBuffer() []byte {
	return bp.AcquireWithMinSize(128)
}

// ReleaseKeyBuffer returns a key buffer to the pool
func (bp *BufferPool) ReleaseKeyBuffer(buf []byte) {
	bp.Release(buf)
}

// AcquireValueBuffer gets a value buffer from the pool
func (bp *BufferPool) AcquireValueBuffer() []byte {
	return bp.AcquireWithMinSize(256)
}

// ReleaseValueBuffer returns a value buffer to the pool
func (bp *BufferPool) ReleaseValueBuffer(buf []byte) {
	bp.Release(buf)
}

// AcquireRowIDBuffer gets a row ID buffer from the pool
func (bp *BufferPool) AcquireRowIDBuffer() []byte {
	return bp.AcquireWithMinSize(64)
}

// ReleaseRowIDBuffer returns a row ID buffer to the pool
func (bp *BufferPool) ReleaseRowIDBuffer(buf []byte) {
	bp.Release(buf)
}