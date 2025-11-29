package pools

// KeyPools manages various key buffer resources
type KeyPools struct {
	indexKeyPool     BasePool // Pool for index keys
	tableKeyPool     BasePool // Pool for table keys
	metaKeyPool      BasePool // Pool for metadata keys
	compositeKeyPool BasePool // Pool for composite keys
}

// NewKeyPools creates new key pools
func NewKeyPools() *KeyPools {
	return &KeyPools{
		indexKeyPool:     *NewBasePool("indexKey", func() interface{} { return make([]byte, 0, 64) }),
		tableKeyPool:     *NewBasePool("tableKey", func() interface{} { return make([]byte, 0, 32) }),
		metaKeyPool:      *NewBasePool("metaKey", func() interface{} { return make([]byte, 0, 128) }),
		compositeKeyPool: *NewBasePool("compositeKey", func() interface{} { return make([]byte, 0, 128) }),
	}
}

// AcquireIndexKeyBuffer gets a buffer optimized for index keys
func (kp *KeyPools) AcquireIndexKeyBuffer(size int) []byte {
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
	kp.indexKeyPool.Put(buf)
}

// AcquireTableKeyBuffer gets a buffer optimized for table keys
func (kp *KeyPools) AcquireTableKeyBuffer(size int) []byte {
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
	kp.tableKeyPool.Put(buf)
}

// AcquireMetaKeyBuffer gets a buffer optimized for metadata keys
func (kp *KeyPools) AcquireMetaKeyBuffer(size int) []byte {
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
	kp.metaKeyPool.Put(buf)
}

// AcquireCompositeKeyBuffer gets a buffer optimized for composite keys
func (kp *KeyPools) AcquireCompositeKeyBuffer(size int) []byte {
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
	kp.compositeKeyPool.Put(buf)
}