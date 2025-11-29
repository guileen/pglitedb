package pools

import (
	"github.com/guileen/pglitedb/codec"
)

// KeyEncoderPool manages key encoder resources
type KeyEncoderPool struct {
	BasePool
}

// NewKeyEncoderPool creates a new key encoder pool
func NewKeyEncoderPool() *KeyEncoderPool {
	return &KeyEncoderPool{
		BasePool: *NewBasePool("keyEncoder", func() interface{} {
			return codec.NewMemComparableCodec()
		}),
	}
}

// Acquire gets a key encoder from the pool
func (kep *KeyEncoderPool) Acquire() interface{} {
	encoder := kep.BasePool.pool.Get()
	fromPool := encoder != nil

	if !fromPool {
		encoder = codec.NewMemComparableCodec()
	}

	return encoder
}

// Release returns a key encoder to the pool
func (kep *KeyEncoderPool) Release(encoder interface{}) {
	kep.BasePool.Put(encoder)
}