package codec

import (
	"sync"
)

// Global pool for EncodedRow objects
var encodedRowPool = &sync.Pool{
	New: func() interface{} {
		return &EncodedRow{
			Columns: make(map[string][]byte),
		}
	},
}

// AcquireEncodedRow gets an EncodedRow from the pool
func AcquireEncodedRow() *EncodedRow {
	obj := encodedRowPool.Get()
	if obj != nil {
		er := obj.(*EncodedRow)
		// Clear the map without reallocating
		for k := range er.Columns {
			delete(er.Columns, k)
		}
		er.SchemaVersion = 0
		er.CreatedAt = 0
		er.UpdatedAt = 0
		er.Version = 0
		er.TxnID = 0
		er.TxnTimestamp = 0
		return er
	}
	
	// Create a new encoded row
	er := &EncodedRow{
		Columns: make(map[string][]byte),
	}
	return er
}

// ReleaseEncodedRow returns an EncodedRow to the pool
func ReleaseEncodedRow(er *EncodedRow) {
	// Clear the map without reallocating
	for k := range er.Columns {
		delete(er.Columns, k)
	}
	
	encodedRowPool.Put(er)
}

type memcodec struct {
	keyBufferPools *KeyBufferPools
}

func NewMemComparableCodec() Codec {
	return &memcodec{
		keyBufferPools: NewKeyBufferPools(),
	}
}