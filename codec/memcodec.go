package codec

import (
	"sync"
	"time"

	"github.com/guileen/pglitedb/types"
)

// Global pool for EncodedRow objects
var encodedRowPool = &sync.Pool{
	New: func() interface{} {
		return &EncodedRow{
			Columns: make(map[string][]byte),
		}
	},
}

// Global pool for decoded Record objects
var decodedRecordPool = &sync.Pool{
	New: func() interface{} {
		return &types.Record{
			Data: make(map[string]*types.Value),
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

// AcquireDecodedRecord gets a Record from the pool
func AcquireDecodedRecord() *types.Record {
	obj := decodedRecordPool.Get()
	if obj != nil {
		record := obj.(*types.Record)
		// Clear the map without reallocating
		for k := range record.Data {
			delete(record.Data, k)
		}
		record.ID = ""
		record.Table = ""
		record.CreatedAt = time.Time{}
		record.UpdatedAt = time.Time{}
		record.Version = 0
		return record
	}
	
	// Create a new record
	record := &types.Record{
		Data: make(map[string]*types.Value),
	}
	return record
}

// ReleaseDecodedRecord returns a Record to the pool
func ReleaseDecodedRecord(record *types.Record) {
	// Clear the map without reallocating
	for k := range record.Data {
		delete(record.Data, k)
	}
	
	decodedRecordPool.Put(record)
}

type memcodec struct {
	keyBufferPools *KeyBufferPools
}

func NewMemComparableCodec() Codec {
	return &memcodec{
		keyBufferPools: NewKeyBufferPools(),
	}
}