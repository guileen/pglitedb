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

// Global pool for Value objects
var valuePool = &sync.Pool{
	New: func() interface{} {
		return &types.Value{}
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

// AcquireValue gets a Value from the pool
func AcquireValue() *types.Value {
	obj := valuePool.Get()
	if obj != nil {
		value := obj.(*types.Value)
		value.Data = nil
		value.Type = types.ColumnType("")
		return value
	}
	
	// Create a new value
	value := &types.Value{}
	return value
}

// ReleaseValue returns a Value to the pool
func ReleaseValue(value *types.Value) {
	value.Data = nil
	value.Type = types.ColumnType("")
	valuePool.Put(value)
}

type memcodec struct {
	keyBufferPools *KeyBufferPools
}

func NewMemComparableCodec() Codec {
	return &memcodec{
		keyBufferPools: NewKeyBufferPools(),
	}
}