// Package codec provides the codec module for the database
package codec

import (
	"github.com/guileen/pglitedb/types"
)

type KeyType byte

const (
	KeyTypeMeta     KeyType = 'm'
	KeyTypeTable    KeyType = 't'
	KeyTypeRow      KeyType = 'r'
	KeyTypePK       KeyType = 'p'
	KeyTypeIndex    KeyType = 'i'
	KeyTypeFTS      KeyType = 'f'
	KeyTypeJSON     KeyType = 'j'
	KeyTypeSequence KeyType = 's'
)

type Codec interface {
	EncodeTableKey(tenantID, tableID, rowID int64) []byte
	EncodeTableKeyBuffer(tenantID, tableID, rowID int64, buf []byte) ([]byte, error)
	EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error)
	EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error)
	EncodePKKey(tenantID, tableID int64, pkValue interface{}) ([]byte, error)
	EncodeMetaKey(tenantID int64, metaType string, key string) []byte
	EncodeSequenceKey(tenantID int64, seqName string) []byte
	EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte
	EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte

	DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error)
	DecodeIndexKey(key []byte) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error)
	DecodeIndexKeyWithSchema(key []byte, indexColumnTypes []types.ColumnType) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error)
	DecodePKKey(key []byte) (tenantID, tableID int64, err error)
	
	// ExtractRowIDFromIndexKey efficiently extracts just the rowID from an index key
	ExtractRowIDFromIndexKey(key []byte) (int64, error)

	EncodeRow(row *types.Record, schemaDef *types.TableDefinition) ([]byte, error)
	DecodeRow(data []byte, schemaDef *types.TableDefinition) (*types.Record, error)

	EncodeValue(value interface{}, colType types.ColumnType) ([]byte, error)
	DecodeValue(data []byte, colType types.ColumnType) (interface{}, error)

	EncodeCompositeKey(values []interface{}, types []types.ColumnType) ([]byte, error)
	DecodeCompositeKey(data []byte, types []types.ColumnType) ([]interface{}, error)
	
	// Release methods for returning buffers to pools
	ReleaseTableKey(buf []byte)
	ReleaseIndexKey(buf []byte)
	ReleaseCompositeIndexKey(buf []byte)
	ReleasePKKey(buf []byte)
	ReleaseMetaKey(buf []byte)
	ReleaseSequenceKey(buf []byte)
	ReleaseIndexScanKey(buf []byte)
}

type EncodedRow struct {
	SchemaVersion uint32
	Columns       map[string][]byte
	CreatedAt     int64
	UpdatedAt     int64
	Version       int
	// Transaction metadata for isolation levels
	TxnID        uint64 // Transaction ID that last modified this row
	TxnTimestamp int64  // Timestamp when the transaction started
}