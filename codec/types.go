package codec

import (
	"github.com/guileen/pglitedb/table"
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
	EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error)
	EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error)
	EncodePKKey(tenantID, tableID int64, pkValue interface{}) ([]byte, error)
	EncodeMetaKey(tenantID int64, metaType string, key string) []byte
	EncodeSequenceKey(tenantID int64, seqName string) []byte

	DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error)
	DecodeIndexKey(key []byte) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error)
	DecodeIndexKeyWithSchema(key []byte, indexColumnTypes []table.ColumnType) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error)
	DecodePKKey(key []byte) (tenantID, tableID int64, err error)

	EncodeRow(row *table.Record, schemaDef *table.TableDefinition) ([]byte, error)
	DecodeRow(data []byte, schemaDef *table.TableDefinition) (*table.Record, error)

	EncodeValue(value interface{}, colType table.ColumnType) ([]byte, error)
	DecodeValue(data []byte, colType table.ColumnType) (interface{}, error)

	EncodeCompositeKey(values []interface{}, types []table.ColumnType) ([]byte, error)
	DecodeCompositeKey(data []byte, types []table.ColumnType) ([]interface{}, error)
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
