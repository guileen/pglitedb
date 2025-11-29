package scan

import (
	dbTypes "github.com/guileen/pglitedb/types"
)

// mockIterator is a mock storage iterator for testing
type mockIterator struct {
	closed bool
}

func (m *mockIterator) First() bool { return true }
func (m *mockIterator) Last() bool  { return true }
func (m *mockIterator) Next() bool  { return true }
func (m *mockIterator) Prev() bool  { return true }
func (m *mockIterator) Seek(key []byte) bool { return true }
func (m *mockIterator) Valid() bool { return true }
func (m *mockIterator) Key() []byte { return []byte("key") }
func (m *mockIterator) Value() []byte { return []byte("value") }
func (m *mockIterator) Error() error { return nil }
func (m *mockIterator) Close() error { 
	m.closed = true
	return nil 
}

// mockCodec is a mock codec for testing
type mockCodec struct{}

func (m *mockCodec) EncodeTableKey(tenantID, tableID, rowID int64) []byte {
	return []byte("table_key")
}

func (m *mockCodec) EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error) {
	return []byte("index_key"), nil
}

func (m *mockCodec) EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte {
	return []byte("index_start")
}

func (m *mockCodec) EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte {
	return []byte("index_end")
}

func (m *mockCodec) DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error) {
	return 1, 1, 1, nil
}

func (m *mockCodec) DecodeRow(value []byte, schemaDef interface{}) (interface{}, error) {
	return &dbTypes.Record{}, nil
}

func (m *mockCodec) DecodeIndexKeyWithSchema(key []byte, columnTypes []interface{}) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	return 1, 1, 1, []interface{}{"value"}, 1, nil
}

func (m *mockCodec) ExtractRowIDFromIndexKey(key []byte) (rowID int64, err error) {
	return 1, nil
}

func (m *mockCodec) EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error) {
	return []byte("composite_key"), nil
}