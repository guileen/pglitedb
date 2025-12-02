package testutils

import (
	"context"
	"errors"

	"github.com/guileen/pglitedb/storage/shared"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// MockKV is a mock implementation of storage.KV for testing
type MockKV struct {
	Data map[string][]byte
}

func NewMockKV() *MockKV {
	return &MockKV{
		Data: make(map[string][]byte),
	}
}

func (m *MockKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if val, ok := m.Data[string(key)]; ok {
		return val, nil
	}
	return nil, shared.ErrNotFound
}

func (m *MockKV) Set(ctx context.Context, key, value []byte) error {
	m.Data[string(key)] = value
	return nil
}

func (m *MockKV) SetWithOptions(ctx context.Context, key, value []byte, opts *shared.WriteOptions) error {
	return m.Set(ctx, key, value)
}

func (m *MockKV) Delete(ctx context.Context, key []byte) error {
	delete(m.Data, string(key))
	return nil
}

func (m *MockKV) DeleteWithOptions(ctx context.Context, key []byte, opts *shared.WriteOptions) error {
	return m.Delete(ctx, key)
}

func (m *MockKV) NewBatch() storage.Batch {
	return &MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
}

func (m *MockKV) Commit(ctx context.Context, batch storage.Batch) error {
	mockBatch, ok := batch.(*MockBatch)
	if !ok {
		return errors.New("invalid batch")
	}
	
	for key, value := range mockBatch.Data {
		m.Data[key] = value
	}
	
	for key := range mockBatch.Deleted {
		delete(m.Data, key)
	}
	
	return nil
}

func (m *MockKV) CommitBatch(ctx context.Context, batch storage.Batch) error {
	return m.Commit(ctx, batch)
}

func (m *MockKV) CommitBatchWithOptions(ctx context.Context, batch storage.Batch, opts *shared.WriteOptions) error {
	return m.Commit(ctx, batch)
}

func (m *MockKV) NewIterator(opts *shared.IteratorOptions) storage.Iterator {
	// Simplified iterator implementation for testing
	return &MockIterator{}
}

func (m *MockKV) NewSnapshot() (storage.Snapshot, error) {
	return &MockSnapshot{}, nil
}

func (m *MockKV) NewTransaction(ctx context.Context) (storage.Transaction, error) {
	return &MockTransaction{}, nil
}

func (m *MockKV) Stats() shared.KVStats {
	return shared.KVStats{}
}

func (m *MockKV) Flush() error {
	return nil
}

func (m *MockKV) Close() error {
	return nil
}

func (m *MockKV) CheckForConflicts(txn storage.Transaction, key []byte) error {
	return nil
}

func (m *MockKV) Scan(ctx context.Context, startKey, endKey []byte) (storage.Iterator, error) {
	return &MockIterator{}, nil
}

// MockBatch is a mock implementation of storage.Batch for testing
type MockBatch struct {
	Data    map[string][]byte
	Deleted map[string]bool
}

func (m *MockBatch) Set(key, value []byte) error {
	m.Data[string(key)] = value
	delete(m.Deleted, string(key))
	return nil
}

func (m *MockBatch) Delete(key []byte) error {
	delete(m.Data, string(key))
	m.Deleted[string(key)] = true
	return nil
}

func (m *MockBatch) Count() int {
	return len(m.Data) + len(m.Deleted)
}

func (m *MockBatch) Reset() {
	m.Data = make(map[string][]byte)
	m.Deleted = make(map[string]bool)
}

func (m *MockBatch) Close() error {
	return nil
}

// MockCodec is a mock implementation of codec.Codec for testing
type MockCodec struct{}

func (m *MockCodec) EncodeTableKey(tenantID, tableID, rowID int64) []byte {
	return []byte{}
}

func (m *MockCodec) EncodeTableKeyBuffer(tenantID, tableID, rowID int64, buf []byte) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCodec) EncodeIndexKey(tenantID, tableID, indexID int64, indexValue interface{}, rowID int64) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCodec) EncodeCompositeIndexKey(tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCodec) EncodePKKey(tenantID, tableID int64, pkValue interface{}) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCodec) EncodeMetaKey(tenantID int64, metaType string, key string) []byte {
	return []byte{}
}

func (m *MockCodec) EncodeSequenceKey(tenantID int64, seqName string) []byte {
	return []byte{}
}

func (m *MockCodec) EncodeIndexScanStartKey(tenantID, tableID, indexID int64) []byte {
	return []byte{}
}

func (m *MockCodec) EncodeIndexScanEndKey(tenantID, tableID, indexID int64) []byte {
	return []byte{}
}

func (m *MockCodec) DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error) {
	return 0, 0, 0, nil
}

func (m *MockCodec) DecodeIndexKey(key []byte) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	return 0, 0, 0, nil, 0, nil
}

func (m *MockCodec) DecodeIndexKeyWithSchema(key []byte, indexColumnTypes []dbTypes.ColumnType) (tenantID, tableID, indexID int64, indexValues []interface{}, rowID int64, err error) {
	return 0, 0, 0, nil, 0, nil
}

func (m *MockCodec) DecodePKKey(key []byte) (tenantID, tableID int64, err error) {
	return 0, 0, nil
}

func (m *MockCodec) ExtractRowIDFromIndexKey(key []byte) (int64, error) {
	return 0, nil
}

func (m *MockCodec) EncodeRow(row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCodec) DecodeRow(data []byte, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	return &dbTypes.Record{
		Data: make(map[string]*dbTypes.Value),
	}, nil
}

func (m *MockCodec) EncodeValue(value interface{}, colType dbTypes.ColumnType) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCodec) DecodeValue(data []byte, colType dbTypes.ColumnType) (interface{}, error) {
	return nil, nil
}

func (m *MockCodec) EncodeCompositeKey(values []interface{}, types []dbTypes.ColumnType) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockCodec) DecodeCompositeKey(data []byte, types []dbTypes.ColumnType) ([]interface{}, error) {
	return nil, nil
}

func (m *MockCodec) ReleaseTableKey(buf []byte) {}

func (m *MockCodec) ReleaseIndexKey(buf []byte) {}

func (m *MockCodec) ReleaseCompositeIndexKey(buf []byte) {}

func (m *MockCodec) ReleasePKKey(buf []byte) {}

func (m *MockCodec) ReleaseMetaKey(buf []byte) {}

func (m *MockCodec) ReleaseSequenceKey(buf []byte) {}

func (m *MockCodec) ReleaseIndexScanKey(buf []byte) {}

// Additional mock types for testing
type MockIterator struct{}

func (m *MockIterator) Valid() bool {
	return false
}

func (m *MockIterator) Next() bool {
	return false
}

func (m *MockIterator) Prev() bool {
	return false
}

func (m *MockIterator) Key() []byte {
	return []byte{}
}

func (m *MockIterator) Value() []byte {
	return []byte{}
}

func (m *MockIterator) Error() error {
	return nil
}

func (m *MockIterator) SeekGE(key []byte) bool {
	return false
}

func (m *MockIterator) SeekLT(key []byte) bool {
	return false
}

func (m *MockIterator) First() bool {
	return false
}

func (m *MockIterator) Last() bool {
	return false
}

func (m *MockIterator) Close() error {
	return nil
}

type MockSnapshot struct{}

func (m *MockSnapshot) Get(key []byte) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockSnapshot) NewIterator(opts *shared.IteratorOptions) storage.Iterator {
	return &MockIterator{}
}

func (m *MockSnapshot) Close() error {
	return nil
}

type MockTransaction struct{}

func (m *MockTransaction) Get(key []byte) ([]byte, error) {
	return []byte{}, nil
}

func (m *MockTransaction) Set(key, value []byte) error {
	return nil
}

func (m *MockTransaction) Delete(key []byte) error {
	return nil
}

func (m *MockTransaction) NewIterator(opts *shared.IteratorOptions) storage.Iterator {
	return &MockIterator{}
}

func (m *MockTransaction) Commit() error {
	return nil
}

func (m *MockTransaction) Rollback() error {
	return nil
}

func (m *MockTransaction) Isolation() shared.IsolationLevel {
	return shared.ReadCommitted
}

func (m *MockTransaction) SetIsolation(level shared.IsolationLevel) error {
	return nil
}

func (m *MockTransaction) Close() error {
	return nil
}