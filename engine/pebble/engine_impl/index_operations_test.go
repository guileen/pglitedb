package engine_impl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockKV is a mock implementation of the KV interface for testing
type mockKV struct {
	data map[string][]byte
}

func (m *mockKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if m.data == nil {
		return nil, storage.ErrNotFound
	}
	value, exists := m.data[string(key)]
	if !exists {
		return nil, storage.ErrNotFound
	}
	return value, nil
}

func (m *mockKV) Set(ctx context.Context, key, value []byte) error {
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	m.data[string(key)] = value
	return nil
}

func (m *mockKV) SetWithOptions(ctx context.Context, key, value []byte, opts *storage.WriteOptions) error {
	return m.Set(ctx, key, value)
}

func (m *mockKV) Delete(ctx context.Context, key []byte) error {
	if m.data != nil {
		delete(m.data, string(key))
	}
	return nil
}

func (m *mockKV) DeleteWithOptions(ctx context.Context, key []byte, opts *storage.WriteOptions) error {
	return m.Delete(ctx, key)
}

func (m *mockKV) NewBatch() storage.Batch {
	// Simplified batch implementation for testing
	return &mockBatch{kv: m}
}

func (m *mockKV) Commit(ctx context.Context, batch storage.Batch) error {
	return nil
}

func (m *mockKV) CommitBatch(ctx context.Context, batch storage.Batch) error {
	return nil
}

func (m *mockKV) CommitBatchWithOptions(ctx context.Context, batch storage.Batch, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKV) NewIterator(opts *storage.IteratorOptions) storage.Iterator {
	return &mockIterator{kv: m, opts: opts}
}

func (m *mockKV) NewSnapshot() (storage.Snapshot, error) {
	return nil, nil
}

func (m *mockKV) NewTransaction(ctx context.Context) (storage.Transaction, error) {
	return nil, nil
}

func (m *mockKV) Stats() storage.KVStats {
	return storage.KVStats{}
}

func (m *mockKV) Flush() error {
	return nil
}

func (m *mockKV) Close() error {
	return nil
}

func (m *mockKV) CheckForConflicts(txn storage.Transaction, key []byte) error {
	return nil
}

// mockBatch is a mock implementation of the Batch interface
type mockBatch struct {
	kv *mockKV
}

func (m *mockBatch) Set(key, value []byte) error {
	return m.kv.Set(context.Background(), key, value)
}

func (m *mockBatch) Delete(key []byte) error {
	return m.kv.Delete(context.Background(), key)
}

func (m *mockBatch) Count() int {
	return 0
}

func (m *mockBatch) Reset() {}

func (m *mockBatch) Close() error {
	return nil
}

// mockIterator is a mock implementation of the Iterator interface
type mockIterator struct {
	kv      *mockKV
	opts    *storage.IteratorOptions
	keys    []string
	current int
	closed  bool
}

func (m *mockIterator) Valid() bool {
	return !m.closed && m.current >= 0 && m.current < len(m.keys)
}

func (m *mockIterator) Next() bool {
	if m.closed || m.current >= len(m.keys)-1 {
		return false
	}
	m.current++
	return true
}

func (m *mockIterator) Prev() bool {
	if m.closed || m.current <= 0 {
		return false
	}
	m.current--
	return true
}

func (m *mockIterator) Key() []byte {
	if m.closed || !m.Valid() {
		return nil
	}
	return []byte(m.keys[m.current])
}

func (m *mockIterator) Value() []byte {
	if m.closed || !m.Valid() {
		return nil
	}
	key := m.keys[m.current]
	if m.kv.data != nil {
		return m.kv.data[key]
	}
	return nil
}

func (m *mockIterator) Error() error {
	return nil
}

func (m *mockIterator) SeekGE(key []byte) bool {
	return false
}

func (m *mockIterator) SeekLT(key []byte) bool {
	return false
}

func (m *mockIterator) First() bool {
	if m.closed || len(m.keys) == 0 {
		return false
	}
	m.current = 0
	return true
}

func (m *mockIterator) Last() bool {
	if m.closed || len(m.keys) == 0 {
		return false
	}
	m.current = len(m.keys) - 1
	return true
}

func (m *mockIterator) Close() error {
	m.closed = true
	return nil
}

func TestIndexOperations(t *testing.T) {
	t.Run("NewIndexOperations", func(t *testing.T) {
		// Test that NewIndexOperations creates a valid instance
		kv := &mockKV{}
		codec := codec.NewMemComparableCodec()
		
		io := NewIndexOperations(kv, codec)
		assert.NotNil(t, io)
		assert.Equal(t, kv, io.kv)
		assert.Equal(t, codec, io.codec)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		// Test CreateIndex method
		kv := &mockKV{}
		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kv, codec)
		
		ctx := context.Background()
		indexDef := &types.IndexDefinition{
			Name:    "test_index",
			Columns: []string{"name"},
			Unique:  false,
			Type:    "btree",
		}
		
		err := io.CreateIndex(ctx, 1, 1, indexDef)
		assert.NoError(t, err)
		// Currently the implementation just returns nil, so we're testing it doesn't panic
	})

	t.Run("DropIndex", func(t *testing.T) {
		// Test DropIndex method
		kv := &mockKV{}
		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kv, codec)
		
		ctx := context.Background()
		err := io.DropIndex(ctx, 1, 1, 1)
		assert.NoError(t, err)
		// Currently the implementation just returns nil, so we're testing it doesn't panic
	})

	t.Run("LookupIndex", func(t *testing.T) {
		// Test LookupIndex method with real KV store for proper testing
		tmpDir, err := os.MkdirTemp("", "index-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
		kvStore, err := storage.NewPebbleKV(config)
		require.NoError(t, err)
		defer kvStore.Close()

		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kvStore, codec)

		ctx := context.Background()
		
		// Test with empty index (should return empty slice)
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		assert.NoError(t, err)
		assert.Empty(t, rowIDs)
		
		// Test with actual index data
		// Manually insert some index entries
		indexKey, err := codec.EncodeIndexKey(1, 1, 1, "test_value", 100)
		require.NoError(t, err)
		
		err = kvStore.Set(ctx, indexKey, []byte{})
		require.NoError(t, err)
		
		// Now lookup should return the row ID
		rowIDs, err = io.LookupIndex(ctx, 1, 1, 1, "test_value")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, int64(100), rowIDs[0])
		
		// Test with multiple entries
		indexKey2, err := codec.EncodeIndexKey(1, 1, 1, "test_value", 200)
		require.NoError(t, err)
		
		err = kvStore.Set(ctx, indexKey2, []byte{})
		require.NoError(t, err)
		
		// Now lookup should return both row IDs
		rowIDs, err = io.LookupIndex(ctx, 1, 1, 1, "test_value")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 2)
		assert.Contains(t, rowIDs, int64(100))
		assert.Contains(t, rowIDs, int64(200))
	})
	
	t.Run("LookupIndexWithDifferentTenantAndTable", func(t *testing.T) {
		// Test LookupIndex with different tenant and table IDs
		tmpDir, err := os.MkdirTemp("", "index-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
		kvStore, err := storage.NewPebbleKV(config)
		require.NoError(t, err)
		defer kvStore.Close()

		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kvStore, codec)

		ctx := context.Background()
		
		// Insert entries for different tenants and tables
		indexKey1, err := codec.EncodeIndexKey(1, 1, 1, "test_value", 100)
		require.NoError(t, err)
		err = kvStore.Set(ctx, indexKey1, []byte{})
		require.NoError(t, err)
		
		indexKey2, err := codec.EncodeIndexKey(2, 1, 1, "test_value", 200)
		require.NoError(t, err)
		err = kvStore.Set(ctx, indexKey2, []byte{})
		require.NoError(t, err)
		
		indexKey3, err := codec.EncodeIndexKey(1, 2, 1, "test_value", 300)
		require.NoError(t, err)
		err = kvStore.Set(ctx, indexKey3, []byte{})
		require.NoError(t, err)
		
		// Lookup for tenant 1, table 1 should only return row 100
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, int64(100), rowIDs[0])
		
		// Lookup for tenant 2, table 1 should only return row 200
		rowIDs, err = io.LookupIndex(ctx, 2, 1, 1, "test_value")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, int64(200), rowIDs[0])
		
		// Lookup for tenant 1, table 2 should only return row 300
		rowIDs, err = io.LookupIndex(ctx, 1, 2, 1, "test_value")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, int64(300), rowIDs[0])
	})
	
	t.Run("LookupIndexWithDifferentIndexIDs", func(t *testing.T) {
		// Test LookupIndex with different index IDs
		tmpDir, err := os.MkdirTemp("", "index-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
		kvStore, err := storage.NewPebbleKV(config)
		require.NoError(t, err)
		defer kvStore.Close()

		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kvStore, codec)

		ctx := context.Background()
		
		// Insert entries for different index IDs
		indexKey1, err := codec.EncodeIndexKey(1, 1, 1, "test_value", 100)
		require.NoError(t, err)
		err = kvStore.Set(ctx, indexKey1, []byte{})
		require.NoError(t, err)
		
		indexKey2, err := codec.EncodeIndexKey(1, 1, 2, "test_value", 200)
		require.NoError(t, err)
		err = kvStore.Set(ctx, indexKey2, []byte{})
		require.NoError(t, err)
		
		// Lookup for index 1 should only return row 100
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, int64(100), rowIDs[0])
		
		// Lookup for index 2 should only return row 200
		rowIDs, err = io.LookupIndex(ctx, 1, 1, 2, "test_value")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, int64(200), rowIDs[0])
	})
	
	t.Run("LookupIndexWithDifferentValues", func(t *testing.T) {
		// Test LookupIndex with different index values
		tmpDir, err := os.MkdirTemp("", "index-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
		kvStore, err := storage.NewPebbleKV(config)
		require.NoError(t, err)
		defer kvStore.Close()

		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kvStore, codec)

		ctx := context.Background()
		
		// Insert entries for different values
		indexKey1, err := codec.EncodeIndexKey(1, 1, 1, "value1", 100)
		require.NoError(t, err)
		err = kvStore.Set(ctx, indexKey1, []byte{})
		require.NoError(t, err)
		
		indexKey2, err := codec.EncodeIndexKey(1, 1, 1, "value2", 200)
		require.NoError(t, err)
		err = kvStore.Set(ctx, indexKey2, []byte{})
		require.NoError(t, err)
		
		// Lookup for value1 should only return row 100
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "value1")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, int64(100), rowIDs[0])
		
		// Lookup for value2 should only return row 200
		rowIDs, err = io.LookupIndex(ctx, 1, 1, 1, "value2")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, 1)
		assert.Equal(t, int64(200), rowIDs[0])
		
		// Lookup for non-existent value should return empty slice
		rowIDs, err = io.LookupIndex(ctx, 1, 1, 1, "non_existent")
		assert.NoError(t, err)
		assert.Empty(t, rowIDs)
	})
}

func TestIndexOperationsEdgeCases(t *testing.T) {
	t.Run("LookupIndexWithNilKV", func(t *testing.T) {
		// Test LookupIndex with nil KV store (should handle gracefully)
		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(nil, codec)
		
		ctx := context.Background()
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		// Should return an error since KV is nil
		assert.Error(t, err)
		assert.Nil(t, rowIDs)
	})
	
	t.Run("LookupIndexWithNilCodec", func(t *testing.T) {
		// Test LookupIndex with nil codec (should handle gracefully)
		kv := &mockKV{}
		io := NewIndexOperations(kv, nil)
		
		ctx := context.Background()
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		// Should return an error since codec is nil
		assert.Error(t, err)
		assert.Nil(t, rowIDs)
	})
	
	t.Run("LookupIndexWithContextCancellation", func(t *testing.T) {
		// Test LookupIndex with cancelled context
		tmpDir, err := os.MkdirTemp("", "index-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
		kvStore, err := storage.NewPebbleKV(config)
		require.NoError(t, err)
		defer kvStore.Close()

		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kvStore, codec)

		// Create a cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		// Depending on implementation, this might return an error or empty result
		// Either way, it should handle the cancelled context gracefully
		// With the updated implementation, we expect a context cancellation error
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, rowIDs)
	})
	
	t.Run("LookupIndexWithLargeNumberOfEntries", func(t *testing.T) {
		// Test LookupIndex with a large number of entries
		tmpDir, err := os.MkdirTemp("", "index-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
		kvStore, err := storage.NewPebbleKV(config)
		require.NoError(t, err)
		defer kvStore.Close()

		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kvStore, codec)

		ctx := context.Background()
		
		// Insert many entries
		numEntries := 1000
		for i := 0; i < numEntries; i++ {
			indexKey, err := codec.EncodeIndexKey(1, 1, 1, "test_value", int64(i))
			require.NoError(t, err)
			err = kvStore.Set(ctx, indexKey, []byte{})
			require.NoError(t, err)
		}
		
		// Lookup should return all entries
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		assert.NoError(t, err)
		assert.Len(t, rowIDs, numEntries)
		
		// Verify all expected row IDs are present
		for i := 0; i < numEntries; i++ {
			assert.Contains(t, rowIDs, int64(i))
		}
	})
}

func TestIndexOperationsErrorHandling(t *testing.T) {
	t.Run("LookupIndexWithIteratorCreationFailure", func(t *testing.T) {
		// Test LookupIndex when iterator creation fails
		// This is difficult to test with real KV, so we'll use a mock that simulates failure
		kv := &mockKVThatFailsIteratorCreation{}
		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kv, codec)
		
		ctx := context.Background()
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		assert.Error(t, err)
		assert.Nil(t, rowIDs)
		assert.Contains(t, err.Error(), "create iterator")
	})
	
	t.Run("LookupIndexWithDecodeError", func(t *testing.T) {
		// Test LookupIndex when decoding fails
		// We'll use a mock KV that returns invalid keys
		kv := &mockKVWithInvalidKeys{}
		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kv, codec)
		
		ctx := context.Background()
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		assert.Error(t, err)
		assert.Nil(t, rowIDs)
		assert.Contains(t, err.Error(), "decode index key")
	})
	
	t.Run("LookupIndexWithIteratorError", func(t *testing.T) {
		// Test LookupIndex when iterator has an error
		kv := &mockKVWithIteratorError{}
		codec := codec.NewMemComparableCodec()
		io := NewIndexOperations(kv, codec)
		
		ctx := context.Background()
		rowIDs, err := io.LookupIndex(ctx, 1, 1, 1, "test_value")
		// With the updated implementation, we expect an error to be returned
		assert.Error(t, err)
		assert.Nil(t, rowIDs)
	})
}

// Additional mock types for error testing
type mockKVThatFailsIteratorCreation struct{}

func (m *mockKVThatFailsIteratorCreation) Get(ctx context.Context, key []byte) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (m *mockKVThatFailsIteratorCreation) Set(ctx context.Context, key, value []byte) error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) SetWithOptions(ctx context.Context, key, value []byte, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) Delete(ctx context.Context, key []byte) error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) DeleteWithOptions(ctx context.Context, key []byte, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) NewBatch() storage.Batch {
	return &mockBatch{kv: &mockKV{}}
}

func (m *mockKVThatFailsIteratorCreation) Commit(ctx context.Context, batch storage.Batch) error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) CommitBatch(ctx context.Context, batch storage.Batch) error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) CommitBatchWithOptions(ctx context.Context, batch storage.Batch, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) NewIterator(opts *storage.IteratorOptions) storage.Iterator {
	return nil // Return nil to simulate failure
}

func (m *mockKVThatFailsIteratorCreation) NewSnapshot() (storage.Snapshot, error) {
	return nil, nil
}

func (m *mockKVThatFailsIteratorCreation) NewTransaction(ctx context.Context) (storage.Transaction, error) {
	return nil, nil
}

func (m *mockKVThatFailsIteratorCreation) Stats() storage.KVStats {
	return storage.KVStats{}
}

func (m *mockKVThatFailsIteratorCreation) Flush() error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) Close() error {
	return nil
}

func (m *mockKVThatFailsIteratorCreation) CheckForConflicts(txn storage.Transaction, key []byte) error {
	return nil
}

type mockKVWithInvalidKeys struct{}

func (m *mockKVWithInvalidKeys) Get(ctx context.Context, key []byte) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (m *mockKVWithInvalidKeys) Set(ctx context.Context, key, value []byte) error {
	return nil
}

func (m *mockKVWithInvalidKeys) SetWithOptions(ctx context.Context, key, value []byte, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVWithInvalidKeys) Delete(ctx context.Context, key []byte) error {
	return nil
}

func (m *mockKVWithInvalidKeys) DeleteWithOptions(ctx context.Context, key []byte, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVWithInvalidKeys) NewBatch() storage.Batch {
	return &mockBatch{kv: &mockKV{}}
}

func (m *mockKVWithInvalidKeys) Commit(ctx context.Context, batch storage.Batch) error {
	return nil
}

func (m *mockKVWithInvalidKeys) CommitBatch(ctx context.Context, batch storage.Batch) error {
	return nil
}

func (m *mockKVWithInvalidKeys) CommitBatchWithOptions(ctx context.Context, batch storage.Batch, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVWithInvalidKeys) NewIterator(opts *storage.IteratorOptions) storage.Iterator {
	// Return an iterator that yields invalid keys
	return &mockIteratorWithInvalidKeys{}
}

func (m *mockKVWithInvalidKeys) NewSnapshot() (storage.Snapshot, error) {
	return nil, nil
}

func (m *mockKVWithInvalidKeys) NewTransaction(ctx context.Context) (storage.Transaction, error) {
	return nil, nil
}

func (m *mockKVWithInvalidKeys) Stats() storage.KVStats {
	return storage.KVStats{}
}

func (m *mockKVWithInvalidKeys) Flush() error {
	return nil
}

func (m *mockKVWithInvalidKeys) Close() error {
	return nil
}

func (m *mockKVWithInvalidKeys) CheckForConflicts(txn storage.Transaction, key []byte) error {
	return nil
}

type mockIteratorWithInvalidKeys struct {
	current int
}

func (m *mockIteratorWithInvalidKeys) Valid() bool {
	return m.current == 0
}

func (m *mockIteratorWithInvalidKeys) Next() bool {
	m.current++
	return false
}

func (m *mockIteratorWithInvalidKeys) Prev() bool {
	return false
}

func (m *mockIteratorWithInvalidKeys) Key() []byte {
	// Return an invalid key that cannot be decoded
	return []byte("invalid_key")
}

func (m *mockIteratorWithInvalidKeys) Value() []byte {
	return []byte{}
}

func (m *mockIteratorWithInvalidKeys) Error() error {
	return nil
}

func (m *mockIteratorWithInvalidKeys) SeekGE(key []byte) bool {
	return false
}

func (m *mockIteratorWithInvalidKeys) SeekLT(key []byte) bool {
	return false
}

func (m *mockIteratorWithInvalidKeys) First() bool {
	return true
}

func (m *mockIteratorWithInvalidKeys) Last() bool {
	return false
}

func (m *mockIteratorWithInvalidKeys) Close() error {
	return nil
}

type mockKVWithIteratorError struct{}

func (m *mockKVWithIteratorError) Get(ctx context.Context, key []byte) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (m *mockKVWithIteratorError) Set(ctx context.Context, key, value []byte) error {
	return nil
}

func (m *mockKVWithIteratorError) SetWithOptions(ctx context.Context, key, value []byte, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVWithIteratorError) Delete(ctx context.Context, key []byte) error {
	return nil
}

func (m *mockKVWithIteratorError) DeleteWithOptions(ctx context.Context, key []byte, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVWithIteratorError) NewBatch() storage.Batch {
	return &mockBatch{kv: &mockKV{}}
}

func (m *mockKVWithIteratorError) Commit(ctx context.Context, batch storage.Batch) error {
	return nil
}

func (m *mockKVWithIteratorError) CommitBatch(ctx context.Context, batch storage.Batch) error {
	return nil
}

func (m *mockKVWithIteratorError) CommitBatchWithOptions(ctx context.Context, batch storage.Batch, opts *storage.WriteOptions) error {
	return nil
}

func (m *mockKVWithIteratorError) NewIterator(opts *storage.IteratorOptions) storage.Iterator {
	return &mockIteratorWithError{}
}

func (m *mockKVWithIteratorError) NewSnapshot() (storage.Snapshot, error) {
	return nil, nil
}

func (m *mockKVWithIteratorError) NewTransaction(ctx context.Context) (storage.Transaction, error) {
	return nil, nil
}

func (m *mockKVWithIteratorError) Stats() storage.KVStats {
	return storage.KVStats{}
}

func (m *mockKVWithIteratorError) Flush() error {
	return nil
}

func (m *mockKVWithIteratorError) Close() error {
	return nil
}

func (m *mockKVWithIteratorError) CheckForConflicts(txn storage.Transaction, key []byte) error {
	return nil
}

type mockIteratorWithError struct {
	errorReturned bool
}

func (m *mockIteratorWithError) Valid() bool {
	return !m.errorReturned
}

func (m *mockIteratorWithError) Next() bool {
	return false
}

func (m *mockIteratorWithError) Prev() bool {
	return false
}

func (m *mockIteratorWithError) Key() []byte {
	return []byte("valid_key")
}

func (m *mockIteratorWithError) Value() []byte {
	return []byte{}
}

func (m *mockIteratorWithError) Error() error {
	if !m.errorReturned {
		m.errorReturned = true
		return fmt.Errorf("simulated iterator error")
	}
	return nil
}

func (m *mockIteratorWithError) SeekGE(key []byte) bool {
	return false
}

func (m *mockIteratorWithError) SeekLT(key []byte) bool {
	return false
}

func (m *mockIteratorWithError) First() bool {
	return true
}

func (m *mockIteratorWithError) Last() bool {
	return false
}

func (m *mockIteratorWithError) Close() error {
	return nil
}