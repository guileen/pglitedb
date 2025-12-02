package persistence

import (
	"context"
	"testing"
	
	"github.com/stretchr/testify/assert"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/storage/shared"
	"github.com/guileen/pglitedb/types"
)

// simpleMockKV is a simple mock implementation of the storage.KV interface for testing
type simpleMockKV struct {
	getFunc             func(ctx context.Context, key []byte) ([]byte, error)
	setFunc             func(ctx context.Context, key, value []byte) error
	deleteFunc          func(ctx context.Context, key []byte) error
	newIteratorFunc     func(opts *shared.IteratorOptions) storage.Iterator
	newBatchFunc        func() storage.Batch
	commitFunc          func(ctx context.Context, batch storage.Batch) error
	newSnapshotFunc     func() (storage.Snapshot, error)
	newTransactionFunc  func(ctx context.Context) (storage.Transaction, error)
	statsFunc           func() shared.KVStats
	flushFunc           func() error
	closeFunc           func() error
	checkForConflictsFunc func(txn storage.Transaction, key []byte) error
}

func (m *simpleMockKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, key)
	}
	return nil, nil
}

func (m *simpleMockKV) Set(ctx context.Context, key, value []byte) error {
	if m.setFunc != nil {
		return m.setFunc(ctx, key, value)
	}
	return nil
}

func (m *simpleMockKV) SetWithOptions(ctx context.Context, key, value []byte, opts *shared.WriteOptions) error {
	return m.Set(ctx, key, value)
}

func (m *simpleMockKV) Delete(ctx context.Context, key []byte) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, key)
	}
	return nil
}

func (m *simpleMockKV) DeleteWithOptions(ctx context.Context, key []byte, opts *shared.WriteOptions) error {
	return m.Delete(ctx, key)
}

func (m *simpleMockKV) NewBatch() storage.Batch {
	if m.newBatchFunc != nil {
		return m.newBatchFunc()
	}
	return nil
}

func (m *simpleMockKV) Commit(ctx context.Context, batch storage.Batch) error {
	if m.commitFunc != nil {
		return m.commitFunc(ctx, batch)
	}
	return nil
}

func (m *simpleMockKV) CommitBatch(ctx context.Context, batch storage.Batch) error {
	return m.Commit(ctx, batch)
}

func (m *simpleMockKV) CommitBatchWithOptions(ctx context.Context, batch storage.Batch, opts *shared.WriteOptions) error {
	return m.Commit(ctx, batch)
}

func (m *simpleMockKV) NewIterator(opts *shared.IteratorOptions) storage.Iterator {
	if m.newIteratorFunc != nil {
		return m.newIteratorFunc(opts)
	}
	return nil
}

func (m *simpleMockKV) NewSnapshot() (storage.Snapshot, error) {
	if m.newSnapshotFunc != nil {
		return m.newSnapshotFunc()
	}
	return nil, nil
}

func (m *simpleMockKV) NewTransaction(ctx context.Context) (storage.Transaction, error) {
	if m.newTransactionFunc != nil {
		return m.newTransactionFunc(ctx)
	}
	return nil, nil
}

func (m *simpleMockKV) Stats() shared.KVStats {
	if m.statsFunc != nil {
		return m.statsFunc()
	}
	return shared.KVStats{}
}

func (m *simpleMockKV) Flush() error {
	if m.flushFunc != nil {
		return m.flushFunc()
	}
	return nil
}

func (m *simpleMockKV) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *simpleMockKV) CheckForConflicts(txn storage.Transaction, key []byte) error {
	if m.checkForConflictsFunc != nil {
		return m.checkForConflictsFunc(txn, key)
	}
	return nil
}

func TestPersister_NewPersister(t *testing.T) {
	mockKV := &simpleMockKV{}
	persister := NewPersister(mockKV)
	assert.NotNil(t, persister)
}

func TestPersister_PersistSchema(t *testing.T) {
	called := false
	mockKV := &simpleMockKV{
		setFunc: func(ctx context.Context, key, value []byte) error {
			called = true
			return nil
		},
	}
	
	persister := NewPersister(mockKV)
	
	// Create a test table definition
	tableDef := &types.TableDefinition{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
	
	ctx := context.Background()
	tenantID := int64(1)
	tableName := "test_table"
	
	err := persister.PersistSchema(ctx, tenantID, tableName, tableDef)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestPersister_DeleteSchema(t *testing.T) {
	called := false
	mockKV := &simpleMockKV{
		deleteFunc: func(ctx context.Context, key []byte) error {
			called = true
			return nil
		},
	}
	
	persister := NewPersister(mockKV)
	
	ctx := context.Background()
	tenantID := int64(1)
	tableName := "test_table"
	
	err := persister.DeleteSchema(ctx, tenantID, tableName)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestPersister_PersistView(t *testing.T) {
	called := false
	mockKV := &simpleMockKV{
		setFunc: func(ctx context.Context, key, value []byte) error {
			called = true
			return nil
		},
	}
	
	persister := NewPersister(mockKV)
	
	// Create a test view definition
	viewDef := &types.ViewDefinition{
		Name:  "test_view",
		Query: "SELECT * FROM test_table",
	}
	
	ctx := context.Background()
	tenantID := int64(1)
	viewName := "test_view"
	
	err := persister.PersistView(ctx, tenantID, viewName, viewDef)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestPersister_DeleteView(t *testing.T) {
	called := false
	mockKV := &simpleMockKV{
		deleteFunc: func(ctx context.Context, key []byte) error {
			called = true
			return nil
		},
	}
	
	persister := NewPersister(mockKV)
	
	ctx := context.Background()
	tenantID := int64(1)
	viewName := "test_view"
	
	err := persister.DeleteView(ctx, tenantID, viewName)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestPersister_NilKV(t *testing.T) {
	persister := NewPersister(nil)
	
	ctx := context.Background()
	
	// All operations should return nil when KV is nil
	err := persister.PersistSchema(ctx, 1, "test", &types.TableDefinition{})
	assert.NoError(t, err)
	
	err = persister.DeleteSchema(ctx, 1, "test")
	assert.NoError(t, err)
	
	err = persister.PersistView(ctx, 1, "test", &types.ViewDefinition{})
	assert.NoError(t, err)
	
	err = persister.DeleteView(ctx, 1, "test")
	assert.NoError(t, err)
	
	err = persister.LoadSchemas(ctx, func(int64, string, *types.TableDefinition) error { return nil })
	assert.NoError(t, err)
}