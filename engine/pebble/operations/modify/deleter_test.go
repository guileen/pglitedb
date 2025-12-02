package modify

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/engine/pebble/operations/testutils"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

// deleterMockKVOverride overrides the NewBatch method to return a specific batch
type deleterMockKVOverride struct {
	*testutils.MockKV
	batch storage.Batch
}

func (m *deleterMockKVOverride) NewBatch() storage.Batch {
	return m.batch
}

// deleterMockKVOverrideWithError overrides methods to simulate errors
type deleterMockKVOverrideWithError struct {
	*testutils.MockKV
	batch storage.Batch
}

func (m *deleterMockKVOverrideWithError) NewBatch() storage.Batch {
	return m.batch
}

func (m *deleterMockKVOverrideWithError) CommitBatch(ctx context.Context, batch storage.Batch) error {
	return assert.AnError // Simulate a commit error
}



func TestDeleter_DeleteRow(t *testing.T) {
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(100)
	rowID := int64(1000)
	schemaDef := &types.TableDefinition{}

	t.Run("successful delete", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		deleter := NewDeleter(mockKV, mockCodec)

		// Execute
		err := deleter.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("delete with data", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}
		
		// Add some data to delete
		key := []byte("test_key")
		mockKV.Data[string(key)] = []byte("test_value")

		deleter := NewDeleter(mockKV, mockCodec)

		// Execute
		err := deleter.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef)

		// Assert
		assert.NoError(t, err)
	})
}

func TestDeleter_DeleteRowBatch(t *testing.T) {
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(100)
	rowIDs := []int64{1000, 1001, 1002}
	schemaDef := &types.TableDefinition{}

	t.Run("successful batch delete", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Override NewBatch to return our mockBatch
		mockKVOverride := &deleterMockKVOverride{MockKV: mockKV, batch: mockBatch}
		deleter := NewDeleter(mockKVOverride, mockCodec)

		// Execute
		err := deleter.DeleteRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("empty row ids", func(t *testing.T) {
		// Setup
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}
		deleter := NewDeleter(mockKV, mockCodec)

		// Execute
		err := deleter.DeleteRowBatch(ctx, tenantID, tableID, []int64{}, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("batch delete", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Override NewBatch to return our mockBatch
		mockKVOverride := &deleterMockKVOverride{MockKV: mockKV, batch: mockBatch}
		deleter := NewDeleter(mockKVOverride, mockCodec)

		// Execute
		err := deleter.DeleteRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("commit batch error", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Override NewBatch to return our mockBatch and CommitBatch to return an error
		mockKVOverride := &deleterMockKVOverrideWithError{MockKV: mockKV, batch: mockBatch}
		deleter := NewDeleter(mockKVOverride, mockCodec)

		// Execute
		err := deleter.DeleteRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)

		// Assert
		assert.Error(t, err) // Expect an error due to our override
	})
}