package modify

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/engine/pebble/operations/testutils"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)



func TestUpdater_UpdateRow(t *testing.T) {
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(100)
	rowID := int64(1000)
	schemaDef := &types.TableDefinition{}
	updates := map[string]*types.Value{
		"name": {Type: types.ColumnTypeString, Data: "new_name"},
	}

	t.Run("successful update", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		// Add initial data to mockKV so getRow will succeed
		key := mockCodec.EncodeTableKey(tenantID, tableID, rowID)
		mockKV.Data[string(key)] = []byte("initial_data")

		updater := NewUpdater(mockKV, mockCodec)

		// Execute
		err := updater.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("update with data", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		// Add some initial data
		key := mockCodec.EncodeTableKey(tenantID, tableID, rowID)
		mockKV.Data[string(key)] = []byte("initial_data")

		updater := NewUpdater(mockKV, mockCodec)

		// Execute
		err := updater.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("update with existing record", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		// Add initial data to mockKV so getRow will succeed
		key := mockCodec.EncodeTableKey(tenantID, tableID, rowID)
		mockKV.Data[string(key)] = []byte("initial_data")

		updater := NewUpdater(mockKV, mockCodec)

		// Execute
		err := updater.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("update with data", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		// Add some initial data
		key := mockCodec.EncodeTableKey(tenantID, tableID, rowID)
		mockKV.Data[string(key)] = []byte("initial_data")

		updater := NewUpdater(mockKV, mockCodec)

		// Execute
		err := updater.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("update with existing record", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		// Add initial data to mockKV so getRow will succeed
		key := mockCodec.EncodeTableKey(tenantID, tableID, rowID)
		mockKV.Data[string(key)] = []byte("initial_data")

		updater := NewUpdater(mockKV, mockCodec)

		// Execute
		err := updater.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef)

		// Assert
		assert.NoError(t, err)
	})
}

// updaterMockKVOverride overrides the NewBatch method to return a specific batch
type updaterMockKVOverride struct {
	*testutils.MockKV
	batch storage.Batch
}

func (m *updaterMockKVOverride) NewBatch() storage.Batch {
	return m.batch
}

// mockKVOverrideWithError overrides methods to simulate errors
type mockKVOverrideWithError struct {
	*testutils.MockKV
	batch storage.Batch
}

func (m *mockKVOverrideWithError) NewBatch() storage.Batch {
	return m.batch
}

func (m *mockKVOverrideWithError) CommitBatch(ctx context.Context, batch storage.Batch) error {
	return assert.AnError // Simulate a commit error
}

func TestUpdater_UpdateRowBatch(t *testing.T) {
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(100)
	schemaDef := &types.TableDefinition{}
	updates := []RowUpdate{
		{
			RowID: 1000,
			Updates: map[string]*types.Value{
				"name": {Type: types.ColumnTypeString, Data: "new_name1"},
			},
		},
		{
			RowID: 1001,
			Updates: map[string]*types.Value{
				"name": {Type: types.ColumnTypeString, Data: "new_name2"},
			},
		},
	}

	t.Run("successful batch update", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Add initial data to mockKV so getRowBatch will succeed
		for _, update := range updates {
			key := mockCodec.EncodeTableKey(tenantID, tableID, update.RowID)
			mockKV.Data[string(key)] = []byte("initial_data")
		}

		// Override NewBatch to return our mockBatch
		mockKVOverride := &updaterMockKVOverride{MockKV: mockKV, batch: mockBatch}
		updater := NewUpdater(mockKVOverride, mockCodec)

		// Execute
		err := updater.UpdateRowBatch(ctx, tenantID, tableID, updates, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("empty updates", func(t *testing.T) {
		// Setup
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}
		updater := NewUpdater(mockKV, mockCodec)

		// Execute
		err := updater.UpdateRowBatch(ctx, tenantID, tableID, []RowUpdate{}, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("batch update", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Add initial data to mockKV so getRowBatch will succeed
		for _, update := range updates {
			key := mockCodec.EncodeTableKey(tenantID, tableID, update.RowID)
			mockKV.Data[string(key)] = []byte("initial_data")
		}

		// Override NewBatch to return our mockBatch
		mockKVOverride := &updaterMockKVOverride{MockKV: mockKV, batch: mockBatch}
		updater := NewUpdater(mockKVOverride, mockCodec)

		// Execute
		err := updater.UpdateRowBatch(ctx, tenantID, tableID, updates, schemaDef)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("commit batch error", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Override NewBatch to return our mockBatch and CommitBatch to return an error
		mockKVOverride := &mockKVOverrideWithError{MockKV: mockKV, batch: mockBatch}
		updater := NewUpdater(mockKVOverride, mockCodec)

		// Execute
		err := updater.UpdateRowBatch(ctx, tenantID, tableID, updates, schemaDef)

		// Assert
		assert.Error(t, err) // Expect an error due to our override
	})
}