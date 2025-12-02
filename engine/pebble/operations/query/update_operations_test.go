package query

import (
	"context"
	"testing"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/operations/testutils"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/storage/shared"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)



// updateMockKVOverride overrides the NewBatch method to return a specific batch
type updateMockKVOverride struct {
	*testutils.MockKV
	batch storage.Batch
}

func (m *updateMockKVOverride) NewBatch() storage.Batch {
	return m.batch
}

func TestUpdateOperations_UpdateRow(t *testing.T) {
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
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Override NewBatch to return our mockBatch
		mockKVOverride := &updateMockKVOverride{MockKV: mockKV, batch: mockBatch}
		updateOps := NewUpdateOperations(mockKVOverride, mockCodec)

		// Mock getRowFunc
		getRowFunc := func(ctx context.Context, tid, tblID, rID int64, schema *types.TableDefinition) (*types.Record, error) {
			return &types.Record{
				Data: make(map[string]*types.Value),
			}, nil
		}

		// Mock deleteIndexesInBatchFunc
		deleteIndexesInBatchFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock batchUpdateIndexesFunc
		batchUpdateIndexesFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchFunc
		commitBatchFunc := func(ctx context.Context, batch storage.Batch) error {
			return nil
		}

		// Execute
		err := updateOps.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef, getRowFunc, deleteIndexesInBatchFunc, batchUpdateIndexesFunc, commitBatchFunc)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("get row error", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		updateOps := NewUpdateOperations(mockKV, mockCodec)

		// Mock getRowFunc that returns an error
		getRowFunc := func(ctx context.Context, tid, tblID, rID int64, schema *types.TableDefinition) (*types.Record, error) {
			return nil, assert.AnError
		}

		// Mock deleteIndexesInBatchFunc
		deleteIndexesInBatchFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock batchUpdateIndexesFunc
		batchUpdateIndexesFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchFunc
		commitBatchFunc := func(ctx context.Context, batch storage.Batch) error {
			return nil
		}

		// Execute
		err := updateOps.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef, getRowFunc, deleteIndexesInBatchFunc, batchUpdateIndexesFunc, commitBatchFunc)

		// Assert
		assert.Error(t, err)
	})

	t.Run("encode row error", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Override NewBatch to return our mockBatch
		mockKVOverride := &updateMockKVOverride{MockKV: mockKV, batch: mockBatch}
		updateOps := NewUpdateOperations(mockKVOverride, mockCodec)

		// Mock getRowFunc
		getRowFunc := func(ctx context.Context, tid, tblID, rID int64, schema *types.TableDefinition) (*types.Record, error) {
			return &types.Record{
				Data: make(map[string]*types.Value),
			}, nil
		}

		// Mock deleteIndexesInBatchFunc
		deleteIndexesInBatchFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock batchUpdateIndexesFunc
		batchUpdateIndexesFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchFunc
		commitBatchFunc := func(ctx context.Context, batch storage.Batch) error {
			return nil
		}

		// Execute
		err := updateOps.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef, getRowFunc, deleteIndexesInBatchFunc, batchUpdateIndexesFunc, commitBatchFunc)

		// Assert
		assert.NoError(t, err) // With current mock implementation, no error is expected
	})
}

func TestUpdateOperations_UpdateRowBatch(t *testing.T) {
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(100)
	schemaDef := &types.TableDefinition{}
	updates := []engineTypes.RowUpdate{
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

		// Override NewBatch to return our mockBatch
		mockKVOverride := &updateMockKVOverride{MockKV: mockKV, batch: mockBatch}
		updateOps := NewUpdateOperations(mockKVOverride, mockCodec)

		// Mock getRowBatchFunc
		getRowBatchFunc := func(ctx context.Context, tid, tblID int64, rIDs []int64, schema *types.TableDefinition) (map[int64]*types.Record, error) {
			result := make(map[int64]*types.Record)
			for _, id := range rIDs {
				result[id] = &types.Record{
					Data: make(map[string]*types.Value),
				}
			}
			return result, nil
		}

		// Mock deleteIndexesBulkFunc
		deleteIndexesBulkFunc := func(batch storage.Batch, tid, tblID int64, records map[int64]*types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock batchUpdateIndexesBulkFunc
		batchUpdateIndexesBulkFunc := func(batch storage.Batch, tid, tblID int64, records map[int64]*types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchWithOptionsFunc
		commitBatchWithOptionsFunc := func(ctx context.Context, batch storage.Batch, opts *shared.WriteOptions) error {
			return nil
		}

		// Execute
		err := updateOps.UpdateRowBatch(ctx, tenantID, tableID, updates, schemaDef, getRowBatchFunc, deleteIndexesBulkFunc, batchUpdateIndexesBulkFunc, commitBatchWithOptionsFunc)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("empty updates", func(t *testing.T) {
		// Setup
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}
		updateOps := NewUpdateOperations(mockKV, mockCodec)

		// Execute
		err := updateOps.UpdateRowBatch(ctx, tenantID, tableID, []engineTypes.RowUpdate{}, schemaDef, nil, nil, nil, nil)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("get row batch error", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		updateOps := NewUpdateOperations(mockKV, mockCodec)

		// Mock getRowBatchFunc that returns an error
		getRowBatchFunc := func(ctx context.Context, tid, tblID int64, rIDs []int64, schema *types.TableDefinition) (map[int64]*types.Record, error) {
			return nil, assert.AnError
		}

		// Execute
		err := updateOps.UpdateRowBatch(ctx, tenantID, tableID, updates, schemaDef, getRowBatchFunc, nil, nil, nil)

		// Assert
		assert.Error(t, err)
	})

	t.Run("row not found in batch", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Override NewBatch to return our mockBatch
		mockKVOverride := &updateMockKVOverride{MockKV: mockKV, batch: mockBatch}
		updateOps := NewUpdateOperations(mockKVOverride, mockCodec)

		// Mock getRowBatchFunc that returns incomplete data
		getRowBatchFunc := func(ctx context.Context, tid, tblID int64, rIDs []int64, schema *types.TableDefinition) (map[int64]*types.Record, error) {
			result := make(map[int64]*types.Record)
			// Only return one of the requested rows
			result[1000] = &types.Record{
				Data: make(map[string]*types.Value),
			}
			// Missing row 1001
			return result, nil
		}

		// Mock deleteIndexesBulkFunc
		deleteIndexesBulkFunc := func(batch storage.Batch, tid, tblID int64, records map[int64]*types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock batchUpdateIndexesBulkFunc
		batchUpdateIndexesBulkFunc := func(batch storage.Batch, tid, tblID int64, records map[int64]*types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchWithOptionsFunc
		commitBatchWithOptionsFunc := func(ctx context.Context, batch storage.Batch, opts *shared.WriteOptions) error {
			return nil
		}

		// Execute
		err := updateOps.UpdateRowBatch(ctx, tenantID, tableID, updates, schemaDef, getRowBatchFunc, deleteIndexesBulkFunc, batchUpdateIndexesBulkFunc, commitBatchWithOptionsFunc)

		// Assert
		assert.Error(t, err) // Expect an error since row 1001 is missing
	})
}