package query

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/engine/pebble/operations/testutils"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/storage/shared"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)



// deleteMockKVOverride overrides the NewBatch method to return a specific batch
type deleteMockKVOverride struct {
	*testutils.MockKV
	batch storage.Batch
}

func (m *deleteMockKVOverride) NewBatch() storage.Batch {
	return m.batch
}

func TestDeleteOperations_DeleteRow(t *testing.T) {
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(100)
	rowID := int64(1000)
	schemaDef := &types.TableDefinition{}

	t.Run("successful delete", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockBatch := &testutils.MockBatch{Data: make(map[string][]byte), Deleted: make(map[string]bool)}
		mockCodec := &testutils.MockCodec{}

		// Override NewBatch to return our mockBatch
		mockKVOverride := &deleteMockKVOverride{MockKV: mockKV, batch: mockBatch}
		deleteOps := NewDeleteOperations(mockKVOverride, mockCodec)

		// Mock getRowFunc
		getRowFunc := func(ctx context.Context, tid, tblID, rID int64, schema *types.TableDefinition) (*types.Record, error) {
			return &types.Record{}, nil
		}

		// Mock deleteIndexesInBatchFunc
		deleteIndexesInBatchFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchFunc
		commitBatchFunc := func(ctx context.Context, batch storage.Batch) error {
			return nil
		}

		// Execute
		err := deleteOps.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef, getRowFunc, deleteIndexesInBatchFunc, commitBatchFunc)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("row not found", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		deleteOps := NewDeleteOperations(mockKV, mockCodec)

		// Mock getRowFunc that returns record not found
		getRowFunc := func(ctx context.Context, tid, tblID, rID int64, schema *types.TableDefinition) (*types.Record, error) {
			return nil, types.ErrRecordNotFound
		}

		// Mock deleteIndexesInBatchFunc
		deleteIndexesInBatchFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchFunc
		commitBatchFunc := func(ctx context.Context, batch storage.Batch) error {
			return nil
		}

		// Execute
		err := deleteOps.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef, getRowFunc, deleteIndexesInBatchFunc, commitBatchFunc)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("get row error", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		deleteOps := NewDeleteOperations(mockKV, mockCodec)

		// Mock getRowFunc that returns an error
		getRowFunc := func(ctx context.Context, tid, tblID, rID int64, schema *types.TableDefinition) (*types.Record, error) {
			return nil, assert.AnError
		}

		// Mock deleteIndexesInBatchFunc
		deleteIndexesInBatchFunc := func(batch storage.Batch, tid, tblID, rID int64, record *types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchFunc
		commitBatchFunc := func(ctx context.Context, batch storage.Batch) error {
			return nil
		}

		// Execute
		err := deleteOps.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef, getRowFunc, deleteIndexesInBatchFunc, commitBatchFunc)

		// Assert
		assert.Error(t, err)
	})
}

func TestDeleteOperations_DeleteRowBatch(t *testing.T) {
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
		mockKVOverride := &deleteMockKVOverride{MockKV: mockKV, batch: mockBatch}
		deleteOps := NewDeleteOperations(mockKVOverride, mockCodec)

		// Mock getRowBatchFunc
		getRowBatchFunc := func(ctx context.Context, tid, tblID int64, rIDs []int64, schema *types.TableDefinition) (map[int64]*types.Record, error) {
			result := make(map[int64]*types.Record)
			for _, id := range rIDs {
				result[id] = &types.Record{}
			}
			return result, nil
		}

		// Mock deleteIndexesBulkFunc
		deleteIndexesBulkFunc := func(batch storage.Batch, tid, tblID int64, records map[int64]*types.Record, schema *types.TableDefinition) error {
			return nil
		}

		// Mock commitBatchWithOptionsFunc
		commitBatchWithOptionsFunc := func(ctx context.Context, batch storage.Batch, opts *shared.WriteOptions) error {
			return nil
		}

		// Execute
		err := deleteOps.DeleteRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef, getRowBatchFunc, deleteIndexesBulkFunc, commitBatchWithOptionsFunc)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("empty row ids", func(t *testing.T) {
		// Setup
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}
		deleteOps := NewDeleteOperations(mockKV, mockCodec)

		// Execute
		err := deleteOps.DeleteRowBatch(ctx, tenantID, tableID, []int64{}, schemaDef, nil, nil, nil)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("get row batch error", func(t *testing.T) {
		// Setup mocks
		mockKV := testutils.NewMockKV()
		mockCodec := &testutils.MockCodec{}

		deleteOps := NewDeleteOperations(mockKV, mockCodec)

		// Mock getRowBatchFunc that returns an error
		getRowBatchFunc := func(ctx context.Context, tid, tblID int64, rIDs []int64, schema *types.TableDefinition) (map[int64]*types.Record, error) {
			return nil, assert.AnError
		}

		// Execute
		err := deleteOps.DeleteRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef, getRowBatchFunc, nil, nil)

		// Assert
		assert.Error(t, err)
	})
}