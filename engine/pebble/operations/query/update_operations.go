package query

import (
	"context"
	"fmt"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/storage/shared"
	dbTypes "github.com/guileen/pglitedb/types"
)

// UpdateOperations provides update operations for the pebble engine
type UpdateOperations struct {
	kv    storage.KV
	codec Codec
}

// NewUpdateOperations creates a new UpdateOperations instance
func NewUpdateOperations(kv storage.KV, codec Codec) *UpdateOperations {
	return &UpdateOperations{
		kv:    kv,
		codec: codec,
	}
}

// UpdateRow updates a single row
func (u *UpdateOperations) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition, getRowFunc func(context.Context, int64, int64, int64, *dbTypes.TableDefinition) (*dbTypes.Record, error), deleteIndexesInBatchFunc func(storage.Batch, int64, int64, int64, *dbTypes.Record, *dbTypes.TableDefinition) error, batchUpdateIndexesFunc func(storage.Batch, int64, int64, int64, *dbTypes.Record, *dbTypes.TableDefinition) error, commitBatchFunc func(context.Context, storage.Batch) error) error {
	oldRow, err := getRowFunc(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	batch := u.kv.NewBatch()
	defer batch.Close()

	if err := deleteIndexesInBatchFunc(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("delete old indexes in batch: %w", err)
	}

	// Apply updates efficiently by reusing the existing record
	for k, v := range updates {
		oldRow.Data[k] = v
	}
	// Update timestamps
	oldRow.UpdatedAt = time.Now()

	key := u.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := u.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := batch.Set(key, value); err != nil {
		return fmt.Errorf("batch set row: %w", err)
	}

	if err := batchUpdateIndexesFunc(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("update indexes in batch: %w", err)
	}

	if err := commitBatchFunc(ctx, batch); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// UpdateRowBatch updates multiple rows in a batch
func (u *UpdateOperations) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition, getRowBatchFunc func(context.Context, int64, int64, []int64, *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error), deleteIndexesBulkFunc func(storage.Batch, int64, int64, map[int64]*dbTypes.Record, *dbTypes.TableDefinition) error, batchUpdateIndexesBulkFunc func(storage.Batch, int64, int64, map[int64]*dbTypes.Record, *dbTypes.TableDefinition) error, commitBatchWithOptionsFunc func(context.Context, storage.Batch, *shared.WriteOptions) error) error {
	if len(updates) == 0 {
		return nil
	}

	rowIDs := make([]int64, len(updates))
	for i, update := range updates {
		rowIDs[i] = update.RowID
	}

	oldRows, err := getRowBatchFunc(ctx, tenantID, tableID, rowIDs, schemaDef)
	if err != nil {
		return fmt.Errorf("get old rows: %w", err)
	}

	batch := u.kv.NewBatch()
	defer batch.Close()

	if err := deleteIndexesBulkFunc(batch, tenantID, tableID, oldRows, schemaDef); err != nil {
		return fmt.Errorf("delete old indexes: %w", err)
	}

	updatedRows := make(map[int64]*dbTypes.Record, len(updates))
	for _, update := range updates {
		oldRow, ok := oldRows[update.RowID]
		if !ok {
			return fmt.Errorf("row %d not found", update.RowID)
		}

		for colName, newValue := range update.Updates {
			oldRow.Data[colName] = newValue
		}
		updatedRows[update.RowID] = oldRow

		value, err := u.codec.EncodeRow(oldRow, schemaDef)
		if err != nil {
			return fmt.Errorf("encode row %d: %w", update.RowID, err)
		}

		key := u.codec.EncodeTableKey(tenantID, tableID, update.RowID)
		if err := batch.Set(key, value); err != nil {
			return fmt.Errorf("batch set row %d: %w", update.RowID, err)
		}
	}

	if err := batchUpdateIndexesBulkFunc(batch, tenantID, tableID, updatedRows, schemaDef); err != nil {
		return fmt.Errorf("update indexes: %w", err)
	}

	if err := commitBatchWithOptionsFunc(ctx, batch, &shared.WriteOptions{
		Durability: shared.DurabilityEventual,
	}); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}