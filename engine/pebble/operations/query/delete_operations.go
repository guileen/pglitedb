package query

import (
	"context"
	"fmt"
	"sort"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/storage/shared"
	dbTypes "github.com/guileen/pglitedb/types"
)

// DeleteOperations provides delete operations for the pebble engine
type DeleteOperations struct {
	kv    storage.KV
	codec Codec
}

// NewDeleteOperations creates a new DeleteOperations instance
func NewDeleteOperations(kv storage.KV, codec Codec) *DeleteOperations {
	return &DeleteOperations{
		kv:    kv,
		codec: codec,
	}
}

// DeleteRow deletes a single row
func (d *DeleteOperations) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition, getRowFunc func(context.Context, int64, int64, int64, *dbTypes.TableDefinition) (*dbTypes.Record, error), deleteIndexesInBatchFunc func(storage.Batch, int64, int64, int64, *dbTypes.Record, *dbTypes.TableDefinition) error, commitBatchFunc func(context.Context, storage.Batch) error) error {
	oldRow, err := getRowFunc(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get row: %w", err)
	}

	batch := d.kv.NewBatch()
	defer batch.Close()

	key := d.codec.EncodeTableKey(tenantID, tableID, rowID)
	if err := batch.Delete(key); err != nil {
		return fmt.Errorf("batch delete row: %w", err)
	}

	if err := deleteIndexesInBatchFunc(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("delete indexes in batch: %w", err)
	}

	if err := commitBatchFunc(ctx, batch); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// DeleteRowBatch deletes multiple rows in a batch
func (d *DeleteOperations) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition, getRowBatchFunc func(context.Context, int64, int64, []int64, *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error), deleteIndexesBulkFunc func(storage.Batch, int64, int64, map[int64]*dbTypes.Record, *dbTypes.TableDefinition) error, commitBatchWithOptionsFunc func(context.Context, storage.Batch, *shared.WriteOptions) error) error {
	if len(rowIDs) == 0 {
		return nil
	}

	oldRows, err := getRowBatchFunc(ctx, tenantID, tableID, rowIDs, schemaDef)
	if err != nil {
		return fmt.Errorf("get old rows: %w", err)
	}

	batch := d.kv.NewBatch()
	defer batch.Close()

	if err := deleteIndexesBulkFunc(batch, tenantID, tableID, oldRows, schemaDef); err != nil {
		return fmt.Errorf("delete indexes: %w", err)
	}

	sort.Slice(rowIDs, func(i, j int) bool { return rowIDs[i] < rowIDs[j] })

	for _, rowID := range rowIDs {
		if _, ok := oldRows[rowID]; !ok {
			continue
		}

		key := d.codec.EncodeTableKey(tenantID, tableID, rowID)
		if err := batch.Delete(key); err != nil {
			return fmt.Errorf("batch delete row %d: %w", rowID, err)
		}
	}

	if err := commitBatchWithOptionsFunc(ctx, batch, &shared.WriteOptions{
		Durability: shared.DurabilityEventual,
	}); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}