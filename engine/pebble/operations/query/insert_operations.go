package query

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// InsertOperations provides insert operations for the pebble engine
type InsertOperations struct {
	kv    storage.KV
	codec Codec
}

// NewInsertOperations creates a new InsertOperations instance
func NewInsertOperations(kv storage.KV, codec Codec) *InsertOperations {
	return &InsertOperations{
		kv:    kv,
		codec: codec,
	}
}

// InsertRow inserts a single row
func (i *InsertOperations) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition, nextRowIDFunc func(context.Context, int64, int64) (int64, error), updateIndexesFunc func(context.Context, int64, int64, int64, *dbTypes.Record, *dbTypes.TableDefinition, bool) error) (int64, error) {
	rowID, err := nextRowIDFunc(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}
	key := i.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := i.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}
	if err := i.kv.Set(ctx, key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}
	if err := updateIndexesFunc(ctx, tenantID, tableID, rowID, row, schemaDef, true); err != nil {
		return 0, fmt.Errorf("update indexes: %w", err)
	}
	return rowID, nil
}

// InsertRowBatch inserts multiple rows in a batch
func (i *InsertOperations) InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*dbTypes.Record, schemaDef *dbTypes.TableDefinition, nextRowIDFunc func(context.Context, int64, int64) (int64, error), batchUpdateIndexesFunc func(storage.Batch, int64, int64, int64, *dbTypes.Record, *dbTypes.TableDefinition) error, commitFunc func(context.Context, storage.Batch) error) ([]int64, error) {
	if len(rows) == 0 {
		return []int64{}, nil
	}

	// Pre-allocate rowIDs slice with exact capacity
	rowIDs := make([]int64, len(rows))

	// Generate all row IDs first to minimize lock contention
	for idx := range rows {
		rowID, err := nextRowIDFunc(ctx, tenantID, tableID)
		if err != nil {
			return nil, fmt.Errorf("generate row id: %w", err)
		}
		rowIDs[idx] = rowID
	}

	batch := i.kv.NewBatch()
	defer batch.Close()

	// Process rows in batch with reduced allocations
	for idx, row := range rows {
		key := i.codec.EncodeTableKey(tenantID, tableID, rowIDs[idx])
		value, err := i.codec.EncodeRow(row, schemaDef)
		if err != nil {
			return nil, fmt.Errorf("encode row %d: %w", idx, err)
		}

		if err := batch.Set(key, value); err != nil {
			return nil, fmt.Errorf("batch set row %d: %w", idx, err)
		}

		if err := batchUpdateIndexesFunc(batch, tenantID, tableID, rowIDs[idx], row, schemaDef); err != nil {
			return nil, fmt.Errorf("batch update indexes for row %d: %w", idx, err)
		}
	}

	if err := commitFunc(ctx, batch); err != nil {
		return nil, fmt.Errorf("commit batch: %w", err)
	}

	return rowIDs, nil
}