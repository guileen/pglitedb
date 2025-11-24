package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/guileen/pglitedb/storage/shared"
	"github.com/guileen/pglitedb/types"
)

func (e *pebbleEngine) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []RowUpdate, schemaDef *types.TableDefinition) error {
	if len(updates) == 0 {
		return nil
	}

	rowIDs := make([]int64, len(updates))
	for i, update := range updates {
		rowIDs[i] = update.RowID
	}

	oldRows, err := e.GetRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)
	if err != nil {
		return fmt.Errorf("get old rows: %w", err)
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	for _, update := range updates {
		oldRow, ok := oldRows[update.RowID]
		if !ok {
			return fmt.Errorf("row %d not found", update.RowID)
		}

		for colName, newValue := range update.Updates {
			oldRow.Data[colName] = newValue
		}

		if err := e.deleteIndexesInBatch(batch, tenantID, tableID, update.RowID, oldRows[update.RowID], schemaDef); err != nil {
			return fmt.Errorf("delete old indexes for row %d: %w", update.RowID, err)
		}

		value, err := e.codec.EncodeRow(oldRow, schemaDef)
		if err != nil {
			return fmt.Errorf("encode row %d: %w", update.RowID, err)
		}

		key := e.codec.EncodeTableKey(tenantID, tableID, update.RowID)
		if err := batch.Set(key, value); err != nil {
			return fmt.Errorf("batch set row %d: %w", update.RowID, err)
		}

		if err := e.batchUpdateIndexes(batch, tenantID, tableID, update.RowID, oldRow, schemaDef); err != nil {
			return fmt.Errorf("update indexes for row %d: %w", update.RowID, err)
		}
	}

	if err := e.kv.CommitBatchWithOptions(ctx, batch, &shared.WriteOptions{
		Durability: shared.DurabilityEventual,
	}); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

func (e *pebbleEngine) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error {
	if len(rowIDs) == 0 {
		return nil
	}

	oldRows, err := e.GetRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)
	if err != nil {
		return fmt.Errorf("get old rows: %w", err)
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	sort.Slice(rowIDs, func(i, j int) bool { return rowIDs[i] < rowIDs[j] })

	for _, rowID := range rowIDs {
		oldRow, ok := oldRows[rowID]
		if !ok {
			continue
		}

		key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
		if err := batch.Delete(key); err != nil {
			return fmt.Errorf("batch delete row %d: %w", rowID, err)
		}

		if err := e.deleteIndexesInBatch(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
			return fmt.Errorf("delete indexes for row %d: %w", rowID, err)
		}
	}

	if err := e.kv.CommitBatchWithOptions(ctx, batch, &shared.WriteOptions{
		Durability: shared.DurabilityEventual,
	}); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}
