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

	if err := e.deleteIndexesBulk(batch, tenantID, tableID, oldRows, schemaDef); err != nil {
		return fmt.Errorf("delete old indexes: %w", err)
	}

	updatedRows := make(map[int64]*types.Record, len(updates))
	for _, update := range updates {
		oldRow, ok := oldRows[update.RowID]
		if !ok {
			return fmt.Errorf("row %d not found", update.RowID)
		}

		for colName, newValue := range update.Updates {
			oldRow.Data[colName] = newValue
		}
		updatedRows[update.RowID] = oldRow

		value, err := e.codec.EncodeRow(oldRow, schemaDef)
		if err != nil {
			return fmt.Errorf("encode row %d: %w", update.RowID, err)
		}

		key := e.codec.EncodeTableKey(tenantID, tableID, update.RowID)
		if err := batch.Set(key, value); err != nil {
			return fmt.Errorf("batch set row %d: %w", update.RowID, err)
		}
	}

	if err := e.batchUpdateIndexesBulk(batch, tenantID, tableID, updatedRows, schemaDef); err != nil {
		return fmt.Errorf("update indexes: %w", err)
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

	if err := e.deleteIndexesBulk(batch, tenantID, tableID, oldRows, schemaDef); err != nil {
		return fmt.Errorf("delete indexes: %w", err)
	}

	sort.Slice(rowIDs, func(i, j int) bool { return rowIDs[i] < rowIDs[j] })

	for _, rowID := range rowIDs {
		if _, ok := oldRows[rowID]; !ok {
			continue
		}

		key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
		if err := batch.Delete(key); err != nil {
			return fmt.Errorf("batch delete row %d: %w", rowID, err)
		}
	}

	if err := e.kv.CommitBatchWithOptions(ctx, batch, &shared.WriteOptions{
		Durability: shared.DurabilityEventual,
	}); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}
