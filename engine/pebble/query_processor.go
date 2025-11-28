package pebble

import (
	"context"
	"fmt"
	"sort"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/storage/shared"
	dbTypes "github.com/guileen/pglitedb/types"
)

// GetRow retrieves a single row by its ID
func (e *pebbleEngine) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := e.kv.Get(ctx, key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, dbTypes.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := e.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

// InsertRow inserts a single row
func (e *pebbleEngine) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	rowID, err := e.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}
	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := e.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}
	if err := e.kv.Set(ctx, key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}
	if err := e.updateIndexes(ctx, tenantID, tableID, rowID, row, schemaDef, true); err != nil {
		return 0, fmt.Errorf("update indexes: %w", err)
	}
	return rowID, nil
}

// UpdateRow updates a single row
func (e *pebbleEngine) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	oldRow, err := e.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	if err := e.deleteIndexesInBatch(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("delete old indexes in batch: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := e.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := batch.Set(key, value); err != nil {
		return fmt.Errorf("batch set row: %w", err)
	}

	if err := e.batchUpdateIndexes(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("update indexes in batch: %w", err)
	}

	if err := e.kv.CommitBatch(ctx, batch); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// DeleteRow deletes a single row
func (e *pebbleEngine) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	oldRow, err := e.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get row: %w", err)
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	if err := batch.Delete(key); err != nil {
		return fmt.Errorf("batch delete row: %w", err)
	}

	if err := e.deleteIndexesInBatch(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("delete indexes in batch: %w", err)
	}

	if err := e.kv.CommitBatch(ctx, batch); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// GetRowBatch retrieves multiple rows by their IDs
func (e *pebbleEngine) GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error) {
	if len(rowIDs) == 0 {
		return make(map[int64]*dbTypes.Record), nil
	}

	result := make(map[int64]*dbTypes.Record, len(rowIDs))
	
	sorted := make([]int64, len(rowIDs))
	copy(sorted, rowIDs)
	sortInt64Slice(sorted)
	
	startKey := e.codec.EncodeTableKey(tenantID, tableID, sorted[0])
	endKey := e.codec.EncodeTableKey(tenantID, tableID, sorted[len(sorted)-1]+1)
	
	iter := e.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer iter.Close()
	
	targetIdx := 0
	for iter.First(); iter.Valid() && targetIdx < len(sorted); iter.Next() {
		_, _, rowID, err := e.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return nil, fmt.Errorf("decode table key: %w", err)
		}
		
		for targetIdx < len(sorted) && sorted[targetIdx] < rowID {
			targetIdx++
		}
		
		if targetIdx < len(sorted) && sorted[targetIdx] == rowID {
			record, err := e.codec.DecodeRow(iter.Value(), schemaDef)
			if err != nil {
				return nil, fmt.Errorf("decode row %d: %w", rowID, err)
			}
			result[rowID] = record
			targetIdx++
		}
	}

	return result, nil
}

// InsertRowBatch inserts multiple rows in a batch
func (e *pebbleEngine) InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]int64, error) {
	if len(rows) == 0 {
		return []int64{}, nil
	}

	// Pre-allocate rowIDs slice with exact capacity
	rowIDs := make([]int64, len(rows))
	
	// Generate all row IDs first to minimize lock contention
	for i := range rows {
		rowID, err := e.NextRowID(ctx, tenantID, tableID)
		if err != nil {
			return nil, fmt.Errorf("generate row id: %w", err)
		}
		rowIDs[i] = rowID
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	// Process rows in batch with reduced allocations
	for i, row := range rows {
		key := e.codec.EncodeTableKey(tenantID, tableID, rowIDs[i])
		value, err := e.codec.EncodeRow(row, schemaDef)
		if err != nil {
			return nil, fmt.Errorf("encode row %d: %w", i, err)
		}

		if err := batch.Set(key, value); err != nil {
			return nil, fmt.Errorf("batch set row %d: %w", i, err)
		}

		if err := e.batchUpdateIndexes(batch, tenantID, tableID, rowIDs[i], row, schemaDef); err != nil {
			return nil, fmt.Errorf("batch update indexes for row %d: %w", i, err)
		}
	}

	if err := e.kv.Commit(ctx, batch); err != nil {
		return nil, fmt.Errorf("commit batch: %w", err)
	}

	return rowIDs, nil
}

// UpdateRowBatch updates multiple rows in a batch
func (e *pebbleEngine) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
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

// DeleteRowBatch deletes multiple rows in a batch
func (e *pebbleEngine) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
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

// sortInt64Slice sorts a slice of int64 values
func sortInt64Slice(arr []int64) {
	// Use efficient sorting algorithm instead of bubble sort
	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
}