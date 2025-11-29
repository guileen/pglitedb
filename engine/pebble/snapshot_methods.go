package pebble

import (
	"context"
	"fmt"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/utils"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// GetRow retrieves a row by its ID
func (tx *snapshotTransaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	if tx.closed {
		return nil, storage.ErrClosed
	}

	pe := tx.engine

	key := pe.codec.EncodeTableKey(tenantID, tableID, rowID)

	if val, ok := tx.mutations[string(key)]; ok {
		if val == nil {
			// Row is marked for deletion
			return nil, storage.ErrNotFound
		}
		return pe.codec.DecodeRow(val, schemaDef)
	}

	val, err := tx.snapshot.Get(key)
	if err != nil {
		return nil, err
	}

	// Check if the value is nil (marked for deletion)
	if val == nil {
		return nil, storage.ErrNotFound
	}

	return pe.codec.DecodeRow(val, schemaDef)
}

// InsertRow inserts a new row
func (tx *snapshotTransaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	pe := tx.engine
	
	rowID, err := pe.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, err
	}

	key := pe.codec.EncodeTableKey(tenantID, tableID, rowID)
	val, err := pe.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, err
	}

	tx.mutations[string(key)] = val

	if schemaDef.Indexes != nil {
		for i, indexDef := range schemaDef.Indexes {
			indexID := int64(i + 1)
			
			indexValues := make([]interface{}, 0, len(indexDef.Columns))
			for _, colName := range indexDef.Columns {
				if val, ok := row.Data[colName]; ok && val != nil {
					indexValues = append(indexValues, val.Data)
				} else {
					break
				}
			}
			
			if len(indexValues) == len(indexDef.Columns) {
				var indexKey []byte
				var err error
				if len(indexValues) == 1 {
					indexKey, err = pe.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
				} else {
					indexKey, err = pe.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
				}
				if err != nil {
					return 0, fmt.Errorf("encode index key: %w", err)
				}
				tx.mutations[string(indexKey)] = []byte{}
			}
		}
	}

	return rowID, nil
}

// UpdateRow updates an existing row
func (tx *snapshotTransaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	record, err := tx.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return err
	}

	for col, val := range updates {
		record.Data[col] = val
	}

	pe := tx.engine
	
	key := pe.codec.EncodeTableKey(tenantID, tableID, rowID)
	val, err := pe.codec.EncodeRow(record, schemaDef)
	if err != nil {
		return err
	}

	tx.mutations[string(key)] = val
	return nil
}

// DeleteRow deletes a row by its ID
func (tx *snapshotTransaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	pe := tx.engine
	
	key := pe.codec.EncodeTableKey(tenantID, tableID, rowID)
	tx.mutations[string(key)] = nil

	record, err := tx.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil && err != storage.ErrNotFound {
		return err
	}

	if record != nil && schemaDef.Indexes != nil {
		for i, indexDef := range schemaDef.Indexes {
			indexID := int64(i + 1)
			
			indexValues := make([]interface{}, 0, len(indexDef.Columns))
			for _, colName := range indexDef.Columns {
				if val, ok := record.Data[colName]; ok && val != nil {
					indexValues = append(indexValues, val.Data)
				} else {
					break
				}
			}
			
			if len(indexValues) == len(indexDef.Columns) {
				var indexKey []byte
				var err error
				if len(indexValues) == 1 {
					indexKey, err = pe.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
				} else {
					indexKey, err = pe.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
				}
				if err != nil {
					return fmt.Errorf("encode index key: %w", err)
				}
				tx.mutations[string(indexKey)] = nil
			}
		}
	}

	return nil
}

// UpdateRowBatch updates multiple rows in batch
func (tx *snapshotTransaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	for _, update := range updates {
		if err := tx.UpdateRow(ctx, tenantID, tableID, update.RowID, update.Updates, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRowBatch deletes multiple rows in batch
func (tx *snapshotTransaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	for _, rowID := range rowIDs {
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

// UpdateRowsBatch updates multiple rows in a single batch operation for snapshot transactions
func (tx *snapshotTransaction) UpdateRowsBatch(ctx context.Context, tenantID, tableID int64, rowUpdates map[int64]map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	// Process all updates in a single batch to minimize transaction overhead
	for rowID, updates := range rowUpdates {
		if err := tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return fmt.Errorf("update row %d: %w", rowID, err)
		}
	}
	return nil
}

// DeleteRowsBatch deletes multiple rows in a single batch operation for snapshot transactions
func (tx *snapshotTransaction) DeleteRowsBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	// Process all deletions in a single batch to minimize transaction overhead
	for _, rowID := range rowIDs {
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return fmt.Errorf("delete row %d: %w", rowID, err)
		}
	}
	return nil
}

// UpdateRows updates multiple rows that match the given conditions for snapshot transactions
func (tx *snapshotTransaction) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*dbTypes.Value, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	// Collect all matching row IDs by scanning through the table
	var matchingRowIDs []int64
	
	pe := tx.engine
	
	// Create iterator options to scan the entire table
	iterOpts := &storage.IteratorOptions{
		LowerBound: pe.codec.EncodeTableKey(tenantID, tableID, 0),
		UpperBound: pe.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1)),
	}
	
	iter := tx.snapshot.NewIterator(iterOpts)
	if iter == nil {
		return 0, fmt.Errorf("failed to create iterator")
	}
	defer iter.Close()
	
	// Call First() and check if it succeeded
	if !iter.First() {
		// Check if there was an error
		if err := iter.Error(); err != nil {
			return 0, fmt.Errorf("iterator error: %w", err)
		}
		// No rows to iterate over, which is fine
		return 0, nil
	}
	
	// Continue with the iteration
	for iter.Valid() {
		// Attempt to decode the key
		_, _, rowID, err := pe.codec.DecodeTableKey(iter.Key())
		if err != nil {
			// This might be an index key or other non-table key, skip it
			if !iter.Next() {
				break
			}
			continue
		}
		
		// Decode the row
		value := iter.Value()
		record, err := pe.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return 0, fmt.Errorf("decode row: %w", err)
		}
		
		// Check conditions
		if tx.matchesConditions(record, conditions) {
			matchingRowIDs = append(matchingRowIDs, rowID)
		}
		
		// Move to next row
		if !iter.Next() {
			break
		}
	}
	
	// If no rows match, return early
	if len(matchingRowIDs) == 0 {
		return 0, nil
	}
	
	// Use batch update if there are multiple rows
	if len(matchingRowIDs) > 1 {
		rowUpdates := make(map[int64]map[string]*dbTypes.Value)
		for _, rowID := range matchingRowIDs {
			rowUpdates[rowID] = updates
		}
		if err := tx.UpdateRowsBatch(ctx, tenantID, tableID, rowUpdates, schemaDef); err != nil {
			return 0, fmt.Errorf("batch update rows: %w", err)
		}
		return int64(len(matchingRowIDs)), nil
	}
	
	// Fallback to individual updates
	var count int64
	for _, rowID := range matchingRowIDs {
		if err := tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return count, fmt.Errorf("update row %d: %w", rowID, err)
		}
		count++
	}
	
	return count, nil
}

// DeleteRows deletes multiple rows that match the given conditions for snapshot transactions
func (tx *snapshotTransaction) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	// Collect all matching row IDs by scanning through the table
	var matchingRowIDs []int64
	
	pe := tx.engine
	
	// Create iterator options to scan the entire table
	iterOpts := &storage.IteratorOptions{
		LowerBound: pe.codec.EncodeTableKey(tenantID, tableID, 0),
		UpperBound: pe.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1)),
	}
	
	iter := tx.snapshot.NewIterator(iterOpts)
	if iter == nil {
		return 0, fmt.Errorf("failed to create iterator")
	}
	defer iter.Close()
	
	// Call First() to position the iterator at the first element
	if !iter.First() {
		// Check if there was an error
		if err := iter.Error(); err != nil {
			return 0, fmt.Errorf("iterator error: %w", err)
		}
		// No rows to iterate over, which is fine
		return 0, nil
	}
	
	// Continue with the iteration
	for iter.Valid() {
		// Attempt to decode the key
		_, _, rowID, err := pe.codec.DecodeTableKey(iter.Key())
		if err != nil {
			// This might be an index key or other non-table key, skip it
			if !iter.Next() {
				break
			}
			continue
		}
		
		// Decode the row
		value := iter.Value()
		record, err := pe.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return 0, fmt.Errorf("decode row: %w", err)
		}
		
		// Check conditions
		if tx.matchesConditions(record, conditions) {
			matchingRowIDs = append(matchingRowIDs, rowID)
		}
		
		// Move to next row
		if !iter.Next() {
			break
		}
	}
	
	// If no rows match, return early
	if len(matchingRowIDs) == 0 {
		return 0, nil
	}
	
	// Use batch delete if there are multiple rows
	if len(matchingRowIDs) > 1 {
		if err := tx.DeleteRowsBatch(ctx, tenantID, tableID, matchingRowIDs, schemaDef); err != nil {
			return 0, fmt.Errorf("batch delete rows: %w", err)
		}
		return int64(len(matchingRowIDs)), nil
	}
	
	// Fallback to individual deletions
	var count int64
	for _, rowID := range matchingRowIDs {
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return count, fmt.Errorf("delete row %d: %w", rowID, err)
		}
		count++
	}
	
	return count, nil
}

// matchesConditions checks if a record matches the given conditions
func (tx *snapshotTransaction) matchesConditions(record *dbTypes.Record, conditions map[string]interface{}) bool {
	for col, val := range conditions {
		field, exists := record.Data[col]
		if !exists {
			return false
		}
		
		// Direct comparison first
		if field.Data == val {
			continue
		}
		
		// Handle numeric type mismatches (e.g., int vs int64)
		// This is a common issue when data is encoded/decoded
		if utils.IsNumeric(field.Data) && utils.IsNumeric(val) {
			if utils.CompareNumerics(field.Data, val) == 0 {
				continue
			}
		}
		
		return false
	}
	return true
}

// Commit commits the transaction
func (tx *snapshotTransaction) Commit() error {
	if tx.closed {
		return storage.ErrClosed
	}

	tx.closed = true
	defer tx.snapshot.Close()

	pe := tx.engine
	
	batch := pe.kv.NewBatch()
	for k, v := range tx.mutations {
		if v == nil {
			batch.Delete([]byte(k))
		} else {
			batch.Set([]byte(k), v)
		}
	}

	return pe.kv.CommitBatchWithOptions(context.Background(), batch, storage.SyncWriteOptions)
}

// Rollback rolls back the transaction
func (tx *snapshotTransaction) Rollback() error {
	if tx.closed {
		return nil
	}

	tx.closed = true
	return tx.snapshot.Close()
}

// SetIsolation sets the isolation level for the transaction
func (tx *snapshotTransaction) SetIsolation(level storage.IsolationLevel) error {
	return fmt.Errorf("cannot change isolation level after transaction started")
}

// Isolation returns the isolation level of the transaction
func (tx *snapshotTransaction) Isolation() storage.IsolationLevel {
	// Snapshot transactions typically run at snapshot isolation level
	return storage.SnapshotIsolation
}