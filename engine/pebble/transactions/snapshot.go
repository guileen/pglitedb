package transactions

import (
	"context"
	"fmt"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/utils"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/engine/pebble/transactions/errors"
)

// SnapshotTransaction represents a snapshot transaction implementation
type SnapshotTransaction struct {
	engine     engineTypes.StorageEngine
	snapshot   storage.Snapshot
	mutations  map[string][]byte
	closed     bool
	codec      codec.Codec
}

// NewSnapshotTransaction creates a new snapshot transaction
func NewSnapshotTransaction(engine engineTypes.StorageEngine, snapshot storage.Snapshot) *SnapshotTransaction {
	// Get the codec from the engine
	codec := engine.GetCodec()
	
	return &SnapshotTransaction{
		engine:    engine,
		snapshot:  snapshot,
		mutations: make(map[string][]byte),
		closed:    false,
		codec:     codec,
	}
}

// GetRow retrieves a row by its ID using snapshot semantics
func (tx *SnapshotTransaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	if tx.closed {
		return nil, errors.ErrClosed
	}

	key := tx.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check mutations first (writes in this transaction)
	if value, exists := tx.mutations[string(key)]; exists {
		if value == nil {
			// Deleted in this transaction
			return nil, errors.ErrRowNotFound
		}
		// Updated/inserted in this transaction
		record, err := tx.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return nil, errors.Wrap(err, "decoding_failure", "decode row from mutation")
		}
		return record, nil
	}

	// Not in mutations, check snapshot
	value, err := tx.snapshot.Get(key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, errors.ErrRowNotFound
		}
		return nil, errors.Wrap(err, "snapshot_get_failure", "get from snapshot")
	}

	record, err := tx.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, errors.Wrap(err, "decoding_failure", "decode row from snapshot")
	}

	return record, nil
}

// InsertRow inserts a new row in the snapshot transaction
func (tx *SnapshotTransaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, errors.ErrClosed
	}

	rowID, err := tx.engine.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, errors.Wrap(err, "generate_id_failure", "generate row id")
	}

	key := tx.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := tx.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, errors.Wrap(err, "encoding_failure", "encode row")
	}

	// Store in mutations
	tx.mutations[string(key)] = value

	return rowID, nil
}

// UpdateRow updates an existing row in the snapshot transaction
func (tx *SnapshotTransaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	// Get the current row (could be from snapshot or previous mutations)
	oldRow, err := tx.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return errors.Wrap(err, "get_row_failure", "get row")
	}

	// Apply updates
	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := tx.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := tx.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return errors.Wrap(err, "encoding_failure", "encode row")
	}

	// Store in mutations
	tx.mutations[string(key)] = value

	return nil
}

// DeleteRow deletes a row in the snapshot transaction
func (tx *SnapshotTransaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	key := tx.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Mark as deleted in mutations (nil value indicates deletion)
	tx.mutations[string(key)] = nil

	return nil
}

// UpdateRowsBatch updates multiple rows in a single batch operation
func (tx *SnapshotTransaction) UpdateRowsBatch(ctx context.Context, tenantID, tableID int64, rowUpdates map[int64]map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	// Process all updates in a single batch to minimize transaction overhead
	for rowID, updates := range rowUpdates {
		if err := tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return errors.Wrap(err, "batch_update_failure", "update row %d", rowID)
		}
	}
	return nil
}

// DeleteRowsBatch deletes multiple rows in a single batch operation
func (tx *SnapshotTransaction) DeleteRowsBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	// Process all deletions in a single batch to minimize transaction overhead
	for _, rowID := range rowIDs {
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return errors.Wrap(err, "batch_delete_failure", "delete row %d", rowID)
		}
	}
	return nil
}

// DeleteRowBatch deletes multiple rows in batch
func (tx *SnapshotTransaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	for _, rowID := range rowIDs {
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return errors.Wrap(err, "batch_delete_failure", "delete row %d", rowID)
		}
	}
	return nil
}

// UpdateRowBatch updates multiple rows in batch
func (tx *SnapshotTransaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	for _, update := range updates {
		if err := tx.UpdateRow(ctx, tenantID, tableID, update.RowID, update.Updates, schemaDef); err != nil {
			return errors.Wrap(err, "batch_update_failure", "update row %d", update.RowID)
		}
	}
	return nil
}

// UpdateRows updates multiple rows that match the given conditions
func (tx *SnapshotTransaction) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*dbTypes.Value, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, errors.ErrClosed
	}

	// Collect all matching row IDs by scanning through the table
	var matchingRowIDs []int64
	
	// Create iterator options to scan the entire table
	iterOpts := &storage.IteratorOptions{
		LowerBound: tx.codec.EncodeTableKey(tenantID, tableID, 0),
		UpperBound: tx.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1)),
	}
	
	iter := tx.snapshot.NewIterator(iterOpts)
	if iter == nil {
		return 0, errors.ErrIteratorCreation
	}
	defer iter.Close()
	
	// Call First() and check if it succeeded
	if !iter.First() {
		// Check if there was an error
		if err := iter.Error(); err != nil {
			return 0, errors.Wrap(err, "iterator_failure", "iterator error")
		}
		// No rows to iterate over, which is fine
		return 0, nil
	}
	
	// Continue with the iteration
	for iter.Valid() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		
		_, _, rowID, err := tx.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode table key")
		}
		
		// Decode the row
		value := iter.Value()
		record, err := tx.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode row")
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
			return 0, errors.Wrap(err, "batch_update_failure", "batch update rows")
		}
		return int64(len(matchingRowIDs)), nil
	}
	
	// Fallback to individual updates
	var count int64
	for _, rowID := range matchingRowIDs {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}
		
		if err := tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return count, errors.Wrap(err, "update_failure", "update row %d", rowID)
		}
		count++
	}
	
	return count, nil
}

// DeleteRows deletes multiple rows that match the given conditions
func (tx *SnapshotTransaction) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, errors.ErrClosed
	}

	// Collect all matching row IDs by scanning through the table
	var matchingRowIDs []int64
	
	// Create iterator options to scan the entire table
	iterOpts := &storage.IteratorOptions{
		LowerBound: tx.codec.EncodeTableKey(tenantID, tableID, 0),
		UpperBound: tx.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1)),
	}
	
	iter := tx.snapshot.NewIterator(iterOpts)
	if iter == nil {
		return 0, errors.ErrIteratorCreation
	}
	defer iter.Close()
	
	// Call First() and check if it succeeded
	if !iter.First() {
		// Check if there was an error
		if err := iter.Error(); err != nil {
			return 0, errors.Wrap(err, "iterator_failure", "iterator error")
		}
		// No rows to iterate over, which is fine
		return 0, nil
	}
	
	// Continue with the iteration
	for iter.Valid() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		
		_, _, rowID, err := tx.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode table key")
		}
		
		// Decode the row
		value := iter.Value()
		record, err := tx.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode row")
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
			return 0, errors.Wrap(err, "batch_delete_failure", "batch delete rows")
		}
		return int64(len(matchingRowIDs)), nil
	}
	
	// Fallback to individual deletions
	var count int64
	for _, rowID := range matchingRowIDs {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}
		
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return count, errors.Wrap(err, "delete_failure", "delete row %d", rowID)
		}
		count++
	}
	
	return count, nil
}

// matchesConditions checks if a record matches the given conditions
func (tx *SnapshotTransaction) matchesConditions(record *dbTypes.Record, conditions map[string]interface{}) bool {
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
func (tx *SnapshotTransaction) Commit() error {
	if tx.closed {
		return errors.ErrClosed
	}

	tx.closed = true
	defer tx.snapshot.Close()

	// Apply mutations to the KV store
	batch := tx.engine.GetKV().NewBatch()
	defer batch.Close()
	
	for key, value := range tx.mutations {
		if value == nil {
			if err := batch.Delete([]byte(key)); err != nil {
				return errors.Wrap(err, "batch_operation", "delete key")
			}
		} else {
			if err := batch.Set([]byte(key), value); err != nil {
				return errors.Wrap(err, "batch_operation", "set key")
			}
		}
	}
	
	// Commit the batch
	if err := tx.engine.GetKV().CommitBatch(context.Background(), batch); err != nil {
		return errors.Wrap(err, "batch_commit", "commit batch")
	}
	
	return nil
}

// Rollback rolls back the transaction
func (tx *SnapshotTransaction) Rollback() error {
	if tx.closed {
		return nil
	}

	tx.closed = true
	return tx.snapshot.Close()
}

// SetIsolation sets the isolation level for the transaction
func (tx *SnapshotTransaction) SetIsolation(level storage.IsolationLevel) error {
	return errors.Wrap(errors.ErrInvalidIsolation, "invalid_operation", "cannot change isolation level after transaction started")
}

// Isolation returns the isolation level of the transaction
func (tx *SnapshotTransaction) Isolation() storage.IsolationLevel {
	if tx.closed {
		return storage.SnapshotIsolation // Still return the expected isolation level even when closed
	}
	// Snapshot transactions typically run at snapshot isolation level
	return storage.SnapshotIsolation
}