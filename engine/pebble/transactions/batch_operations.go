// Package transactions provides transaction implementations for the pebble engine.
package transactions

import (
	"context"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/engine/pebble/transactions/errors"
	"github.com/guileen/pglitedb/storage"
)

// BatchOperations provides batch operation methods for transactions
type BatchOperations struct {
	// This struct is intentionally empty as we'll use methods with transaction receivers
}

// UpdateRowsBatch updates multiple rows in a single batch operation
func (t *RegularTransaction) UpdateRowsBatch(ctx context.Context, tenantID, tableID int64, rowUpdates map[int64]map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	if t.closed {
		return errors.ErrClosed
	}

	// Use KV store batch operations for better performance
	batch := t.engine.GetKV().NewBatch()
	defer batch.Close()

	for rowID, updates := range rowUpdates {
		// Get the existing row
		key := t.codec.EncodeTableKey(tenantID, tableID, rowID)
		
		// Check for conflicts before writing
		if err := t.engine.CheckForConflicts(t.kvTxn, key); err != nil {
			return errors.Wrap(err, "conflict_check", "conflict check failed for row %d", rowID)
		}

		value, err := t.kvTxn.Get(key)
		if err != nil {
			if storage.IsNotFound(err) {
				return errors.ErrRowNotFound
			}
			return errors.Wrap(err, "get_failure", "get row %d", rowID)
		}

		record, err := t.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return errors.Wrap(err, "decoding_failure", "decode row %d", rowID)
		}

		// Apply updates
		for k, v := range updates {
			record.Data[k] = v
		}

		// Encode and set in batch
		encodedValue, err := t.codec.EncodeRow(record, schemaDef)
		if err != nil {
			return errors.Wrap(err, "encoding_failure", "encode row %d", rowID)
		}

		if err := batch.Set(key, encodedValue); err != nil {
			return errors.Wrap(err, "batch_set_failure", "batch set row %d", rowID)
		}
	}

	// Commit the batch
	if err := t.engine.GetKV().CommitBatch(ctx, batch); err != nil {
		return errors.Wrap(err, "batch_commit_failure", "commit batch")
	}

	return nil
}

// DeleteRowsBatch deletes multiple rows in a single batch operation
func (t *RegularTransaction) DeleteRowsBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if t.closed {
		return errors.ErrClosed
	}

	// Use KV store batch operations for better performance
	batch := t.engine.GetKV().NewBatch()
	defer batch.Close()

	// Process all deletions in a single batch to minimize transaction overhead
	for _, rowID := range rowIDs {
		key := t.codec.EncodeTableKey(tenantID, tableID, rowID)
		
		// Check for conflicts before deleting
		if err := t.engine.CheckForConflicts(t.kvTxn, key); err != nil {
			return errors.Wrap(err, "conflict_check", "conflict check failed for row %d", rowID)
		}

		if err := batch.Delete(key); err != nil {
			return errors.Wrap(err, "batch_delete_failure", "batch delete row %d", rowID)
		}
	}

	// Commit the batch
	if err := t.engine.GetKV().CommitBatch(ctx, batch); err != nil {
		return errors.Wrap(err, "batch_commit_failure", "commit batch")
	}

	return nil
}

// DeleteRowBatch deletes multiple rows in batch
func (t *RegularTransaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if t.closed {
		return errors.ErrClosed
	}

	for _, rowID := range rowIDs {
		if err := t.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return errors.Wrap(err, "batch_delete_failure", "failed to delete row %d", rowID)
		}
	}
	return nil
}

// UpdateRowBatch updates multiple rows in batch
func (t *RegularTransaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	if t.closed {
		return errors.ErrClosed
	}

	for _, update := range updates {
		if err := t.UpdateRow(ctx, tenantID, tableID, update.RowID, update.Updates, schemaDef); err != nil {
			return errors.Wrap(err, "batch_update_failure", "failed to update row %d", update.RowID)
		}
	}
	return nil
}

// UpdateRowsBatch for SnapshotTransaction
func (tx *SnapshotTransaction) UpdateRowsBatch(ctx context.Context, tenantID, tableID int64, rowUpdates map[int64]map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	// Process all updates in a single batch to minimize transaction overhead
	for rowID, updates := range rowUpdates {
		if err := tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return errors.Wrap(err, "batch_update_failure", "failed to update row %d", rowID)
		}
	}
	return nil
}

// DeleteRowsBatch for SnapshotTransaction
func (tx *SnapshotTransaction) DeleteRowsBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	// Process all deletions in a single batch to minimize transaction overhead
	for _, rowID := range rowIDs {
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return errors.Wrap(err, "batch_delete_failure", "failed to delete row %d", rowID)
		}
	}
	return nil
}

// DeleteRowBatch for SnapshotTransaction
func (tx *SnapshotTransaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	for _, rowID := range rowIDs {
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return errors.Wrap(err, "batch_delete_failure", "failed to delete row %d", rowID)
		}
	}
	return nil
}

// UpdateRowBatch for SnapshotTransaction
func (tx *SnapshotTransaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return errors.ErrClosed
	}

	for _, update := range updates {
		if err := tx.UpdateRow(ctx, tenantID, tableID, update.RowID, update.Updates, schemaDef); err != nil {
			return errors.Wrap(err, "batch_update_failure", "failed to update row %d", update.RowID)
		}
	}
	return nil
}