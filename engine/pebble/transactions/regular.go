package transactions

import (
	"context"
	"fmt"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	dbTypes "github.com/guileen/pglitedb/types"
)

// RegularTransaction represents a regular transaction implementation
type RegularTransaction struct {
	engine   engineTypes.StorageEngine
	kvTxn    storage.Transaction
	codec    codec.Codec
	isolation storage.IsolationLevel
}

// NewRegularTransaction creates a new regular transaction
func NewRegularTransaction(engine engineTypes.StorageEngine, kvTxn storage.Transaction, codec codec.Codec) *RegularTransaction {
	return &RegularTransaction{
		engine:   engine,
		kvTxn:    kvTxn,
		codec:    codec,
		isolation: storage.ReadCommitted,
	}
}

// GetRow retrieves a row by its ID
func (t *RegularTransaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := t.kvTxn.Get(key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, dbTypes.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := t.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

// InsertRow inserts a new row
func (t *RegularTransaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	rowID, err := t.engine.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	// TODO: Implement conflict checking mechanism
	// if err := t.engine.CheckForConflicts(t.kvTxn, key); err != nil {
	// 	return 0, fmt.Errorf("conflict check failed: %w", err)
	// }

	value, err := t.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}

	return rowID, nil
}

// UpdateRow updates an existing row
func (t *RegularTransaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	oldRow, err := t.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	// TODO: Implement conflict checking mechanism
	// if err := t.engine.CheckForConflicts(t.kvTxn, key); err != nil {
	// 	return fmt.Errorf("conflict check failed: %w", err)
	// }

	value, err := t.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return fmt.Errorf("update row: %w", err)
	}

	return nil
}

// DeleteRow deletes a row by its ID
func (t *RegularTransaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before deleting
	// TODO: Implement conflict checking mechanism
	// if err := t.engine.CheckForConflicts(t.kvTxn, key); err != nil {
	// 	return fmt.Errorf("conflict check failed: %w", err)
	// }

	if err := t.kvTxn.Delete(key); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	return nil
}

// UpdateRowsBatch updates multiple rows in a single batch operation
func (t *RegularTransaction) UpdateRowsBatch(ctx context.Context, tenantID, tableID int64, rowUpdates map[int64]map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	// Process all updates in a single batch to minimize transaction overhead
	for rowID, updates := range rowUpdates {
		if err := t.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return fmt.Errorf("update row %d: %w", rowID, err)
		}
	}
	return nil
}

// DeleteRowsBatch deletes multiple rows in a single batch operation
func (t *RegularTransaction) DeleteRowsBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	// Process all deletions in a single batch to minimize transaction overhead
	for _, rowID := range rowIDs {
		if err := t.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return fmt.Errorf("delete row %d: %w", rowID, err)
		}
	}
	return nil
}

// DeleteRowBatch deletes multiple rows in batch
func (t *RegularTransaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	for _, rowID := range rowIDs {
		if err := t.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

// UpdateRowBatch updates multiple rows in batch
func (t *RegularTransaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	for _, update := range updates {
		if err := t.UpdateRow(ctx, tenantID, tableID, update.RowID, update.Updates, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

// UpdateRows updates multiple rows that match the given conditions
func (t *RegularTransaction) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*dbTypes.Value, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	// This would need to be implemented with a proper row handler
	// For now, we'll return an error indicating it's not implemented
	return 0, fmt.Errorf("UpdateRows not implemented")
}

// DeleteRows deletes multiple rows that match the given conditions
func (t *RegularTransaction) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	// This would need to be implemented with a proper row handler
	// For now, we'll return an error indicating it's not implemented
	return 0, fmt.Errorf("DeleteRows not implemented")
}

// Commit commits the transaction
func (t *RegularTransaction) Commit() error {
	return t.kvTxn.Commit()
}

// Rollback rolls back the transaction
func (t *RegularTransaction) Rollback() error {
	return t.kvTxn.Rollback()
}

// SetIsolation sets the isolation level for the transaction
func (t *RegularTransaction) SetIsolation(level storage.IsolationLevel) error {
	if err := t.kvTxn.SetIsolation(level); err != nil {
		return err
	}
	t.isolation = level

	// Additional logic for different isolation levels can be added here
	switch level {
	case storage.ReadUncommitted:
		// Minimal consistency guarantees
	case storage.ReadCommitted:
		// Default behavior
	case storage.RepeatableRead:
		// Need to track snapshot
	case storage.SnapshotIsolation:
		// Need to create a snapshot
	case storage.Serializable:
		// Highest isolation level
	}

	return nil
}

// Isolation returns the isolation level of the transaction
func (t *RegularTransaction) Isolation() storage.IsolationLevel {
	return t.isolation
}