package transactions

import (
	"context"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/utils"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/engine/pebble/transactions/errors"
)

// RegularTransaction represents a regular transaction implementation
type RegularTransaction struct {
	engine   engineTypes.StorageEngine
	kvTxn    storage.Transaction
	codec    codec.Codec
	isolation storage.IsolationLevel
	closed   bool
}

// NewRegularTransaction creates a new regular transaction
func NewRegularTransaction(engine engineTypes.StorageEngine, kvTxn storage.Transaction, codec codec.Codec) *RegularTransaction {
	return &RegularTransaction{
		engine:   engine,
		kvTxn:    kvTxn,
		codec:    codec,
		isolation: storage.ReadCommitted,
		closed:   false,
	}
}

// GetRow retrieves a row by its ID
func (t *RegularTransaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	if t.closed {
		return nil, errors.ErrClosed
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := t.kvTxn.Get(key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, errors.ErrRowNotFound
		}
		return nil, errors.Wrap(err, "get_failure", "get row")
	}

	record, err := t.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, errors.Wrap(err, "decoding_failure", "decode row")
	}

	return record, nil
}

// InsertRow inserts a new row
func (t *RegularTransaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if t.closed {
		return 0, errors.ErrClosed
	}

	rowID, err := t.engine.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, errors.Wrap(err, "generate_id_failure", "generate row id")
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	// if err := t.engine.CheckForConflicts(t.kvTxn, key); err != nil {
	// 	return 0, errors.Wrap(err, "conflict_check", "conflict check failed")
	// }

	value, err := t.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, errors.Wrap(err, "encoding_failure", "encode row")
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return 0, errors.Wrap(err, "set_failure", "insert row")
	}

	return rowID, nil
}

// UpdateRow updates an existing row
func (t *RegularTransaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	if t.closed {
		return errors.ErrClosed
	}

	oldRow, err := t.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return errors.Wrap(err, "get_row_failure", "get old row")
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	// if err := t.engine.CheckForConflicts(t.kvTxn, key); err != nil {
	// 	return errors.Wrap(err, "conflict_check", "conflict check failed")
	// }

	value, err := t.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return errors.Wrap(err, "encoding_failure", "encode row")
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return errors.Wrap(err, "set_failure", "update row")
	}

	return nil
}

// DeleteRow deletes a row by its ID
func (t *RegularTransaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	if t.closed {
		return errors.ErrClosed
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before deleting
	// if err := t.engine.CheckForConflicts(t.kvTxn, key); err != nil {
	// 	return errors.Wrap(err, "conflict_check", "conflict check failed")
	// }

	if err := t.kvTxn.Delete(key); err != nil {
		return errors.Wrap(err, "delete_failure", "delete row")
	}

	return nil
}

// Commit commits the transaction
func (t *RegularTransaction) Commit() error {
	if t.closed {
		return errors.ErrClosed
	}

	t.closed = true
	return t.kvTxn.Commit()
}

// Rollback rolls back the transaction
func (t *RegularTransaction) Rollback() error {
	if t.closed {
		return nil
	}

	t.closed = true
	return t.kvTxn.Rollback()
}

// SetIsolation sets the isolation level for the transaction
func (t *RegularTransaction) SetIsolation(level storage.IsolationLevel) error {
	if t.closed {
		return errors.ErrClosed
	}

	if err := t.kvTxn.SetIsolation(level); err != nil {
		return errors.Wrap(err, "isolation_set_failure", "set isolation level")
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
	if t.closed {
		return storage.ReadCommitted // Return default isolation level when closed
	}
	return t.isolation
}

// matchesConditions checks if a record matches the given conditions
func (t *RegularTransaction) matchesConditions(record *dbTypes.Record, conditions map[string]interface{}) bool {
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