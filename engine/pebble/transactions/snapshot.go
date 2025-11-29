package transactions

import (
	"context"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/utils"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/engine/errors"
	"github.com/guileen/pglitedb/engine/pebble/constants"
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
	return errors.Wrap(errors.ErrInvalidOperation, "invalid_operation", "cannot change isolation level after transaction started")
}

// Isolation returns the isolation level of the transaction
func (tx *SnapshotTransaction) Isolation() storage.IsolationLevel {
	if tx.closed {
		return storage.SnapshotIsolation // Still return the expected isolation level even when closed
	}
	// Snapshot transactions typically run at snapshot isolation level
	return storage.SnapshotIsolation
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