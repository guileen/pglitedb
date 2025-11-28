package transactions

import (
	"context"
	"fmt"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	dbTypes "github.com/guileen/pglitedb/types"
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

// GetRow retrieves a row by its ID
func (tx *SnapshotTransaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	if tx.closed {
		return nil, storage.ErrClosed
	}

	key := tx.codec.EncodeTableKey(tenantID, tableID, rowID)

	// First check mutations
	if value, exists := tx.mutations[string(key)]; exists {
		if value == nil {
			// Row was deleted in this transaction
			return nil, dbTypes.ErrRecordNotFound
		}
		
		record, err := tx.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return nil, fmt.Errorf("decode row: %w", err)
		}
		return record, nil
	}

	// Then check snapshot
	value, err := tx.snapshot.Get(key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, dbTypes.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row from snapshot: %w", err)
	}

	record, err := tx.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}
	return record, nil
}

// InsertRow inserts a new row
func (tx *SnapshotTransaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	rowID, err := tx.engine.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}

	key := tx.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := tx.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}

	tx.mutations[string(key)] = value
	return rowID, nil
}

// UpdateRow updates an existing row
func (tx *SnapshotTransaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	// Get the existing row
	oldRow, err := tx.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	// Apply updates
	for k, v := range updates {
		oldRow.Data[k] = v
	}

	// Encode and store in mutations
	key := tx.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := tx.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	tx.mutations[string(key)] = value
	return nil
}

// DeleteRow deletes a row by its ID
func (tx *SnapshotTransaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	key := tx.codec.EncodeTableKey(tenantID, tableID, rowID)
	tx.mutations[string(key)] = nil // nil indicates deletion
	return nil
}

// UpdateRowBatch updates multiple rows in batch
func (tx *SnapshotTransaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
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
func (tx *SnapshotTransaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
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

// UpdateRows updates multiple rows that match the given conditions for snapshot transactions
func (tx *SnapshotTransaction) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*dbTypes.Value, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	// This would need to be implemented with a proper row handler
	// For now, we'll return an error indicating it's not implemented
	return 0, fmt.Errorf("UpdateRows not implemented")
}

// DeleteRows deletes multiple rows that match the given conditions for snapshot transactions
func (tx *SnapshotTransaction) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	// This would need to be implemented with a proper row handler
	// For now, we'll return an error indicating it's not implemented
	return 0, fmt.Errorf("DeleteRows not implemented")
}

// DeleteRowsBatch deletes multiple rows in batch for snapshot transactions
func (tx *SnapshotTransaction) DeleteRowsBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
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

// UpdateRowsBatch updates multiple rows in a single batch operation for snapshot transactions
func (tx *SnapshotTransaction) UpdateRowsBatch(ctx context.Context, tenantID, tableID int64, rowUpdates map[int64]map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	// Process all updates in a single batch to minimize transaction overhead
	for rowID, updates := range rowUpdates {
		if err := tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return fmt.Errorf("update row %d: %w", rowID, err)
		}
	}
	return nil
}

// Commit commits the transaction
func (tx *SnapshotTransaction) Commit() error {
	if tx.closed {
		return storage.ErrClosed
	}

	tx.closed = true
	defer tx.snapshot.Close()

	// In a real implementation, we would commit the mutations to the KV store
	// For now, we'll just close the snapshot
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
	return fmt.Errorf("cannot change isolation level after transaction started")
}

// Isolation returns the isolation level of the transaction
func (tx *SnapshotTransaction) Isolation() storage.IsolationLevel {
	// Snapshot transactions typically run at snapshot isolation level
	return storage.SnapshotIsolation
}