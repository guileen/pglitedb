package pebble

import (
	"context"
	"fmt"

	engineTypes "github.com/guileen/pglitedb/engine/types"
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
		return pe.codec.DecodeRow(val, schemaDef)
	}

	val, err := tx.snapshot.Get(key)
	if err != nil {
		return nil, err
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

// UpdateRows updates multiple rows that match the given conditions for snapshot transactions
func (tx *snapshotTransaction) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*dbTypes.Value, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	handler := NewRowHandler(tx.engine.buildFilterExpression)
	return handler.UpdateRows(ctx, tenantID, tableID, updates, conditions, schemaDef, tx.engine.ScanRows, tx.updateRowImpl)
}

// DeleteRows deletes multiple rows that match the given conditions for snapshot transactions
func (tx *snapshotTransaction) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	handler := NewRowHandler(tx.engine.buildFilterExpression)
	return handler.DeleteRows(ctx, tenantID, tableID, conditions, schemaDef, tx.engine.ScanRows, tx.deleteRowImpl)
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

// updateRowImpl implements the update row functionality for snapshot transactions
func (tx *snapshotTransaction) updateRowImpl(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	return tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef)
}

// deleteRowImpl implements the delete row functionality for snapshot transactions
func (tx *snapshotTransaction) deleteRowImpl(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	return tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef)
}