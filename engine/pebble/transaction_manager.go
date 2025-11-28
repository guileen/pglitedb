package pebble

import (
	"context"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// Regular transaction implementation
type transaction struct {
	*BaseTransaction
	kvTxn storage.Transaction
	codec codec.Codec
}

// Snapshot transaction implementation
type snapshotTransaction struct {
	*BaseTransaction
	snapshot  storage.Snapshot
	beginTS   int64
	mutations map[string][]byte
	engine    *pebbleEngine
	closed    bool
}

// BeginTx starts a new transaction with default isolation level
func (e *pebbleEngine) BeginTx(ctx context.Context) (engineTypes.Transaction, error) {
	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	baseTx := NewBaseTransaction(e, storage.ReadCommitted)
	tx := &transaction{
		BaseTransaction: baseTx,
		kvTxn:           kvTxn,
		codec:           e.codec,
	}
	
	// Set the isolation level on the base transaction
	if err := tx.SetIsolation(storage.ReadCommitted); err != nil {
		kvTxn.Rollback()
		return nil, fmt.Errorf("set isolation level: %w", err)
	}
	
	return tx, nil
}

// BeginTxWithIsolation starts a new transaction with specified isolation level
func (e *pebbleEngine) BeginTxWithIsolation(ctx context.Context, level storage.IsolationLevel) (engineTypes.Transaction, error) {
	if level >= storage.RepeatableRead {
		return e.newSnapshotTx(ctx, level)
	}

	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	if err := kvTxn.SetIsolation(level); err != nil {
		kvTxn.Rollback()
		return nil, fmt.Errorf("set isolation level: %w", err)
	}

	baseTx := NewBaseTransaction(e, level)
	tx := &transaction{
		BaseTransaction: baseTx,
		kvTxn:           kvTxn,
		codec:           e.codec,
	}
	
	// Set the isolation level on the base transaction
	tx.SetIsolation(level)
	
	return tx, nil
}

// newSnapshotTx creates a new snapshot transaction
func (e *pebbleEngine) newSnapshotTx(ctx context.Context, level storage.IsolationLevel) (engineTypes.Transaction, error) {
	snapshot, err := e.kv.NewSnapshot()
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}

	baseTx := NewBaseTransaction(e, level)
	tx := &snapshotTransaction{
		BaseTransaction: baseTx,
		snapshot:        snapshot,
		beginTS:         time.Now().UnixNano(),
		mutations:       make(map[string][]byte),
		engine:          e,
		closed:          false,
	}
	
	return tx, nil
}

// Transaction methods
func (t *transaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
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

func (t *transaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	rowID, err := t.engine.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return 0, fmt.Errorf("conflict check failed: %w", err)
	}

	value, err := t.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}

	return rowID, nil
}

func (t *transaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	oldRow, err := t.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return fmt.Errorf("conflict check failed: %w", err)
	}

	value, err := t.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return fmt.Errorf("update row: %w", err)
	}

	return nil
}

func (t *transaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before deleting
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return fmt.Errorf("conflict check failed: %w", err)
	}

	if err := t.kvTxn.Delete(key); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	return nil
}

func (t *transaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	for _, update := range updates {
		if err := t.UpdateRow(ctx, tenantID, tableID, update.RowID, update.Updates, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	for _, rowID := range rowIDs {
		if err := t.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) Isolation() storage.IsolationLevel {
	return t.isolation
}

func (t *transaction) SetIsolation(level storage.IsolationLevel) error {
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

func (t *transaction) Commit() error {
	return t.kvTxn.Commit()
}

func (t *transaction) Rollback() error {
	return t.kvTxn.Rollback()
}



// Snapshot transaction methods
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

func (tx *snapshotTransaction) Rollback() error {
	if tx.closed {
		return nil
	}

	tx.closed = true
	return tx.snapshot.Close()
}

func (tx *snapshotTransaction) Isolation() storage.IsolationLevel {
	return tx.isolation
}

func (tx *snapshotTransaction) SetIsolation(level storage.IsolationLevel) error {
	return fmt.Errorf("cannot change isolation level after transaction started")
}

// updateRowImpl implements the update row functionality for regular transactions
func (t *transaction) updateRowImpl(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	return t.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef)
}

// deleteRowImpl implements the delete row functionality for regular transactions
func (t *transaction) deleteRowImpl(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	return t.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef)
}

// updateRowImpl implements the update row functionality for snapshot transactions
func (tx *snapshotTransaction) updateRowImpl(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	return tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef)
}

// deleteRowImpl implements the delete row functionality for snapshot transactions
func (tx *snapshotTransaction) deleteRowImpl(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	return tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef)
}