package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type snapshotTransaction struct {
	snapshot  storage.Snapshot
	beginTS   int64
	mutations map[string][]byte
	engine    *pebbleEngine
	closed    bool
	isolation storage.IsolationLevel
}

func (e *pebbleEngine) newSnapshotTx(ctx context.Context, level storage.IsolationLevel) (Transaction, error) {
	snapshot, err := e.kv.NewSnapshot()
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}

	return &snapshotTransaction{
		snapshot:  snapshot,
		beginTS:   time.Now().UnixNano(),
		mutations: make(map[string][]byte),
		engine:    e,
		closed:    false,
		isolation: level,
	}, nil
}

func (tx *snapshotTransaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error) {
	if tx.closed {
		return nil, storage.ErrClosed
	}

	key := tx.engine.codec.EncodeTableKey(tenantID, tableID, rowID)

	if val, ok := tx.mutations[string(key)]; ok {
		return tx.engine.codec.DecodeRow(val, schemaDef)
	}

	val, err := tx.snapshot.Get(key)
	if err != nil {
		return nil, err
	}

	return tx.engine.codec.DecodeRow(val, schemaDef)
}

func (tx *snapshotTransaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, storage.ErrClosed
	}

	rowID, err := tx.engine.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, err
	}

	key := tx.engine.codec.EncodeTableKey(tenantID, tableID, rowID)
	val, err := tx.engine.codec.EncodeRow(row, schemaDef)
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
					indexKey, err = tx.engine.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
				} else {
					indexKey, err = tx.engine.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
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

func (tx *snapshotTransaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error {
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

	key := tx.engine.codec.EncodeTableKey(tenantID, tableID, rowID)
	val, err := tx.engine.codec.EncodeRow(record, schemaDef)
	if err != nil {
		return err
	}

	tx.mutations[string(key)] = val
	return nil
}

func (tx *snapshotTransaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error {
	if tx.closed {
		return storage.ErrClosed
	}

	key := tx.engine.codec.EncodeTableKey(tenantID, tableID, rowID)
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
					indexKey, err = tx.engine.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
				} else {
					indexKey, err = tx.engine.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
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

func (tx *snapshotTransaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []RowUpdate, schemaDef *types.TableDefinition) error {
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

func (tx *snapshotTransaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error {
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

	batch := tx.engine.kv.NewBatch()
	for k, v := range tx.mutations {
		if v == nil {
			batch.Delete([]byte(k))
		} else {
			batch.Set([]byte(k), v)
		}
	}

	return tx.engine.kv.CommitBatchWithOptions(context.Background(), batch, storage.SyncWriteOptions)
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
