package transactions

import (
	"context"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

// mockEngine implements the StorageEngine interface for testing purposes
type mockEngine struct {
	kv    storage.KV
	codec codec.Codec
}

func (e *mockEngine) Close() error {
	return e.kv.Close()
}

func (e *mockEngine) GetCodec() codec.Codec {
	return e.codec
}

func (e *mockEngine) GetKV() storage.KV {
	return e.kv
}

func (e *mockEngine) CheckForConflicts(txn storage.Transaction, key []byte) error {
	return e.kv.CheckForConflicts(txn, key)
}

// RowOperations interface implementation
func (e *mockEngine) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error) {
	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	
	// Create a simple transaction for reading
	txn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	value, err := txn.Get(key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, types.ErrRecordNotFound
		}
		return nil, err
	}

	record, err := e.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (e *mockEngine) GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) (map[int64]*types.Record, error) {
	return nil, nil
}

func (e *mockEngine) InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error) {
	rowID, err := e.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, err
	}

	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := e.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, err
	}

	// For simplicity in tests, we'll use a direct KV operation
	batch := e.kv.NewBatch()
	defer batch.Close()
	
	if err := batch.Set(key, value); err != nil {
		return 0, err
	}
	
	return rowID, e.kv.Commit(ctx, batch)
}

func (e *mockEngine) InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*types.Record, schemaDef *types.TableDefinition) ([]int64, error) {
	return nil, nil
}

func (e *mockEngine) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error {
	return nil
}

func (e *mockEngine) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *types.TableDefinition) error {
	return nil
}

func (e *mockEngine) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*types.Value, conditions map[string]interface{}, schemaDef *types.TableDefinition) (int64, error) {
	return 0, nil
}

func (e *mockEngine) UpdateRowsBatch(ctx context.Context, tenantID, tableID int64, rowUpdates map[int64]map[string]*types.Value, schemaDef *types.TableDefinition) error {
	return nil
}

func (e *mockEngine) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error {
	return nil
}

func (e *mockEngine) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error {
	return nil
}

func (e *mockEngine) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *types.TableDefinition) (int64, error) {
	return 0, nil
}

func (e *mockEngine) DeleteRowsBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error {
	return nil
}

// IndexOperations interface implementation
func (e *mockEngine) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *types.IndexDefinition) error {
	return nil
}

func (e *mockEngine) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	return nil
}

func (e *mockEngine) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	return nil, nil
}

// ScanOperations interface implementation
func (e *mockEngine) ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *types.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	return nil, nil
}

func (e *mockEngine) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *types.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	return nil, nil
}

// TransactionOperations interface implementation
func (e *mockEngine) BeginTx(ctx context.Context) (engineTypes.Transaction, error) {
	return nil, nil
}

func (e *mockEngine) BeginTxWithIsolation(ctx context.Context, level storage.IsolationLevel) (engineTypes.Transaction, error) {
	return nil, nil
}

// IDGeneration interface implementation
func (e *mockEngine) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	// Simple implementation for tests
	return time.Now().UnixNano(), nil
}

func (e *mockEngine) NextTableID(ctx context.Context, tenantID int64) (int64, error) {
	return time.Now().UnixNano(), nil
}

func (e *mockEngine) NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return time.Now().UnixNano(), nil
}