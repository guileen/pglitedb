package types

import (
	"context"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type StorageEngine interface {
	RowOperations
	IndexOperations
	ScanOperations
	TransactionOperations
	IDGeneration

	Close() error
}

type RowUpdate struct {
	RowID   int64
	Updates map[string]*types.Value
}

type ScanOptions struct {
	StartKey   []byte
	EndKey     []byte
	Prefix     []byte
	Limit      int
	Offset     int
	Reverse    bool
	Projection []string
	Filter     *FilterExpression
}

type FilterExpression struct {
	Type     string              // "simple", "and", "or", "not"
	Column   string              // for simple filters
	Operator string              // "=", ">", "<", ">=", "<=", "IN", "BETWEEN", "LIKE"
	Value    interface{}         // single value
	Values   []interface{}       // multiple values (IN, BETWEEN)
	Children []*FilterExpression // for AND/OR/NOT composite filters
}

type RowIterator interface {
	Next() bool
	Row() *types.Record
	Error() error
	Close() error
}

type Transaction interface {
	GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error)
	InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error)
	UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error
	UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []RowUpdate, schemaDef *types.TableDefinition) error
	UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*types.Value, conditions map[string]interface{}, schemaDef *types.TableDefinition) (int64, error)
	DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error
	DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error
	DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *types.TableDefinition) (int64, error)
	Commit() error
	Rollback() error

	// Isolation returns the isolation level of the transaction
	Isolation() storage.IsolationLevel
	// SetIsolation sets the isolation level for the transaction
	SetIsolation(level storage.IsolationLevel) error
}