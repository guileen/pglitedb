// Package engine provides the engine module for the database
package engine

import (
	"context"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type StorageEngine interface {
	GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error)
	GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) (map[int64]*types.Record, error)
	InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error)
	InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*types.Record, schemaDef *types.TableDefinition) ([]int64, error)
	UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error
	UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []RowUpdate, schemaDef *types.TableDefinition) error
	DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error
	DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error

	CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *types.IndexDefinition) error
	DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error
	LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error)

	ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *types.TableDefinition, opts *ScanOptions) (RowIterator, error)
	ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *types.TableDefinition, opts *ScanOptions) (RowIterator, error)

	BeginTx(ctx context.Context) (Transaction, error)
	BeginTxWithIsolation(ctx context.Context, level storage.IsolationLevel) (Transaction, error)

	NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error)
	NextTableID(ctx context.Context, tenantID int64) (int64, error)
	NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error)

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
	Type     string                // "simple", "and", "or", "not"
	Column   string                // for simple filters
	Operator string                // "=", ">", "<", ">=", "<=", "IN", "BETWEEN", "LIKE"
	Value    interface{}           // single value
	Values   []interface{}         // multiple values (IN, BETWEEN)
	Children []*FilterExpression   // for AND/OR/NOT composite filters
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
	DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error
	DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error
	Commit() error
	Rollback() error

	// Isolation returns the isolation level of the transaction
	Isolation() storage.IsolationLevel
	// SetIsolation sets the isolation level for the transaction
	SetIsolation(level storage.IsolationLevel) error
}
