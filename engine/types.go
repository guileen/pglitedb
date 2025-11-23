package engine

import (
	"context"

	"github.com/guileen/pqlitedb/kv"
	"github.com/guileen/pqlitedb/table"
)

type StorageEngine interface {
	GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *table.TableDefinition) (*table.Record, error)
	InsertRow(ctx context.Context, tenantID, tableID int64, row *table.Record, schemaDef *table.TableDefinition) (int64, error)
	UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*table.Value, schemaDef *table.TableDefinition) error
	DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *table.TableDefinition) error

	CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *table.IndexDefinition) error
	DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error
	LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error)

	ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *table.TableDefinition, opts *ScanOptions) (RowIterator, error)
	ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *table.TableDefinition, opts *ScanOptions) (RowIterator, error)

	BeginTx(ctx context.Context) (Transaction, error)
	BeginTxWithIsolation(ctx context.Context, level kv.IsolationLevel) (Transaction, error)

	NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error)
	NextTableID(ctx context.Context, tenantID int64) (int64, error)
	NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error)

	Close() error
}

type ScanOptions struct {
	StartKey   []byte
	EndKey     []byte
	Prefix     []byte
	Limit      int
	Offset     int
	Reverse    bool
	Projection []string
}

type RowIterator interface {
	Next() bool
	Row() *table.Record
	Error() error
	Close() error
}

type Transaction interface {
	GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *table.TableDefinition) (*table.Record, error)
	InsertRow(ctx context.Context, tenantID, tableID int64, row *table.Record, schemaDef *table.TableDefinition) (int64, error)
	UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*table.Value, schemaDef *table.TableDefinition) error
	DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *table.TableDefinition) error
	Commit() error
	Rollback() error

	// Isolation returns the isolation level of the transaction
	Isolation() kv.IsolationLevel
	// SetIsolation sets the isolation level for the transaction
	SetIsolation(level kv.IsolationLevel) error
}
