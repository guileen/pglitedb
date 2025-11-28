package types

import (
	"context"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

// RowOperations defines CRUD operations for rows
type RowOperations interface {
	GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error)
	GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) (map[int64]*types.Record, error)
	InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error)
	InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*types.Record, schemaDef *types.TableDefinition) ([]int64, error)
	UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error
	UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []RowUpdate, schemaDef *types.TableDefinition) error
	UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*types.Value, conditions map[string]interface{}, schemaDef *types.TableDefinition) (int64, error)
	DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error
	DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error
	DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *types.TableDefinition) (int64, error)
}

// IndexOperations defines index management operations
type IndexOperations interface {
	CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *types.IndexDefinition) error
	DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error
	LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error)
}

// ScanOperations defines scan operations
type ScanOperations interface {
	ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *types.TableDefinition, opts *ScanOptions) (RowIterator, error)
	ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *types.TableDefinition, opts *ScanOptions) (RowIterator, error)
}

// TransactionOperations defines transaction handling operations
type TransactionOperations interface {
	BeginTx(ctx context.Context) (Transaction, error)
	BeginTxWithIsolation(ctx context.Context, level storage.IsolationLevel) (Transaction, error)
}

// IDGeneration defines ID generation services
type IDGeneration interface {
	NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error)
	NextTableID(ctx context.Context, tenantID int64) (int64, error)
	NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error)
}