package catalog

import (
	"context"

	"github.com/guileen/pglitedb/types"
)

type SchemaManager interface {
	CreateTable(ctx context.Context, tenantID int64, def *types.TableDefinition) error
	DropTable(ctx context.Context, tenantID int64, tableName string) error
	GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error)
	AlterTable(ctx context.Context, tenantID int64, tableName string, changes *AlterTableChanges) error
	ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error)
	LoadSchemas(ctx context.Context) error
}

type DataManager interface {
	Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*types.Record, error)
	InsertBatch(ctx context.Context, tenantID int64, tableName string, rows []map[string]interface{}) ([]*types.Record, error)
	Update(ctx context.Context, tenantID int64, tableName string, rowID int64, data map[string]interface{}) (*types.Record, error)
	Delete(ctx context.Context, tenantID int64, tableName string, rowID int64) error
	Get(ctx context.Context, tenantID int64, tableName string, rowID int64) (*types.Record, error)
}

type QueryManager interface {
	Query(ctx context.Context, tenantID int64, tableName string, opts *types.QueryOptions) (*types.QueryResult, error)
	Count(ctx context.Context, tenantID int64, tableName string, filter map[string]interface{}) (int64, error)
}

type IndexManager interface {
	CreateIndex(ctx context.Context, tenantID int64, tableName string, indexDef *types.IndexDefinition) error
	DropIndex(ctx context.Context, tenantID int64, tableName string, indexName string) error
}

type Manager interface {
	SchemaManager
	DataManager
	QueryManager
	IndexManager
}

type AlterTableChanges struct {
	AddColumns    []types.ColumnDefinition
	DropColumns   []string
	ModifyColumns []types.ColumnDefinition
	AddIndexes    []types.IndexDefinition
	DropIndexes   []string
}
