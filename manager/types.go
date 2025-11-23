package manager

import (
	"context"

	"github.com/guileen/pglitedb/table"
)

type Manager interface {
	CreateTable(ctx context.Context, tenantID int64, def *table.TableDefinition) error
	DropTable(ctx context.Context, tenantID int64, tableName string) error
	GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*table.TableDefinition, error)
	AlterTable(ctx context.Context, tenantID int64, tableName string, changes *AlterTableChanges) error
	ListTables(ctx context.Context, tenantID int64) ([]*table.TableDefinition, error)

	Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*table.Record, error)
	InsertBatch(ctx context.Context, tenantID int64, tableName string, rows []map[string]interface{}) ([]*table.Record, error)
	Update(ctx context.Context, tenantID int64, tableName string, rowID int64, data map[string]interface{}) (*table.Record, error)
	Delete(ctx context.Context, tenantID int64, tableName string, rowID int64) error
	Get(ctx context.Context, tenantID int64, tableName string, rowID int64) (*table.Record, error)

	Query(ctx context.Context, tenantID int64, tableName string, opts *table.QueryOptions) (*table.QueryResult, error)
	Count(ctx context.Context, tenantID int64, tableName string, filter map[string]interface{}) (int64, error)

	CreateIndex(ctx context.Context, tenantID int64, tableName string, indexDef *table.IndexDefinition) error
	DropIndex(ctx context.Context, tenantID int64, tableName string, indexName string) error
}

type AlterTableChanges struct {
	AddColumns    []table.ColumnDefinition
	DropColumns   []string
	ModifyColumns []table.ColumnDefinition
	AddIndexes    []table.IndexDefinition
	DropIndexes   []string
}
