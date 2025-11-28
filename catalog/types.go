package catalog

import (
	"context"

	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
)

type SchemaManager interface {
	CreateTable(ctx context.Context, tenantID int64, def *types.TableDefinition) error
	DropTable(ctx context.Context, tenantID int64, tableName string) error
	GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error)
	AlterTable(ctx context.Context, tenantID int64, tableName string, changes *AlterTableChanges) error
	ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error)
	LoadSchemas(ctx context.Context) error
	
	// View management
	CreateView(ctx context.Context, tenantID int64, viewName string, query string, replace bool) error
	DropView(ctx context.Context, tenantID int64, viewName string) error
	GetViewDefinition(ctx context.Context, tenantID int64, viewName string) (*types.ViewDefinition, error)
	
	// Constraint validation
	ValidateConstraint(ctx context.Context, tenantID int64, tableName string, constraint *types.ConstraintDef) error
}

type DataManager interface {
	Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*types.Record, error)
	InsertBatch(ctx context.Context, tenantID int64, tableName string, rows []map[string]interface{}) ([]*types.Record, error)
	Update(ctx context.Context, tenantID int64, tableName string, rowID int64, data map[string]interface{}) (*types.Record, error)
	Delete(ctx context.Context, tenantID int64, tableName string, rowID int64) error
	Get(ctx context.Context, tenantID int64, tableName string, rowID int64) (*types.Record, error)
	
	// New methods for bulk DML operations
	UpdateRows(ctx context.Context, tenantID int64, tableName string, values map[string]interface{}, conditions map[string]interface{}) (int64, error)
	DeleteRows(ctx context.Context, tenantID int64, tableName string, conditions map[string]interface{}) (int64, error)
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
	QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error)
	
	// New methods for DML operations with more flexible interfaces
	InsertRow(ctx context.Context, tenantID int64, tableName string, values map[string]interface{}) (int64, error)
	UpdateRows(ctx context.Context, tenantID int64, tableName string, values map[string]interface{}, conditions map[string]interface{}) (int64, error)
	DeleteRows(ctx context.Context, tenantID int64, tableName string, conditions map[string]interface{}) (int64, error)
	
	// Statistics collector access
	GetStatsCollector() interfaces.StatsManager
	
	// Helper method for query manager to access system table queries
	SystemTableQuery(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error)
}

type AlterTableChanges struct {
	AddColumns      []types.ColumnDefinition
	DropColumns     []string
	ModifyColumns   []types.ColumnDefinition
	AddIndexes      []types.IndexDefinition
	DropIndexes     []string
	AddConstraints  []types.ConstraintDef
	DropConstraints []string
}