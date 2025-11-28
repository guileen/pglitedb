package system

import (
	"context"
	"fmt"
	
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/catalog/system/information_schema"
	"github.com/guileen/pglitedb/catalog/system/pg_catalog"
	"github.com/guileen/pglitedb/catalog/system/query"
	"github.com/guileen/pglitedb/types"
)

// Catalog implements the SystemCatalog interface
type Catalog struct {
	manager interfaces.TableManager
	infoSchemaProvider *informationschema.Provider
	pgCatalogProvider *pgcatalog.Provider
}

// NewCatalog creates a new system catalog
func NewCatalog(manager interfaces.TableManager) *Catalog {
	return &Catalog{
		manager: manager,
		infoSchemaProvider: informationschema.NewProvider(manager),
		pgCatalogProvider: pgcatalog.NewProvider(manager),
	}
}

// QuerySystemTable implements the SystemCatalog interface
func (c *Catalog) QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	query := query.ParseSystemTableQuery(fullTableName)
	if query == nil {
		return nil, fmt.Errorf("invalid system table name: %s", fullTableName)
	}
	if filter != nil {
		query.Filter = filter
	}
	
	switch query.Schema {
	case "information_schema":
		switch query.TableName {
		case "tables":
			return c.infoSchemaProvider.QueryTables(ctx, query.Filter)
		case "columns":
			return c.infoSchemaProvider.QueryColumns(ctx, query.Filter)
		default:
			return nil, fmt.Errorf("unsupported information_schema table: %s", query.TableName)
		}
	case "pg_catalog":
		switch query.TableName {
		case "pg_tables":
			return c.pgCatalogProvider.QueryPgTables(ctx, query.Filter)
		case "pg_columns":
			return c.pgCatalogProvider.QueryPgColumns(ctx, query.Filter)
		case "pg_indexes":
			return c.pgCatalogProvider.QueryPgIndexes(ctx, query.Filter)
		case "pg_constraint":
			return c.pgCatalogProvider.QueryPgConstraint(ctx, query.Filter)
		case "pg_views":
			return c.pgCatalogProvider.QueryPgViews(ctx, query.Filter)
		case "pg_stat_user_tables":
			return c.pgCatalogProvider.QueryPgStatUserTables(ctx, query.Filter)
		case "pg_stat_user_indexes":
			return c.pgCatalogProvider.QueryPgStatUserIndexes(ctx, query.Filter)
		case "pg_stats":
			return c.pgCatalogProvider.QueryPgStats(ctx, query.Filter)
		case "pg_stat_database":
			return c.pgCatalogProvider.QueryPgStatDatabase(ctx, query.Filter)
		case "pg_stat_bgwriter":
			return c.pgCatalogProvider.QueryPgStatBgWriter(ctx, query.Filter)
		case "pg_index":
			return c.pgCatalogProvider.QueryPgIndex(ctx, query.Filter)
		case "pg_inherits":
			return c.pgCatalogProvider.QueryPgInherits(ctx, query.Filter)
		case "pg_class":
			return c.pgCatalogProvider.QueryPgClass(ctx, query.Filter)
		case "pg_attribute":
			return c.pgCatalogProvider.QueryPgAttribute(ctx, query.Filter)
		case "pg_type":
			return c.pgCatalogProvider.QueryPgType(ctx, query.Filter)
		case "pg_namespace":
			return c.pgCatalogProvider.QueryPgNamespace(ctx, query.Filter)
		case "pg_proc":
			return c.pgCatalogProvider.QueryPgProc(ctx, query.Filter)
		default:
			return nil, fmt.Errorf("unsupported pg_catalog table: %s", query.TableName)
		}
	default:
		return nil, fmt.Errorf("unsupported system schema: %s", query.Schema)
	}
}