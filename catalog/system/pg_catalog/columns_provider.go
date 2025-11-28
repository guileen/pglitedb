package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/catalog/system/pg_catalog/utils"
	"github.com/guileen/pglitedb/types"
)

// QueryPgColumns implements the pg_columns query
func (p *Provider) QueryPgColumns(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	filterTableName, hasTableFilter := filter["tablename"].(string)
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		if hasTableFilter && table.Name != filterTableName {
			continue
		}
		tableSchema, err := p.manager.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		for _, col := range tableSchema.Columns {
			dataType := utils.MapTypeToSQL(col.Type)
			row := []interface{}{
				table.Name,     // tablename
				col.Name,       // columnname
				"public",       // schemaname
				dataType,       // datatype
				nil,            // ordinal_position
				utils.BoolToYesNo(!col.Nullable), // notnull
				col.Default,    // column_default
				nil,            // is_primary_key
				nil,            // is_unique
				nil,            // is_serial
			}
			rows = append(rows, row)
		}
	}
	columns := []types.ColumnInfo{
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "columnname", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "datatype", Type: types.ColumnTypeText},
		{Name: "ordinal_position", Type: types.ColumnTypeInteger},
		{Name: "notnull", Type: types.ColumnTypeText},
		{Name: "column_default", Type: types.ColumnTypeText},
		{Name: "is_primary_key", Type: types.ColumnTypeBoolean},
		{Name: "is_unique", Type: types.ColumnTypeBoolean},
		{Name: "is_serial", Type: types.ColumnTypeBoolean},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}