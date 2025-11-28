package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/types"
)

// QueryPgTables implements the pg_tables query
func (p *Provider) QueryPgTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		// Apply filters
		if filterTableName, ok := filter["tablename"].(string); ok {
			if table.Name != filterTableName {
				continue
			}
		}
		row := []interface{}{
			table.Name,     // tablename
			"public",       // schemaname
			nil,            // tableowner
			nil,            // hasindexes
			nil,            // hasrules
			nil,            // hastriggers
			nil,            // rowsecurity
		}
		rows = append(rows, row)
	}
	columns := []types.ColumnInfo{
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "tableowner", Type: types.ColumnTypeText},
		{Name: "hasindexes", Type: types.ColumnTypeBoolean},
		{Name: "hasrules", Type: types.ColumnTypeBoolean},
		{Name: "hastriggers", Type: types.ColumnTypeBoolean},
		{Name: "rowsecurity", Type: types.ColumnTypeBoolean},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}