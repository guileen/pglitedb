package pgcatalog

import (
	"context"
	"strings"
	"github.com/guileen/pglitedb/types"
)

// QueryPgIndexes implements the pg_indexes query
func (p *Provider) QueryPgIndexes(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		// Apply table name filter
		if filterTableName, ok := filter["tablename"].(string); ok {
			if table.Name != filterTableName {
				continue
			}
		}
		tableSchema, err := p.manager.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		// Add row for each index
		for _, index := range tableSchema.Indexes {
			// Determine if this is a primary key index
			isPrimary := false
			for _, constraint := range tableSchema.Constraints {
				if constraint.Type == "primary_key" && len(constraint.Columns) == len(index.Columns) {
					match := true
					for i, col := range constraint.Columns {
						if i >= len(index.Columns) || col != index.Columns[i] {
							match = false
							break
						}
					}
					if match {
						isPrimary = true
						break
					}
				}
			}
			row := []interface{}{
				table.Name,                          // tablename
				index.Name,                          // indexname
				"public",                            // schemaname
				strings.Join(index.Columns, ","),    // columnnames
				index.Unique,                        // unique
				index.Type,                          // indextype
				nil,                                 // tablespace
				isPrimary,                           // indisprimary
				strings.Join(index.Columns, ","),    // indexdef (simplified)
			}
			rows = append(rows, row)
		}
	}
	columns := []types.ColumnInfo{
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "indexname", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "columnnames", Type: types.ColumnTypeText},
		{Name: "unique", Type: types.ColumnTypeBoolean},
		{Name: "indextype", Type: types.ColumnTypeText},
		{Name: "tablespace", Type: types.ColumnTypeText},
		{Name: "indisprimary", Type: types.ColumnTypeBoolean},
		{Name: "indexdef", Type: types.ColumnTypeText},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}