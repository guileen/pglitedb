package pgcatalog

import (
	"context"
	"strings"
	"github.com/guileen/pglitedb/types"
)

// QueryPgConstraint implements the pg_constraint query
func (p *Provider) QueryPgConstraint(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
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
		// Add row for each constraint
		for _, constraint := range tableSchema.Constraints {
			// Prepare foreign key information
			var foreignTable, foreignColumns interface{}
			if constraint.Reference != nil {
				foreignTable = constraint.Reference.Table
				foreignColumns = strings.Join(constraint.Reference.Columns, ",")
			}
			// Map constraint type to PostgreSQL format
			contype := ""
			switch constraint.Type {
			case "primary_key":
				contype = "p" // Primary key
			case "unique":
				contype = "u" // Unique
			case "check":
				contype = "c" // Check
			case "foreign_key":
				contype = "f" // Foreign key
			default:
				contype = "x" // Other
			}
			row := []interface{}{
				constraint.Name,              // conname
				table.Name,                   // tablename
				"public",                     // schemaname
				contype,                      // contype
				strings.Join(constraint.Columns, ","), // columnnames
				constraint.CheckExpression,   // check_expr
				foreignTable,                 // foreign_table
				foreignColumns,               // foreign_columns
				constraint.Type,              // contypename (human-readable)
			}
			rows = append(rows, row)
		}
	}
	columns := []types.ColumnInfo{
		{Name: "conname", Type: types.ColumnTypeText},
		{Name: "tablename", Type: types.ColumnTypeText},
		{Name: "schemaname", Type: types.ColumnTypeText},
		{Name: "contype", Type: types.ColumnTypeText},
		{Name: "columnnames", Type: types.ColumnTypeText},
		{Name: "check_expr", Type: types.ColumnTypeText},
		{Name: "foreign_table", Type: types.ColumnTypeText},
		{Name: "foreign_columns", Type: types.ColumnTypeText},
		{Name: "contypename", Type: types.ColumnTypeText},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}