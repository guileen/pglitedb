package pgcatalog

import (
	"context"
	"fmt"
	"strconv"
	"github.com/guileen/pglitedb/types"
)

// QueryPgIndex implements the pg_index query
func (p *Provider) QueryPgIndex(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	// Iterate through all tables to get their indexes
	for _, table := range tables {
		tableID, err := strconv.ParseUint(table.ID, 10, 64)
		if err != nil {
			// Skip tables with invalid IDs
			continue
		}
		// Process each index for this table
		for i, index := range table.Indexes {
			indexID := tableID + uint64(i+1) // Simple index ID generation
			// Convert column names to column numbers
			indKey := make([]int, len(index.Columns))
			for j, colName := range index.Columns {
				// Find column position in table
				colNum := 0
				for k, col := range table.Columns {
					if col.Name == colName {
						colNum = k + 1 // 1-based indexing
						break
					}
				}
				indKey[j] = colNum
			}
			// Determine if this is a primary key or unique index
			isPrimary := false
			isUnique := index.Unique
			// Check if this index is for a primary key constraint
			for _, constraint := range table.Constraints {
				if constraint.Type == "primary_key" && 
				   len(constraint.Columns) == len(index.Columns) {
					match := true
					for k, col := range constraint.Columns {
						if col != index.Columns[k] {
							match = false
							break
						}
					}
					if match {
						isPrimary = true
						isUnique = true
						break
					}
				}
			}
			row := []interface{}{
				indexID,              // indexrelid
				tableID,              // indrelid
				len(index.Columns),   // indnatts
				isUnique,             // indisunique
				isPrimary,            // indisprimary
				false,                // indisclustered
				true,                 // indisvalid
				fmt.Sprintf("%v", indKey),               // indkey
				"{}",           // indcollation (empty for now)
				"{}",           // indclass (empty for now)
				"{}",              // indoption (empty for now)
				"",                   // indexpred (empty for now)
			}
			rows = append(rows, row)
		}
	}
	columns := []types.ColumnInfo{
		{Name: "indexrelid", Type: types.ColumnTypeBigInt},
		{Name: "indrelid", Type: types.ColumnTypeBigInt},
		{Name: "indnatts", Type: types.ColumnTypeInteger},
		{Name: "indisunique", Type: types.ColumnTypeBoolean},
		{Name: "indisprimary", Type: types.ColumnTypeBoolean},
		{Name: "indisclustered", Type: types.ColumnTypeBoolean},
		{Name: "indisvalid", Type: types.ColumnTypeBoolean},
		{Name: "indkey", Type: types.ColumnTypeText}, // Using Text instead of IntegerArray
		{Name: "indcollation", Type: types.ColumnTypeText}, // Using Text instead of BigIntArray
		{Name: "indclass", Type: types.ColumnTypeText}, // Using Text instead of BigIntArray
		{Name: "indoption", Type: types.ColumnTypeText}, // Using Text instead of IntegerArray
		{Name: "indexpred", Type: types.ColumnTypeText},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}