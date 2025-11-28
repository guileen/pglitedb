package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/types"
)

// QueryPgInherits implements the pg_inherits query
func (p *Provider) QueryPgInherits(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// For now, we'll return an empty result since we don't have table inheritance
	// In a full implementation, we would query actual inheritance relationships
	rows := make([][]interface{}, 0)
	columns := []types.ColumnInfo{
		{Name: "inhrelid", Type: types.ColumnTypeBigInt},
		{Name: "inhparent", Type: types.ColumnTypeBigInt},
		{Name: "inhseqno", Type: types.ColumnTypeInteger},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}