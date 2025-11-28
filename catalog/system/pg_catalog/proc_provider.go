package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/types"
)

// QueryPgProc implements the pg_proc query
func (p *Provider) QueryPgProc(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// For now, return an empty result as we don't have function metadata
	rows := make([][]interface{}, 0)
	columns := []types.ColumnInfo{
		{Name: "proname", Type: types.ColumnTypeString},
		{Name: "pronamespace", Type: types.ColumnTypeBigInt},
		{Name: "proowner", Type: types.ColumnTypeBigInt},
		{Name: "prolang", Type: types.ColumnTypeBigInt},
		{Name: "procost", Type: types.ColumnTypeReal},
		{Name: "prorows", Type: types.ColumnTypeReal},
		{Name: "provariadic", Type: types.ColumnTypeBigInt},
		{Name: "prosupport", Type: types.ColumnTypeBigInt},
		{Name: "prokind", Type: types.ColumnTypeChar},
		{Name: "prosecdef", Type: types.ColumnTypeBoolean},
		{Name: "proleakproof", Type: types.ColumnTypeBoolean},
		{Name: "proisstrict", Type: types.ColumnTypeBoolean},
		{Name: "proretset", Type: types.ColumnTypeBoolean},
		{Name: "provolatile", Type: types.ColumnTypeChar},
		{Name: "proparallel", Type: types.ColumnTypeChar},
		{Name: "pronargs", Type: types.ColumnTypeSmallInt},
		{Name: "pronargdefaults", Type: types.ColumnTypeSmallInt},
		{Name: "prorettype", Type: types.ColumnTypeBigInt},
		{Name: "proargtypes", Type: types.ColumnTypeText},
		{Name: "proallargtypes", Type: types.ColumnTypeText},
		{Name: "proargmodes", Type: types.ColumnTypeText},
		{Name: "proargnames", Type: types.ColumnTypeText},
		{Name: "proargdefaults", Type: types.ColumnTypeText},
		{Name: "protrftypes", Type: types.ColumnTypeText},
		{Name: "prosrc", Type: types.ColumnTypeText},
		{Name: "probin", Type: types.ColumnTypeText},
		{Name: "proconfig", Type: types.ColumnTypeText},
		{Name: "proacl", Type: types.ColumnTypeText},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}