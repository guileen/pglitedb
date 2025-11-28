package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/catalog/system/oid"
	"github.com/guileen/pglitedb/catalog/system/pg_catalog/utils"
	"github.com/guileen/pglitedb/types"
)

// QueryPgNamespace implements the pg_namespace query
func (p *Provider) QueryPgNamespace(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// Return implementation with common namespaces using deterministic OIDs
	rows := make([][]interface{}, 0)
	// Add standard namespaces with deterministic OIDs
	namespaces := []struct {
		name string
		oid  int64
	}{
		{"pg_catalog", oid.GenerateNamespaceOID("pg_catalog")},
		{"public", oid.GenerateNamespaceOID("public")},
		{"information_schema", oid.GenerateNamespaceOID("information_schema")},
	}
	for _, ns := range namespaces {
		// Check filters with enhanced matching
		if !utils.MatchSystemTableFilter(filter, "nspname", ns.name) {
			continue
		}
		if !utils.MatchSystemTableFilter(filter, "oid", ns.oid) {
			continue
		}
		row := []interface{}{
			ns.oid,                  // oid
			ns.name,                 // nspname
			int64(0),                // nspowner (placeholder)
			int64(0),                // nspacl (placeholder)
		}
		rows = append(rows, row)
	}
	columns := []types.ColumnInfo{
		{Name: "oid", Type: types.ColumnTypeBigInt},
		{Name: "nspname", Type: types.ColumnTypeString},
		{Name: "nspowner", Type: types.ColumnTypeBigInt},
		{Name: "nspacl", Type: types.ColumnTypeText},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}