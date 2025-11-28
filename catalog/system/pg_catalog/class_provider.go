package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/catalog/system/oid"
	"github.com/guileen/pglitedb/catalog/system/pg_catalog/utils"
	"github.com/guileen/pglitedb/types"
)

// QueryPgClass implements the pg_class query
func (p *Provider) QueryPgClass(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		// Generate a deterministic OID for the table based on its name
		tableOID := oid.GenerateTableOID(table.Name)
		// Generate reltype OID that references pg_type
		reltypeOID := oid.GenerateTypeOID("table_" + table.Name)
		// Check filters with enhanced matching
		if !utils.MatchSystemTableFilter(filter, "relname", table.Name) {
			continue
		}
		// Additional filter checks for other fields
		if !utils.MatchSystemTableFilter(filter, "oid", tableOID) {
			continue
		}
		row := []interface{}{
			tableOID,                // oid
			table.Name,              // relname
			int64(2200),             // relnamespace (public namespace OID)
			reltypeOID,              // reltype (references pg_type)
			int64(0),                // reloftype (placeholder)
			int64(10),               // relowner (placeholder)
			int64(0),                // relam (placeholder)
			tableOID,                // relfilenode (same as OID)
			int64(0),                // reltablespace (placeholder)
			int64(len(table.Columns)), // relpages (approximate)
			float32(0.0),            // reltuples (placeholder)
			int64(0),                // relallvisible (placeholder)
			int64(0),                // reltoastrelid (placeholder)
			len(table.Indexes) > 0,  // relhasindex
			false,                   // relisshared
			"r",                     // relkind (r for regular table)
			int16(len(table.Columns)), // relnatts (number of columns)
			int16(0),                // relchecks (placeholder)
			false,                   // relhasrules
			false,                   // relhastriggers
			false,                   // relhassubclass
			false,                   // relrowsecurity
			false,                   // relforcerowsecurity
			true,                    // relispopulated
			"d",                     // relreplident (default)
			false,                   // relispartition
			int64(0),                // relrewrite (placeholder)
			int64(0),                // relfrozenxid (placeholder)
			int64(0),                // relminmxid (placeholder)
		}
		rows = append(rows, row)
	}
	columns := []types.ColumnInfo{
		{Name: "oid", Type: types.ColumnTypeBigInt},
		{Name: "relname", Type: types.ColumnTypeText},
		{Name: "relnamespace", Type: types.ColumnTypeBigInt},
		{Name: "reltype", Type: types.ColumnTypeBigInt},
		{Name: "reloftype", Type: types.ColumnTypeBigInt},
		{Name: "relowner", Type: types.ColumnTypeBigInt},
		{Name: "relam", Type: types.ColumnTypeBigInt},
		{Name: "relfilenode", Type: types.ColumnTypeBigInt},
		{Name: "reltablespace", Type: types.ColumnTypeBigInt},
		{Name: "relpages", Type: types.ColumnTypeInteger},
		{Name: "reltuples", Type: types.ColumnTypeReal},
		{Name: "relallvisible", Type: types.ColumnTypeInteger},
		{Name: "reltoastrelid", Type: types.ColumnTypeBigInt},
		{Name: "relhasindex", Type: types.ColumnTypeBoolean},
		{Name: "relisshared", Type: types.ColumnTypeBoolean},
		{Name: "relkind", Type: types.ColumnTypeChar},
		{Name: "relnatts", Type: types.ColumnTypeSmallInt},
		{Name: "relchecks", Type: types.ColumnTypeSmallInt},
		{Name: "relhasrules", Type: types.ColumnTypeBoolean},
		{Name: "relhastriggers", Type: types.ColumnTypeBoolean},
		{Name: "relhassubclass", Type: types.ColumnTypeBoolean},
		{Name: "relrowsecurity", Type: types.ColumnTypeBoolean},
		{Name: "relforcerowsecurity", Type: types.ColumnTypeBoolean},
		{Name: "relispopulated", Type: types.ColumnTypeBoolean},
		{Name: "relreplident", Type: types.ColumnTypeChar},
		{Name: "relispartition", Type: types.ColumnTypeBoolean},
		{Name: "relrewrite", Type: types.ColumnTypeBigInt},
		{Name: "relfrozenxid", Type: types.ColumnTypeBigInt},
		{Name: "relminmxid", Type: types.ColumnTypeBigInt},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}