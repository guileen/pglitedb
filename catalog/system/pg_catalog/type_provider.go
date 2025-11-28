package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/catalog/system/oid"
	"github.com/guileen/pglitedb/catalog/system/pg_catalog/utils"
	"github.com/guileen/pglitedb/types"
)

// QueryPgType implements the pg_type query
func (p *Provider) QueryPgType(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	// Return a comprehensive implementation with common PostgreSQL types
	rows := make([][]interface{}, 0)
	// Add comprehensive types mapping
	typesList := []struct {
		name       string
		oid        int64
		namespace  int64
		length     int16
		byval      bool
		typetype   string
		category   string
		align      string
		storage    string
	}{
		// Numeric types
		{"int2", 21, 11, 2, true, "b", "N", "s", "p"},
		{"int4", 23, 11, 4, true, "b", "N", "i", "p"},
		{"int8", 20, 11, 8, true, "b", "N", "d", "p"},
		{"float4", 700, 11, 4, true, "b", "N", "i", "p"},
		{"float8", 701, 11, 8, true, "b", "N", "d", "p"},
		{"numeric", 1700, 11, -1, false, "b", "N", "i", "m"},
		// Text types
		{"text", 25, 11, -1, false, "b", "S", "i", "x"},
		{"varchar", 1043, 11, -1, false, "b", "S", "i", "x"},
		{"char", 18, 11, -1, false, "b", "S", "i", "x"},
		{"name", 19, 11, 64, false, "b", "S", "i", "p"},
		{"bpchar", 1042, 11, -1, false, "b", "S", "i", "x"},
		// Boolean type
		{"bool", 16, 11, 1, true, "b", "B", "c", "p"},
		// Date/time types
		{"date", 1082, 11, 4, true, "b", "D", "i", "p"},
		{"time", 1083, 11, 8, true, "b", "D", "d", "p"},
		{"timestamp", 1114, 11, 8, true, "b", "D", "d", "p"},
		{"timestamptz", 1184, 11, 8, true, "b", "D", "d", "p"},
		// Network types
		{"inet", 869, 11, -1, false, "b", "I", "i", "m"},
		{"cidr", 650, 11, -1, false, "b", "I", "i", "m"},
		// Object identifier types
		{"oid", 26, 11, 4, true, "b", "O", "i", "p"},
		{"xid", 28, 11, 4, true, "b", "O", "i", "p"},
		// JSON types
		{"json", 114, 11, -1, false, "b", "U", "i", "x"},
		{"jsonb", 3802, 11, -1, false, "b", "U", "i", "x"},
		// UUID type
		{"uuid", 2950, 11, 16, false, "b", "U", "i", "p"},
		// Bytea type
		{"bytea", 17, 11, -1, false, "b", "U", "i", "x"},
	}
	// Add table-specific types that reference pg_class
	tables, err := p.manager.ListTables(ctx, 1)
	if err == nil {
		for _, table := range tables {
			tableTypeName := "table_" + table.Name
			tableTypeOID := oid.GenerateTypeOID(tableTypeName)
			typeEntry := struct {
				name       string
				oid        int64
				namespace  int64
				length     int16
				byval      bool
				typetype   string
				category   string
				align      string
				storage    string
			}{
				tableTypeName,
				tableTypeOID,
				2200, // public namespace
				-1,
				false,
				"c", // composite type
				"C", // composite category
				"d", // double alignment
				"x", // extended storage
			}
			typesList = append(typesList, typeEntry)
		}
	}
	for _, t := range typesList {
		// Check filters with enhanced matching
		if !utils.MatchSystemTableFilter(filter, "typname", t.name) {
			continue
		}
		if !utils.MatchSystemTableFilter(filter, "oid", t.oid) {
			continue
		}
		row := []interface{}{
			t.oid,        // oid
			t.name,       // typname
			t.namespace,  // typnamespace
			int64(10),    // typowner (placeholder)
			t.length,     // typlen
			t.byval,      // typbyval
			t.typetype,   // typtype
			t.category,   // typcategory
			",",          // typdelim
			int64(0),     // typrelid (placeholder)
			int64(0),     // typelem (placeholder)
			int64(0),     // typarray (placeholder)
			t.align,      // typalign
			t.storage,    // typstorage
			false,        // typnotnull
			int64(0),     // typbasetype (placeholder)
			int32(-1),    // typtypmod (placeholder)
			int32(0),     // typndims (placeholder)
			int64(0),     // typcollation (placeholder)
			nil,          // typdefaultbin (placeholder)
			nil,          // typdefault (placeholder)
			nil,          // typacl (placeholder)
		}
		rows = append(rows, row)
	}
	columns := []types.ColumnInfo{
		{Name: "oid", Type: types.ColumnTypeBigInt},
		{Name: "typname", Type: types.ColumnTypeText},
		{Name: "typnamespace", Type: types.ColumnTypeBigInt},
		{Name: "typowner", Type: types.ColumnTypeBigInt},
		{Name: "typlen", Type: types.ColumnTypeSmallInt},
		{Name: "typbyval", Type: types.ColumnTypeBoolean},
		{Name: "typtype", Type: types.ColumnTypeChar},
		{Name: "typcategory", Type: types.ColumnTypeChar},
		{Name: "typdelim", Type: types.ColumnTypeChar},
		{Name: "typrelid", Type: types.ColumnTypeBigInt},
		{Name: "typelem", Type: types.ColumnTypeBigInt},
		{Name: "typarray", Type: types.ColumnTypeBigInt},
		{Name: "typinput", Type: types.ColumnTypeBigInt},
		{Name: "typoutput", Type: types.ColumnTypeBigInt},
		{Name: "typreceive", Type: types.ColumnTypeBigInt},
		{Name: "typsend", Type: types.ColumnTypeBigInt},
		{Name: "typmodin", Type: types.ColumnTypeBigInt},
		{Name: "typmodout", Type: types.ColumnTypeBigInt},
		{Name: "typanalyze", Type: types.ColumnTypeBigInt},
		{Name: "typalign", Type: types.ColumnTypeChar},
		{Name: "typstorage", Type: types.ColumnTypeChar},
		{Name: "typnotnull", Type: types.ColumnTypeBoolean},
		{Name: "typbasetype", Type: types.ColumnTypeBigInt},
		{Name: "typtypmod", Type: types.ColumnTypeInteger},
		{Name: "typndims", Type: types.ColumnTypeInteger},
		{Name: "typcollation", Type: types.ColumnTypeBigInt},
		{Name: "typdefaultbin", Type: types.ColumnTypeText},
		{Name: "typdefault", Type: types.ColumnTypeText},
		{Name: "typacl", Type: types.ColumnTypeText},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}