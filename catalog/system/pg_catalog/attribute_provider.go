package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/catalog/system/oid"
	"github.com/guileen/pglitedb/types"
)

// QueryPgAttribute implements the pg_attribute query
func (p *Provider) QueryPgAttribute(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		// Generate a deterministic OID for the table based on its name - consistent with pg_class
		tableOID := oid.GenerateTableOID(table.Name)
		
		// Ensure we have a valid schema definition
		if table.Schema == "" {
			table.Schema = "public"
		}
		
		// Check filters
		if filterRelname, ok := filter["relname"].(string); ok {
			if table.Name != filterRelname {
				continue
			}
		}
		for i, column := range table.Columns {
			// Check filters
			if filterAttnum, ok := filter["attnum"].(int64); ok {
				if int64(i+1) != filterAttnum {
					continue
				}
			}
			// Map column type to OID
			typeOID := int64(0)
			switch column.Type {
			case types.ColumnTypeString, types.ColumnTypeText, types.ColumnTypeVarchar:
				typeOID = 25 // text
			case types.ColumnTypeInteger:
				typeOID = 23 // int4
			case types.ColumnTypeBigInt:
				typeOID = 20 // int8
			case types.ColumnTypeSmallInt:
				typeOID = 21 // int2
			case types.ColumnTypeBoolean:
				typeOID = 16 // bool
			case types.ColumnTypeReal:
				typeOID = 700 // float4
			case types.ColumnTypeDouble:
				typeOID = 701 // float8
			default:
				typeOID = 25 // text as default
			}
			row := []interface{}{
				tableOID,                // attrelid (table OID - references pg_class.oid)
				column.Name,             // attname
				typeOID,                 // atttypid (type OID - references pg_type.oid)
				int16(-1),               // attlen (-1 for varlena types)
				int16(i + 1),            // attnum
				int16(-1),               // attcacheoff (placeholder)
				int16(-1),               // atttypmod (placeholder)
				int16(0),                // attndims (placeholder)
				int16(-1),               // attstattarget (placeholder)
				"",                      // atttypdefault (placeholder)
				true,                    // attislocal
				int16(0),                // attinhcount (placeholder)
				!column.Nullable,        // attnotnull
				"",                      // attidentity (placeholder)
				"",                      // attgenerated (placeholder)
				int64(0),                // attcollation (placeholder)
				nil,                     // attacl (placeholder)
				nil,                     // attoptions (placeholder)
				nil,                     // attfdwoptions (placeholder)
				nil,                     // attmissingval (placeholder)
			}
			rows = append(rows, row)
		}
	}
	columns := []types.ColumnInfo{
		{Name: "attrelid", Type: types.ColumnTypeBigInt},
		{Name: "attname", Type: types.ColumnTypeText},
		{Name: "atttypid", Type: types.ColumnTypeBigInt},
		{Name: "attlen", Type: types.ColumnTypeSmallInt},
		{Name: "attnum", Type: types.ColumnTypeSmallInt},
		{Name: "attcacheoff", Type: types.ColumnTypeSmallInt},
		{Name: "atttypmod", Type: types.ColumnTypeInteger},
		{Name: "attndims", Type: types.ColumnTypeSmallInt},
		{Name: "attstattarget", Type: types.ColumnTypeSmallInt},
		{Name: "atttypdefault", Type: types.ColumnTypeText},
		{Name: "attislocal", Type: types.ColumnTypeBoolean},
		{Name: "attinhcount", Type: types.ColumnTypeSmallInt},
		{Name: "attnotnull", Type: types.ColumnTypeBoolean},
		{Name: "attidentity", Type: types.ColumnTypeChar},
		{Name: "attgenerated", Type: types.ColumnTypeChar},
		{Name: "attcollation", Type: types.ColumnTypeBigInt},
		{Name: "attacl", Type: types.ColumnTypeText},
		{Name: "attoptions", Type: types.ColumnTypeText},
		{Name: "attfdwoptions", Type: types.ColumnTypeText},
		{Name: "attmissingval", Type: types.ColumnTypeText},
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}