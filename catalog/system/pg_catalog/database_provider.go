package pgcatalog

import (
	"context"
	"github.com/guileen/pglitedb/catalog/system/oid"
	"github.com/guileen/pglitedb/catalog/system/pg_catalog/utils"
	"github.com/guileen/pglitedb/types"
)

// QueryPgDatabase implements the pg_database query
func (p *Provider) QueryPgDatabase(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	rows := make([][]interface{}, 0)
	
	// For now, we'll return a single database entry for PGLiteDB
	databaseOID := oid.GenerateDeterministicOID("pglitedb")
	
	// Check filters
	matchName := utils.MatchSystemTableFilter(filter, "datname", "pglitedb")
	matchOID := utils.MatchSystemTableFilter(filter, "oid", databaseOID)
	
	// Only add row if filters match
	if matchName && matchOID {
		row := []interface{}{
			databaseOID,        // oid
			"pglitedb",         // datname
			int64(10),          // datdba (placeholder)
			int64(6),           // encoding (UTF8)
			"UTF8",             // datcollate
			"UTF8",             // datctype
			int64(0),           // datlocprovider (placeholder)
			false,              // datistemplate
			true,               // datallowconn
			int64(-1),          // datconnlimit
			int64(0),           // datfrozenxid (placeholder)
			int64(0),           // datminmxid (placeholder)
			int64(0),           // dattablespace (placeholder)
			nil,                // datacl (placeholder)
		}
		rows = append(rows, row)
	}
	
	columns := []types.ColumnInfo{
		{Name: "oid", Type: types.ColumnTypeBigInt},
		{Name: "datname", Type: types.ColumnTypeText},
		{Name: "datdba", Type: types.ColumnTypeBigInt},
		{Name: "encoding", Type: types.ColumnTypeInteger},
		{Name: "datcollate", Type: types.ColumnTypeText},
		{Name: "datctype", Type: types.ColumnTypeText},
		{Name: "datlocprovider", Type: types.ColumnTypeChar},
		{Name: "datistemplate", Type: types.ColumnTypeBoolean},
		{Name: "datallowconn", Type: types.ColumnTypeBoolean},
		{Name: "datconnlimit", Type: types.ColumnTypeInteger},
		{Name: "datfrozenxid", Type: types.ColumnTypeBigInt},
		{Name: "datminmxid", Type: types.ColumnTypeBigInt},
		{Name: "dattablespace", Type: types.ColumnTypeBigInt},
		{Name: "datacl", Type: types.ColumnTypeText}, // ACL array, using text as placeholder
	}
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}