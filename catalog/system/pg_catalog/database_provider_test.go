package pgcatalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/catalog/system/oid"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

func TestQueryPgDatabase(t *testing.T) {
	t.Run("DefaultDatabaseEntry", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{}

		result, err := provider.QueryPgDatabase(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, 14, len(result.Columns))

		// Check the database entry values
		row := result.Rows[0]
		expectedOID := oid.GenerateDeterministicOID("pglitedb")
		assert.Equal(t, expectedOID, row[0])     // oid
		assert.Equal(t, "pglitedb", row[1])      // datname
		assert.Equal(t, int64(10), row[2])       // datdba
		assert.Equal(t, int64(6), row[3])        // encoding
		assert.Equal(t, "UTF8", row[4])          // datcollate
		assert.Equal(t, "UTF8", row[5])          // datctype
		assert.Equal(t, int64(0), row[6])        // datlocprovider
		assert.Equal(t, false, row[7])           // datistemplate
		assert.Equal(t, true, row[8])            // datallowconn
		assert.Equal(t, int64(-1), row[9])       // datconnlimit
		assert.Equal(t, int64(0), row[10])       // datfrozenxid
		assert.Equal(t, int64(0), row[11])       // datminmxid
		assert.Equal(t, int64(0), row[12])       // dattablespace
		assert.Equal(t, nil, row[13])            // datacl

		// Check column definitions
		expectedColumns := []types.ColumnInfo{
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
			{Name: "datacl", Type: types.ColumnTypeText},
		}
		assert.Equal(t, expectedColumns, result.Columns)
	})

	t.Run("FilterByNameMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{
			"datname": "pglitedb",
		}

		result, err := provider.QueryPgDatabase(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
	})

	t.Run("FilterByNameNoMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{
			"datname": "nonexistent",
		}

		result, err := provider.QueryPgDatabase(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})

	t.Run("FilterByOIDMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		databaseOID := oid.GenerateDeterministicOID("pglitedb")
		ctx := context.Background()
		filter := map[string]interface{}{
			"oid": databaseOID,
		}

		result, err := provider.QueryPgDatabase(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
	})

	t.Run("FilterByOIDNoMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{
			"oid": int64(999999),
		}

		result, err := provider.QueryPgDatabase(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})

	t.Run("ComplexFilterBothMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		databaseOID := oid.GenerateDeterministicOID("pglitedb")
		ctx := context.Background()
		filter := map[string]interface{}{
			"datname": "pglitedb",
			"oid":     databaseOID,
		}

		result, err := provider.QueryPgDatabase(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
	})

	t.Run("ComplexFilterOneMismatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{
			"datname": "pglitedb",
			"oid":     int64(999999),
		}

		result, err := provider.QueryPgDatabase(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})
}