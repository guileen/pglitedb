package pgcatalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/catalog/system/oid"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

func TestQueryPgNamespace(t *testing.T) {
	t.Run("DefaultNamespaces", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgNamespace(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 3) // Should have 3 default namespaces
		assert.Equal(t, 4, len(result.Columns))

		// Check the namespace entries
		publicOID := oid.GenerateNamespaceOID("public")
		pgCatalogOID := oid.GenerateNamespaceOID("pg_catalog")
		infoSchemaOID := oid.GenerateNamespaceOID("information_schema")

		// Find the rows (order might vary)
		oids := make(map[int64][]interface{})
		for _, row := range result.Rows {
			oids[row[0].(int64)] = row
		}

		// Check public namespace
		publicRow, exists := oids[publicOID]
		assert.True(t, exists)
		assert.Equal(t, publicOID, publicRow[0])
		assert.Equal(t, "public", publicRow[1])
		assert.Equal(t, int64(0), publicRow[2])
		assert.Equal(t, int64(0), publicRow[3]) // nspacl is int64(0) in implementation

		// Check pg_catalog namespace
		pgCatalogRow, exists := oids[pgCatalogOID]
		assert.True(t, exists)
		assert.Equal(t, pgCatalogOID, pgCatalogRow[0])
		assert.Equal(t, "pg_catalog", pgCatalogRow[1])

		// Check information_schema namespace
		infoSchemaRow, exists := oids[infoSchemaOID]
		assert.True(t, exists)
		assert.Equal(t, infoSchemaOID, infoSchemaRow[0])
		assert.Equal(t, "information_schema", infoSchemaRow[1])
	})

	t.Run("FilterByNspNameMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{"nspname": "public"}
		result, err := provider.QueryPgNamespace(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "public", result.Rows[0][1])

		// Check column structure
		assert.Equal(t, 4, len(result.Columns))
	})

	t.Run("FilterByNspNameNoMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{"nspname": "nonexistent"}
		result, err := provider.QueryPgNamespace(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})

	t.Run("FilterByOIDMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		publicOID := oid.GenerateNamespaceOID("public")
		ctx := context.Background()
		filter := map[string]interface{}{"oid": publicOID}
		result, err := provider.QueryPgNamespace(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, publicOID, result.Rows[0][0])
	})

	t.Run("FilterByOIDNoMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{"oid": int64(999999)}
		result, err := provider.QueryPgNamespace(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})

	t.Run("ComplexFilterBothMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		publicOID := oid.GenerateNamespaceOID("public")
		ctx := context.Background()
		filter := map[string]interface{}{
			"nspname": "public",
			"oid":     publicOID,
		}
		result, err := provider.QueryPgNamespace(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "public", result.Rows[0][1])
		assert.Equal(t, publicOID, result.Rows[0][0])
	})

	t.Run("ComplexFilterOneMismatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{
			"nspname": "public",
			"oid":     int64(999999),
		}
		result, err := provider.QueryPgNamespace(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
	})
}

func TestQueryPgNamespace_Columns(t *testing.T) {
	t.Run("ColumnStructure", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgNamespace(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check column names and types
		expectedColumns := []types.ColumnInfo{
			{Name: "oid", Type: types.ColumnTypeBigInt},
			{Name: "nspname", Type: types.ColumnTypeString},
			{Name: "nspowner", Type: types.ColumnTypeBigInt},
			{Name: "nspacl", Type: types.ColumnTypeText},
		}

		assert.Equal(t, expectedColumns, result.Columns)
	})
}