package pgcatalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/catalog/system/oid"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQueryPgClass(t *testing.T) {
	t.Run("EmptyTableList", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return empty list
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
		assert.Equal(t, 29, len(result.Columns))

		mockManager.AssertExpectations(t)
	})

	t.Run("SingleTable", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return one table
		tables := []*types.TableDefinition{
			{
				Name: "test_table",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
					{Name: "name", Type: types.ColumnTypeText},
				},
				Indexes: []types.IndexDefinition{},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		
		// Check the values
		row := result.Rows[0]
		tableOID := oid.GenerateTableOID("test_table")
		reltypeOID := oid.GenerateTypeOID("table_test_table")
		
		assert.Equal(t, tableOID, row[0])        // oid
		assert.Equal(t, "test_table", row[1])    // relname
		assert.Equal(t, int64(2200), row[2])     // relnamespace
		assert.Equal(t, reltypeOID, row[3])      // reltype
		assert.Equal(t, int64(0), row[4])        // reloftype
		assert.Equal(t, int64(10), row[5])       // relowner
		assert.Equal(t, int64(0), row[6])        // relam
		assert.Equal(t, tableOID, row[7])        // relfilenode
		assert.Equal(t, int64(0), row[8])        // reltablespace
		assert.Equal(t, int64(2), row[9])        // relpages
		assert.Equal(t, float32(0.0), row[10])   // reltuples
		assert.Equal(t, int64(0), row[11])       // relallvisible
		assert.Equal(t, int64(0), row[12])       // reltoastrelid
		assert.Equal(t, false, row[13])          // relhasindex
		assert.Equal(t, false, row[14])          // relisshared
		assert.Equal(t, "r", row[15])            // relkind
		assert.Equal(t, int16(2), row[16])       // relnatts
		assert.Equal(t, int16(0), row[17])       // relchecks
		assert.Equal(t, false, row[18])          // relhasrules
		assert.Equal(t, false, row[19])          // relhastriggers
		assert.Equal(t, false, row[20])          // relhassubclass
		assert.Equal(t, false, row[21])          // relrowsecurity
		assert.Equal(t, false, row[22])          // relforcerowsecurity
		assert.Equal(t, true, row[23])           // relispopulated
		assert.Equal(t, "d", row[24])            // relreplident
		assert.Equal(t, false, row[25])          // relispartition
		assert.Equal(t, int64(0), row[26])       // relrewrite
		assert.Equal(t, int64(0), row[27])       // relfrozenxid
		assert.Equal(t, int64(0), row[28])       // relminmxid

		assert.Equal(t, 29, len(result.Columns))

		mockManager.AssertExpectations(t)
	})

	t.Run("TableWithIndexes", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return one table with indexes
		tables := []*types.TableDefinition{
			{
				Name: "indexed_table",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
				},
				Indexes: []types.IndexDefinition{
					{Name: "idx_id", Columns: []string{"id"}},
				},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		
		// Check that relhasindex is true
		row := result.Rows[0]
		assert.Equal(t, true, row[13]) // relhasindex

		mockManager.AssertExpectations(t)
	})

	t.Run("FilterByRelNameMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return multiple tables
		tables := []*types.TableDefinition{
			{Name: "users"},
			{Name: "orders"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		ctx := context.Background()
		filter := map[string]interface{}{"relname": "orders"}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "orders", result.Rows[0][1]) // relname

		mockManager.AssertExpectations(t)
	})

	t.Run("FilterByRelNameNoMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return multiple tables
		tables := []*types.TableDefinition{
			{Name: "users"},
			{Name: "orders"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		ctx := context.Background()
		filter := map[string]interface{}{"relname": "nonexistent"}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)

		mockManager.AssertExpectations(t)
	})

	t.Run("FilterByOIDMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return one table
		tables := []*types.TableDefinition{
			{Name: "test_table"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		tableOID := oid.GenerateTableOID("test_table")
		ctx := context.Background()
		filter := map[string]interface{}{"oid": tableOID}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, tableOID, result.Rows[0][0]) // oid

		mockManager.AssertExpectations(t)
	})

	t.Run("FilterByOIDNoMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return one table
		tables := []*types.TableDefinition{
			{Name: "test_table"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		ctx := context.Background()
		filter := map[string]interface{}{"oid": int64(999999)}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)

		mockManager.AssertExpectations(t)
	})

	t.Run("ListTablesError", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return an error
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition(nil), assert.AnError)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.Error(t, err)
		assert.Nil(t, result)

		mockManager.AssertExpectations(t)
	})
}

func TestQueryPgClass_Columns(t *testing.T) {
	t.Run("ColumnStructure", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgClass(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check column names and types
		expectedColumns := []types.ColumnInfo{
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

		assert.Equal(t, expectedColumns, result.Columns)
	})
}