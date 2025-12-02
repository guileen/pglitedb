package pgcatalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestQueryPgColumns(t *testing.T) {
	t.Run("EmptyTableList", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return empty list
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgColumns(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
		assert.Equal(t, 10, len(result.Columns))

		mockManager.AssertExpectations(t)
	})

	t.Run("SingleTableWithColumns", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return one table
		tables := []*types.TableDefinition{
			{
				Name: "test_table",
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		// Mock the GetTableDefinition call to return table schema
		tableDef := &types.TableDefinition{
			Name: "test_table",
			Columns: []types.ColumnDefinition{
				{
					Name:     "id",
					Type:     types.ColumnTypeInteger,
					Nullable: false,
					Default:  nil,
				},
				{
					Name:     "name",
					Type:     types.ColumnTypeText,
					Nullable: true,
					Default:  nil,
				},
			},
		}
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "test_table").Return(tableDef, nil)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgColumns(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 2)
		assert.Equal(t, "test_table", result.Rows[0][0]) // tablename
		assert.Equal(t, "id", result.Rows[0][1])         // columnname
		assert.Equal(t, "public", result.Rows[0][2])     // schemaname
		assert.Equal(t, "integer", result.Rows[0][3])    // datatype
		assert.Equal(t, "test_table", result.Rows[1][0]) // tablename
		assert.Equal(t, "name", result.Rows[1][1])       // columnname
		assert.Equal(t, "text", result.Rows[1][3])       // datatype
		assert.Equal(t, 10, len(result.Columns))

		mockManager.AssertExpectations(t)
	})

	t.Run("FilterByTableName", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return multiple tables
		tables := []*types.TableDefinition{
			{Name: "users"},
			{Name: "orders"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		// Mock the GetTableDefinition call for users table
		usersTableDef := &types.TableDefinition{
			Name: "users",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger},
				{Name: "name", Type: types.ColumnTypeText},
			},
		}
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "users").Return(usersTableDef, nil)

		// Note: orders table definition is not called because we're filtering by tablename=users

		ctx := context.Background()
		filter := map[string]interface{}{"tablename": "users"}
		result, err := provider.QueryPgColumns(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 2) // Only users table columns
		assert.Equal(t, "users", result.Rows[0][0])
		assert.Equal(t, "users", result.Rows[1][0])

		mockManager.AssertExpectations(t)
	})

	t.Run("GetTableDefinitionError", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return one table
		tables := []*types.TableDefinition{
			{Name: "test_table"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		// Mock the GetTableDefinition call to return an error
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "test_table").Return((*types.TableDefinition)(nil), assert.AnError)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgColumns(ctx, filter)

		assert.NoError(t, err) // Error is handled gracefully
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows) // No rows because GetTableDefinition failed

		mockManager.AssertExpectations(t)
	})

	t.Run("ListTablesError", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return an error
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition(nil), assert.AnError)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgColumns(ctx, filter)

		assert.Error(t, err)
		assert.Nil(t, result)

		mockManager.AssertExpectations(t)
	})
}

func TestQueryPgColumns_Columns(t *testing.T) {
	t.Run("ColumnStructure", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgColumns(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check column names and types
		expectedColumns := []types.ColumnInfo{
			{Name: "tablename", Type: types.ColumnTypeText},
			{Name: "columnname", Type: types.ColumnTypeText},
			{Name: "schemaname", Type: types.ColumnTypeText},
			{Name: "datatype", Type: types.ColumnTypeText},
			{Name: "ordinal_position", Type: types.ColumnTypeInteger},
			{Name: "notnull", Type: types.ColumnTypeText},
			{Name: "column_default", Type: types.ColumnTypeText},
			{Name: "is_primary_key", Type: types.ColumnTypeBoolean},
			{Name: "is_unique", Type: types.ColumnTypeBoolean},
			{Name: "is_serial", Type: types.ColumnTypeBoolean},
		}

		assert.Equal(t, expectedColumns, result.Columns)
	})
}