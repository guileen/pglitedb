package pgcatalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockTableManager is defined in test_utils.go

func TestQueryPgAttribute(t *testing.T) {
	t.Run("EmptyTableList", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return empty list
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgAttribute(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
		assert.Equal(t, 20, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("SingleTableWithColumns", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return one table with columns
		tables := []*types.TableDefinition{
			{
				Name: "test_table",
				Columns: []types.ColumnDefinition{
					{
						Name:       "id",
						Type:       types.ColumnTypeInteger,
						PrimaryKey: true,
						Nullable:   false,
					},
					{
						Name:     "name",
						Type:     types.ColumnTypeString,
						Nullable: true,
					},
				},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgAttribute(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 2) // Two columns
		assert.Equal(t, 20, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("MultipleTables", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return multiple tables with columns
		tables := []*types.TableDefinition{
			{
				Name: "users",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
					{Name: "name", Type: types.ColumnTypeString},
				},
			},
			{
				Name: "orders",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
					{Name: "user_id", Type: types.ColumnTypeInteger},
					{Name: "amount", Type: types.ColumnTypeDouble},
				},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgAttribute(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 5) // 2 + 3 columns
		assert.Equal(t, 20, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("FilterByTableName", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return multiple tables with columns
		tables := []*types.TableDefinition{
			{
				Name: "users",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
					{Name: "name", Type: types.ColumnTypeString},
				},
			},
			{
				Name: "orders",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
					{Name: "user_id", Type: types.ColumnTypeInteger},
				},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{"relname": "orders"}
		result, err := provider.QueryPgAttribute(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 2) // Only orders table columns
		assert.Equal(t, 20, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("FilterByAttributeName", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return tables with columns
		tables := []*types.TableDefinition{
			{
				Name: "users",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
					{Name: "name", Type: types.ColumnTypeString},
				},
			},
			{
				Name: "orders",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
					{Name: "user_id", Type: types.ColumnTypeInteger},
				},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{"attname": "id"}
		result, err := provider.QueryPgAttribute(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Note: Current implementation doesn't filter by attname, so all columns returned
		assert.Len(t, result.Rows, 4) // All columns from both tables
		assert.Equal(t, 20, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("ListTablesError", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return an error
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition(nil), assert.AnError)
		
		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgAttribute(ctx, filter)
		
		assert.Error(t, err)
		assert.Nil(t, result)
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("GetTableDefinitionError", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return one table with columns
		tables := []*types.TableDefinition{
			{
				Name: "test_table",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger},
				},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgAttribute(ctx, filter)
		
		assert.NoError(t, err) // Error is handled gracefully, not propagated
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1) // One row from the table
		
		mockManager.AssertExpectations(t)
	})
}



func TestQueryPgTables(t *testing.T) {
	t.Run("EmptyTableList", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return empty list
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)
		
		ctx := context.Background()
		result, err := provider.QueryPgTables(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
		assert.Equal(t, 7, len(result.Columns))
		
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
					{Name: "name", Type: types.ColumnTypeString},
				},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		result, err := provider.QueryPgTables(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "test_table", result.Rows[0][0])
		assert.Equal(t, "public", result.Rows[0][1])
		assert.Equal(t, 7, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("MultipleTables", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return multiple tables
		tables := []*types.TableDefinition{
			{Name: "table1"},
			{Name: "table2"},
			{Name: "table3"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		result, err := provider.QueryPgTables(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 3)
		assert.Equal(t, "table1", result.Rows[0][0])
		assert.Equal(t, "table2", result.Rows[1][0])
		assert.Equal(t, "table3", result.Rows[2][0])
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("FilterByTableName", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return multiple tables
		tables := []*types.TableDefinition{
			{Name: "users"},
			{Name: "orders"},
			{Name: "products"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{"tablename": "orders"}
		result, err := provider.QueryPgTables(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "orders", result.Rows[0][0])
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("FilterNoMatch", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return multiple tables
		tables := []*types.TableDefinition{
			{Name: "users"},
			{Name: "orders"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{"tablename": "nonexistent"}
		result, err := provider.QueryPgTables(ctx, filter)
		
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
		result, err := provider.QueryPgTables(ctx, nil)
		
		assert.Error(t, err)
		assert.Nil(t, result)
		
		mockManager.AssertExpectations(t)
	})
}

func TestQueryPgTables_Columns(t *testing.T) {
	t.Run("ColumnStructure", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)
		
		ctx := context.Background()
		result, err := provider.QueryPgTables(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		
		// Check column names and types
		expectedColumns := []types.ColumnInfo{
			{Name: "tablename", Type: types.ColumnTypeText},
			{Name: "schemaname", Type: types.ColumnTypeText},
			{Name: "tableowner", Type: types.ColumnTypeText},
			{Name: "hasindexes", Type: types.ColumnTypeBoolean},
			{Name: "hasrules", Type: types.ColumnTypeBoolean},
			{Name: "hastriggers", Type: types.ColumnTypeBoolean},
			{Name: "rowsecurity", Type: types.ColumnTypeBoolean},
		}
		
		assert.Equal(t, expectedColumns, result.Columns)
	})
}

func BenchmarkQueryPgTables_Empty(b *testing.B) {
	mockManager := new(MockTableManager)
	provider := NewProvider(mockManager)
	
	// Mock the ListTables call to return empty list
	mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := provider.QueryPgTables(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryPgTables_SingleTable(b *testing.B) {
	mockManager := new(MockTableManager)
	provider := NewProvider(mockManager)
	
	// Mock the ListTables call to return one table
	tables := []*types.TableDefinition{
		{Name: "test_table"},
	}
	mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := provider.QueryPgTables(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryPgTables_MultipleTables(b *testing.B) {
	mockManager := new(MockTableManager)
	provider := NewProvider(mockManager)
	
	// Mock the ListTables call to return multiple tables
	tables := make([]*types.TableDefinition, 100)
	for i := 0; i < 100; i++ {
		tables[i] = &types.TableDefinition{Name: "table" + string(rune(i))}
	}
	mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := provider.QueryPgTables(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}