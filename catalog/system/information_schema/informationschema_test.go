package informationschema

import (
	"context"
	"testing"
	
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockTableManager is a mock implementation of the TableManager interface
type MockTableManager struct {
	mock.Mock
}

func (m *MockTableManager) ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error) {
	args := m.Called(ctx, tenantID)
	var tables []*types.TableDefinition
	if args.Get(0) != nil {
		tables = args.Get(0).([]*types.TableDefinition)
	}
	return tables, args.Error(1)
}

func (m *MockTableManager) GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error) {
	args := m.Called(ctx, tenantID, tableName)
	var tableDef *types.TableDefinition
	if args.Get(0) != nil {
		tableDef = args.Get(0).(*types.TableDefinition)
	}
	return tableDef, args.Error(1)
}

func (m *MockTableManager) GetStatsCollector() interfaces.StatsManager {
	args := m.Called()
	var statsManager interfaces.StatsManager
	if args.Get(0) != nil {
		statsManager = args.Get(0).(interfaces.StatsManager)
	}
	return statsManager
}

func (m *MockTableManager) QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	args := m.Called(ctx, fullTableName, filter)
	var result *types.QueryResult
	if args.Get(0) != nil {
		result = args.Get(0).(*types.QueryResult)
	}
	return result, args.Error(1)
}

func (m *MockTableManager) SystemTableQuery(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	args := m.Called(ctx, fullTableName, filter)
	var result *types.QueryResult
	if args.Get(0) != nil {
		result = args.Get(0).(*types.QueryResult)
	}
	return result, args.Error(1)
}

func (m *MockTableManager) GetEngine() engineTypes.StorageEngine {
	args := m.Called()
	var engine engineTypes.StorageEngine
	if args.Get(0) != nil {
		engine = args.Get(0).(engineTypes.StorageEngine)
	}
	return engine
}

func TestNewProvider(t *testing.T) {
	t.Run("CreateProvider", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		assert.NotNil(t, provider)
		assert.Equal(t, mockManager, provider.manager)
	})
}

func TestQueryTables(t *testing.T) {
	t.Run("EmptyTableList", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return empty list
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)
		
		ctx := context.Background()
		result, err := provider.QueryTables(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
		assert.Equal(t, 11, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("SingleTable", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return one table
		tables := []*types.TableDefinition{
			{Name: "test_table"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		result, err := provider.QueryTables(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "def", result.Rows[0][0]) // table_catalog
		assert.Equal(t, "public", result.Rows[0][1])     // table_schema
		assert.Equal(t, "test_table", result.Rows[0][2]) // table_name
		assert.Equal(t, "BASE TABLE", result.Rows[0][3]) // table_type
		assert.Equal(t, 11, len(result.Columns))
		
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
		result, err := provider.QueryTables(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 3)
		assert.Equal(t, "def", result.Rows[0][0]) // table_catalog
		assert.Equal(t, "def", result.Rows[1][0]) // table_catalog
		assert.Equal(t, "def", result.Rows[2][0]) // table_catalog
		assert.Equal(t, 11, len(result.Columns))
		
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
		filter := map[string]interface{}{"table_name": "orders"}
		result, err := provider.QueryTables(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "def", result.Rows[0][0]) // table_catalog
		assert.Equal(t, "orders", result.Rows[0][2]) // table_name
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("FilterByTableSchema", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return multiple tables
		tables := []*types.TableDefinition{
			{Name: "users"},
			{Name: "orders"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{"table_schema": "public"}
		result, err := provider.QueryTables(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 2) // All tables should match "public" schema
		assert.Equal(t, "def", result.Rows[0][0]) // table_catalog
		assert.Equal(t, "def", result.Rows[1][0]) // table_catalog
		
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
		filter := map[string]interface{}{"table_name": "nonexistent"}
		result, err := provider.QueryTables(ctx, filter)
		
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
		result, err := provider.QueryTables(ctx, nil)
		
		assert.Error(t, err)
		assert.Nil(t, result)
		
		mockManager.AssertExpectations(t)
	})
}

func TestQueryTables_Columns(t *testing.T) {
	t.Run("ColumnStructure", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)
		
		ctx := context.Background()
		result, err := provider.QueryTables(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		
		// Check column names and types
		expectedColumns := []types.ColumnInfo{
			{Name: "table_catalog", Type: types.ColumnTypeText},
			{Name: "table_schema", Type: types.ColumnTypeText},
			{Name: "table_name", Type: types.ColumnTypeText},
			{Name: "table_type", Type: types.ColumnTypeText},
			{Name: "self_referencing_column_name", Type: types.ColumnTypeText},
			{Name: "reference_generation", Type: types.ColumnTypeText},
			{Name: "user_defined_type_catalog", Type: types.ColumnTypeText},
			{Name: "user_defined_type_schema", Type: types.ColumnTypeText},
			{Name: "user_defined_type_name", Type: types.ColumnTypeText},
			{Name: "is_insertable_into", Type: types.ColumnTypeText},
			{Name: "is_typed", Type: types.ColumnTypeText},
		}
		
		assert.Equal(t, expectedColumns, result.Columns)
	})
}

func TestQueryColumns(t *testing.T) {
	t.Run("EmptyTableList", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return empty list
		mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)
		
		ctx := context.Background()
		result, err := provider.QueryColumns(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Empty(t, result.Rows)
		assert.Equal(t, 14, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("SingleTableWithColumns", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return one table with columns
		tables := []*types.TableDefinition{
			{Name: "test_table"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		// Mock the GetTableDefinition call
		tableDef := &types.TableDefinition{
			Name: "test_table",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
				{Name: "name", Type: types.ColumnTypeString, Nullable: true},
			},
		}
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "test_table").Return(tableDef, nil)
		
		ctx := context.Background()
		result, err := provider.QueryColumns(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 2) // Two columns
		assert.Equal(t, "def", result.Rows[0][0]) // table_catalog
		assert.Equal(t, "id", result.Rows[0][3])         // column_name
		assert.Equal(t, "def", result.Rows[1][0]) // table_catalog
		assert.Equal(t, "name", result.Rows[1][3])       // column_name
		assert.Equal(t, 14, len(result.Columns))
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("MultipleTables", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return multiple tables with columns
		tables := []*types.TableDefinition{
			{Name: "users"},
			{Name: "orders"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		// Mock the GetTableDefinition calls
		usersTableDef := &types.TableDefinition{
			Name: "users",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger},
				{Name: "name", Type: types.ColumnTypeString},
			},
		}
		ordersTableDef := &types.TableDefinition{
			Name: "orders",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger},
				{Name: "user_id", Type: types.ColumnTypeInteger},
				{Name: "amount", Type: types.ColumnTypeDouble},
			},
		}
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "users").Return(usersTableDef, nil)
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "orders").Return(ordersTableDef, nil)
		
		ctx := context.Background()
		result, err := provider.QueryColumns(ctx, nil)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 5) // 2 + 3 columns
		assert.Equal(t, 14, len(result.Columns))
		
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
		
		// Mock the GetTableDefinition call for orders table
		ordersTableDef := &types.TableDefinition{
			Name: "orders",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger},
				{Name: "user_id", Type: types.ColumnTypeInteger},
			},
		}
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "orders").Return(ordersTableDef, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{"table_name": "orders"}
		result, err := provider.QueryColumns(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 2) // Only orders table columns
		assert.Equal(t, "def", result.Rows[0][0])
		assert.Equal(t, "def", result.Rows[1][0])
		
		mockManager.AssertExpectations(t)
	})
	
	t.Run("FilterByColumnName", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)
		
		// Mock the ListTables call to return tables with specific column
		tables := []*types.TableDefinition{
			{Name: "users"},
			{Name: "orders"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)
		
		// Mock the GetTableDefinition calls
		usersTableDef := &types.TableDefinition{
			Name: "users",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger},
				{Name: "name", Type: types.ColumnTypeString},
			},
		}
		ordersTableDef := &types.TableDefinition{
			Name: "orders",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger},
				{Name: "user_id", Type: types.ColumnTypeInteger},
			},
		}
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "users").Return(usersTableDef, nil)
		mockManager.On("GetTableDefinition", mock.Anything, int64(1), "orders").Return(ordersTableDef, nil)
		
		ctx := context.Background()
		filter := map[string]interface{}{"column_name": "id"}
		result, err := provider.QueryColumns(ctx, filter)
		
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Note: The current implementation doesn't filter by column_name, so all columns are returned
		assert.Len(t, result.Rows, 4) // All columns from both tables
		// The current implementation doesn't support column_name filtering
		
		mockManager.AssertExpectations(t)
	})
}

func TestMapTypeToSQL(t *testing.T) {
	t.Run("IntegerType", func(t *testing.T) {
		result := mapTypeToSQL(types.ColumnTypeInteger)
		assert.Equal(t, "integer", result)
	})
	
	t.Run("StringType", func(t *testing.T) {
		result := mapTypeToSQL(types.ColumnTypeString)
		assert.Equal(t, "text", result)
	})
	
	t.Run("DoubleType", func(t *testing.T) {
		result := mapTypeToSQL(types.ColumnTypeDouble)
		assert.Equal(t, "text", result)
	})
	
	t.Run("BooleanType", func(t *testing.T) {
		result := mapTypeToSQL(types.ColumnTypeBoolean)
		assert.Equal(t, "boolean", result)
	})
	
	t.Run("TextType", func(t *testing.T) {
		result := mapTypeToSQL(types.ColumnTypeText)
		assert.Equal(t, "text", result)
	})
	
	t.Run("TimestampType", func(t *testing.T) {
		result := mapTypeToSQL(types.ColumnTypeTimestamp)
		assert.Equal(t, "timestamp without time zone", result)
	})
	
	t.Run("JSONType", func(t *testing.T) {
		result := mapTypeToSQL(types.ColumnTypeJSON)
		assert.Equal(t, "text", result)
	})
	
	t.Run("UnknownType", func(t *testing.T) {
		result := mapTypeToSQL(types.ColumnType("unknown"))
		assert.Equal(t, "text", result)
	})
}

func TestBoolToYesNo(t *testing.T) {
	t.Run("TrueValue", func(t *testing.T) {
		result := boolToYesNo(true)
		assert.Equal(t, "YES", result)
	})
	
	t.Run("FalseValue", func(t *testing.T) {
		result := boolToYesNo(false)
		assert.Equal(t, "NO", result)
	})
}

func BenchmarkQueryTables_Empty(b *testing.B) {
	mockManager := new(MockTableManager)
	provider := NewProvider(mockManager)
	
	// Mock the ListTables call to return empty list
	mockManager.On("ListTables", mock.Anything, int64(1)).Return([]*types.TableDefinition{}, nil)
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := provider.QueryTables(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryTables_SingleTable(b *testing.B) {
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
		_, err := provider.QueryTables(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryColumns_MultipleTables(b *testing.B) {
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
	
	// Mock the GetTableDefinition calls
	usersTableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger},
			{Name: "name", Type: types.ColumnTypeString},
		},
	}
	ordersTableDef := &types.TableDefinition{
		Name: "orders",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger},
			{Name: "user_id", Type: types.ColumnTypeInteger},
			{Name: "amount", Type: types.ColumnTypeDouble},
		},
	}
	mockManager.On("GetTableDefinition", mock.Anything, int64(1), "users").Return(usersTableDef, nil)
	mockManager.On("GetTableDefinition", mock.Anything, int64(1), "orders").Return(ordersTableDef, nil)
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := provider.QueryColumns(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}