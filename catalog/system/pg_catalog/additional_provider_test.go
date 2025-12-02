package pgcatalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/catalog/system/interfaces"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// AdditionalMockTableManager is a mock implementation of the TableManager interface for additional tests
type AdditionalMockTableManager struct {
	mock.Mock
}

func (m *AdditionalMockTableManager) ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error) {
	args := m.Called(ctx, tenantID)
	var tables []*types.TableDefinition
	if args.Get(0) != nil {
		tables = args.Get(0).([]*types.TableDefinition)
	}
	return tables, args.Error(1)
}

func (m *AdditionalMockTableManager) GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error) {
	args := m.Called(ctx, tenantID, tableName)
	var tableDef *types.TableDefinition
	if args.Get(0) != nil {
		tableDef = args.Get(0).(*types.TableDefinition)
	}
	return tableDef, args.Error(1)
}

func (m *AdditionalMockTableManager) GetStatsCollector() interfaces.StatsManager {
	args := m.Called()
	var statsManager interfaces.StatsManager
	if args.Get(0) != nil {
		statsManager = args.Get(0).(interfaces.StatsManager)
	}
	return statsManager
}

func (m *AdditionalMockTableManager) QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	args := m.Called(ctx, fullTableName, filter)
	var result *types.QueryResult
	if args.Get(0) != nil {
		result = args.Get(0).(*types.QueryResult)
	}
	return result, args.Error(1)
}

func (m *AdditionalMockTableManager) SystemTableQuery(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	args := m.Called(ctx, fullTableName, filter)
	var result *types.QueryResult
	if args.Get(0) != nil {
		result = args.Get(0).(*types.QueryResult)
	}
	return result, args.Error(1)
}

func (m *AdditionalMockTableManager) GetEngine() engineTypes.StorageEngine {
	args := m.Called()
	var engine engineTypes.StorageEngine
	if args.Get(0) != nil {
		engine = args.Get(0).(engineTypes.StorageEngine)
	}
	return engine
}

// TestProvider_Additional tests additional functionality of the Provider
func TestProvider_Additional(t *testing.T) {
	t.Run("ProviderCreationWithNilManager", func(t *testing.T) {
		// Test provider creation with nil manager (should not panic)
		provider := NewProvider(nil)
		assert.NotNil(t, provider)
		assert.Nil(t, provider.manager)
	})

	t.Run("ProviderCreationWithMockManager", func(t *testing.T) {
		mockManager := new(AdditionalMockTableManager)
		provider := NewProvider(mockManager)
		assert.NotNil(t, provider)
		assert.Equal(t, mockManager, provider.manager)
	})
}

// TestQueryPgAttribute_Additional tests additional scenarios for QueryPgAttribute
func TestQueryPgAttribute_Additional(t *testing.T) {
	t.Run("ComplexTableWithVariousColumnTypes", func(t *testing.T) {
		mockManager := new(AdditionalMockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return a complex table with various column types
		tables := []*types.TableDefinition{
			{
				Name: "complex_table",
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
						Nullable: false,
					},
					{
						Name:     "description",
						Type:     types.ColumnTypeText,
						Nullable: true,
					},
					{
						Name:     "price",
						Type:     types.ColumnTypeDouble,
						Nullable: false,
					},
					{
						Name:     "is_active",
						Type:     types.ColumnTypeBoolean,
						Nullable: false,
					},
					{
						Name:     "created_at",
						Type:     types.ColumnTypeTimestamp,
						Nullable: false,
					},
					{
						Name:     "metadata",
						Type:     types.ColumnTypeJSON,
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
		assert.Len(t, result.Rows, 7) // Seven columns
		assert.Equal(t, 20, len(result.Columns))

		// Verify specific column attributes
		// First column (id) - primary key, not nullable
		assert.Equal(t, "id", result.Rows[0][1])                    // attname
		assert.Equal(t, true, result.Rows[0][12])                   // attnotnull
		assert.Equal(t, int16(1), result.Rows[0][4])                // attnum

		// Second column (name) - not nullable
		assert.Equal(t, "name", result.Rows[1][1])                  // attname
		assert.Equal(t, true, result.Rows[1][12])                   // attnotnull
		assert.Equal(t, int16(2), result.Rows[1][4])                // attnum

		mockManager.AssertExpectations(t)
	})

	t.Run("MultipleComplexTables", func(t *testing.T) {
		mockManager := new(AdditionalMockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return multiple complex tables
		tables := []*types.TableDefinition{
			{
				Name: "users",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
					{Name: "username", Type: types.ColumnTypeString},
					{Name: "email", Type: types.ColumnTypeString},
				},
			},
			{
				Name: "orders",
				Columns: []types.ColumnDefinition{
					{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
					{Name: "user_id", Type: types.ColumnTypeInteger},
					{Name: "product_name", Type: types.ColumnTypeString},
					{Name: "quantity", Type: types.ColumnTypeInteger},
					{Name: "price", Type: types.ColumnTypeDouble},
				},
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		ctx := context.Background()
		filter := map[string]interface{}{}
		result, err := provider.QueryPgAttribute(ctx, filter)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 8) // 3 + 5 columns
		assert.Equal(t, 20, len(result.Columns))

		mockManager.AssertExpectations(t)
	})
}

// TestQueryPgTables_Additional tests additional scenarios for QueryPgTables
func TestQueryPgTables_Additional(t *testing.T) {
	t.Run("TableWithSchema", func(t *testing.T) {
		mockManager := new(AdditionalMockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return a table with schema
		tables := []*types.TableDefinition{
			{
				Name:   "complete_table",
				Schema: "custom_schema",
			},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		ctx := context.Background()
		result, err := provider.QueryPgTables(ctx, nil)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 1)
		assert.Equal(t, "complete_table", result.Rows[0][0])    // tablename
		assert.Equal(t, "public", result.Rows[0][1])            // schemaname (always "public" in current implementation)
		assert.Equal(t, 7, len(result.Columns))

		mockManager.AssertExpectations(t)
	})

	t.Run("MultipleTablesWithDifferentSchemas", func(t *testing.T) {
		mockManager := new(AdditionalMockTableManager)
		provider := NewProvider(mockManager)

		// Mock the ListTables call to return tables with different schemas
		tables := []*types.TableDefinition{
			{Name: "table1", Schema: "public"},
			{Name: "table2", Schema: "private"},
			{Name: "table3", Schema: "admin"},
			{Name: "table4", Schema: "public"},
		}
		mockManager.On("ListTables", mock.Anything, int64(1)).Return(tables, nil)

		ctx := context.Background()
		result, err := provider.QueryPgTables(ctx, nil)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, result.Rows, 4)
		assert.Equal(t, "table1", result.Rows[0][0])
		assert.Equal(t, "public", result.Rows[0][1])  // Always "public" in current implementation
		assert.Equal(t, "table2", result.Rows[1][0])
		assert.Equal(t, "public", result.Rows[1][1])  // Always "public" in current implementation
		assert.Equal(t, "table3", result.Rows[2][0])
		assert.Equal(t, "public", result.Rows[2][1])  // Always "public" in current implementation
		assert.Equal(t, "table4", result.Rows[3][0])
		assert.Equal(t, "public", result.Rows[3][1])  // Always "public" in current implementation

		mockManager.AssertExpectations(t)
	})
}