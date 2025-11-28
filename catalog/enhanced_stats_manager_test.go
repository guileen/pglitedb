package catalog

import (
	"context"
	"testing"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/catalog/system"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/stretchr/testify/assert"
)

// TestEnhancedStatsManager tests the enhanced statistics manager functionality
func TestEnhancedStatsManager(t *testing.T) {
	// Create a simple mock manager for testing
	mockManager := &mockTableManager{}
	
	// Create enhanced stats manager
	statsManager := NewEnhancedStatsManager(interfaces.TableManager(mockManager))
	
	// Test table statistics operations
	tableID := uint64(1)
	tableName := "test_table"
	
	// Create test table statistics
	testTableStats := &system.EnhancedTableStatistics{
		TableID:     tableID,
		TableName:   tableName,
		RowCount:    1000,
		SizeInBytes: 1024000,
		ColumnStats: make(map[string]*system.EnhancedColumnStatistics),
		IndexStats:  make(map[string]*system.EnhancedIndexStatistics),
		LastUpdated: time.Now(),
	}
	
	// Test updating table statistics
	err := statsManager.UpdateTableStatistics(tableID, testTableStats)
	assert.NoError(t, err)
	
	// Test retrieving table statistics
	retrievedStats, err := statsManager.GetTableStatistics(tableID)
	assert.NoError(t, err)
	assert.Equal(t, testTableStats.TableID, retrievedStats.TableID)
	assert.Equal(t, testTableStats.TableName, retrievedStats.TableName)
	assert.Equal(t, testTableStats.RowCount, retrievedStats.RowCount)
	assert.Equal(t, testTableStats.SizeInBytes, retrievedStats.SizeInBytes)
	
	// Test column statistics operations
	columnName := "test_column"
	testColumnStats := &system.EnhancedColumnStatistics{
		TableID:       tableID,
		ColumnName:    columnName,
		DataType:      "VARCHAR",
		DistinctCount: 500,
		NullCount:     10,
		AvgSize:       20,
		Correlation:   0.8,
		LastUpdated:   time.Now(),
	}
	
	// Test updating column statistics
	err = statsManager.UpdateColumnStatistics(tableID, columnName, testColumnStats)
	assert.NoError(t, err)
	
	// Test retrieving column statistics
	retrievedColumnStats, err := statsManager.GetColumnStatistics(tableID, columnName)
	assert.NoError(t, err)
	assert.Equal(t, testColumnStats.TableID, retrievedColumnStats.TableID)
	assert.Equal(t, testColumnStats.ColumnName, retrievedColumnStats.ColumnName)
	assert.Equal(t, testColumnStats.DataType, retrievedColumnStats.DataType)
	assert.Equal(t, testColumnStats.DistinctCount, retrievedColumnStats.DistinctCount)
	assert.Equal(t, testColumnStats.NullCount, retrievedColumnStats.NullCount)
	assert.Equal(t, testColumnStats.AvgSize, retrievedColumnStats.AvgSize)
	assert.Equal(t, testColumnStats.Correlation, retrievedColumnStats.Correlation)
	
	// Test error cases
	_, err = statsManager.GetTableStatistics(999) // Non-existent table
	assert.Error(t, err)
	
	_, err = statsManager.GetColumnStatistics(999, "nonexistent") // Non-existent table
	assert.Error(t, err)
	
	_, err = statsManager.GetColumnStatistics(tableID, "nonexistent") // Non-existent column
	assert.Error(t, err)
}

// TestCollectStatisticsForTable tests the CollectStatisticsForTable method
func TestCollectStatisticsForTable(t *testing.T) {
	// Create a simple mock manager for testing
	mockManager := &mockTableManager{}
	
	// Create enhanced stats manager
	statsManager := NewEnhancedStatsManager(interfaces.TableManager(mockManager))
	
	tableID := uint64(1)
	tableDef := &types.TableDefinition{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger},
			{Name: "name", Type: types.ColumnTypeText},
			{Name: "age", Type: types.ColumnTypeInteger},
		},
	}
	
	// Test collecting statistics for table
	stats, err := statsManager.CollectStatisticsForTable(tableID, tableDef)
	assert.NoError(t, err)
	assert.NotNil(t, stats)
	assert.Equal(t, tableID, stats.TableID)
	assert.Equal(t, tableDef.Name, stats.TableName)
	assert.GreaterOrEqual(t, stats.RowCount, int64(0))
	assert.GreaterOrEqual(t, stats.SizeInBytes, int64(0))
	assert.Len(t, stats.ColumnStats, len(tableDef.Columns))
	assert.NotNil(t, stats.IndexStats)
}

// mockTableManager is a simplified mock implementation for testing
type mockTableManager struct{}

func (m *mockTableManager) ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error) {
	return []*types.TableDefinition{}, nil
}

func (m *mockTableManager) GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error) {
	return &types.TableDefinition{
		Name: tableName,
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger},
			{Name: "name", Type: types.ColumnTypeText},
		},
	}, nil
}

func (m *mockTableManager) ListIndexes(ctx context.Context, tenantID int64, tableName string) ([]*types.IndexDefinition, error) {
	return []*types.IndexDefinition{
		{
			Name: "test_index",
		},
	}, nil
}

func (m *mockTableManager) GetStatsCollector() interfaces.StatsManager {
	return nil
}

func (m *mockTableManager) GetEnhancedStatsManager() system.StatisticsManager {
	return nil
}

func (m *mockTableManager) QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	return &types.QueryResult{}, nil
}

func (m *mockTableManager) SystemTableQuery(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	return &types.QueryResult{}, nil
}

func (m *mockTableManager) GetEngine() engineTypes.StorageEngine {
	return nil
}