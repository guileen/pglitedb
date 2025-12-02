package pgcatalog

import (
	"context"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
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