package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/types"
)

type dataManager struct {
	engine        engine.StorageEngine
	cache         *internal.SchemaCache
	schemaManager SchemaManager
}

func newDataManager(eng engine.StorageEngine, cache *internal.SchemaCache, sm SchemaManager) DataManager {
	return &dataManager{
		engine:        eng,
		cache:         cache,
		schemaManager: sm,
	}
}

func (m *dataManager) Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*types.Record, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err == types.ErrTableNotFound {
		schema = m.inferSchemaFromData(tableName, data)
		if err := m.schemaManager.CreateTable(ctx, tenantID, schema); err != nil {
			return nil, fmt.Errorf("auto-create table: %w", err)
		}
		schema, tableID, err = m.getTableSchema(tenantID, tableName)
	}
	if err != nil {
		return nil, err
	}

	record, err := m.validateAndConvert(data, schema)
	if err != nil {
		return nil, fmt.Errorf("validate data: %w", err)
	}

	record.Table = tableName
	record.CreatedAt = time.Now()
	record.UpdatedAt = time.Now()
	record.Version = 1

	rowID, err := m.engine.InsertRow(ctx, tenantID, tableID, record, schema)
	if err != nil {
		return nil, fmt.Errorf("insert row: %w", err)
	}

	record.ID = strconv.FormatInt(rowID, 10)

	return record, nil
}

func (m *dataManager) InsertBatch(ctx context.Context, tenantID int64, tableName string, rows []map[string]interface{}) ([]*types.Record, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return nil, err
	}

	records := make([]*types.Record, 0, len(rows))

	for _, data := range rows {
		record, err := m.validateAndConvert(data, schema)
		if err != nil {
			return nil, fmt.Errorf("validate data: %w", err)
		}

		record.Table = tableName
		record.CreatedAt = time.Now()
		record.UpdatedAt = time.Now()
		record.Version = 1

		rowID, err := m.engine.InsertRow(ctx, tenantID, tableID, record, schema)
		if err != nil {
			return nil, fmt.Errorf("insert row: %w", err)
		}

		record.ID = strconv.FormatInt(rowID, 10)
		records = append(records, record)
	}

	return records, nil
}

func (m *dataManager) Update(ctx context.Context, tenantID int64, tableName string, rowID int64, data map[string]interface{}) (*types.Record, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return nil, err
	}

	updates := make(map[string]*types.Value)
	for k, v := range data {
		colType, err := m.getColumnType(schema, k)
		if err != nil {
			return nil, err
		}

		val, err := m.convertValue(v, colType)
		if err != nil {
			return nil, fmt.Errorf("convert value for %s: %w", k, err)
		}
		updates[k] = val
	}

	if err := m.engine.UpdateRow(ctx, tenantID, tableID, rowID, updates, schema); err != nil {
		return nil, fmt.Errorf("update row: %w", err)
	}

	return m.Get(ctx, tenantID, tableName, rowID)
}

func (m *dataManager) Delete(ctx context.Context, tenantID int64, tableName string, rowID int64) error {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return err
	}

	if err := m.engine.DeleteRow(ctx, tenantID, tableID, rowID, schema); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	return nil
}

func (m *dataManager) Get(ctx context.Context, tenantID int64, tableName string, rowID int64) (*types.Record, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return nil, err
	}

	record, err := m.engine.GetRow(ctx, tenantID, tableID, rowID, schema)
	if err != nil {
		return nil, fmt.Errorf("get row: %w", err)
	}

	record.ID = strconv.FormatInt(rowID, 10)
	record.Table = tableName

	return record, nil
}

func (m *dataManager) getTableSchema(tenantID int64, tableName string) (*types.TableDefinition, int64, error) {
	key := makeTableKey(tenantID, tableName)
	
	schema, tableID, exists := m.cache.Get(key)
	if !exists {
		return nil, 0, types.ErrTableNotFound
	}

	return schema, tableID, nil
}

func (m *dataManager) validateAndConvert(data map[string]interface{}, schema *types.TableDefinition) (*types.Record, error) {
	record := &types.Record{
		Data: make(map[string]*types.Value),
	}

	for _, col := range schema.Columns {
		val, exists := data[col.Name]

		if !exists {
			if col.Default != nil {
				record.Data[col.Name] = col.Default
				continue
			}
			if !col.Nullable {
				return nil, fmt.Errorf("column %s is required", col.Name)
			}
			record.Data[col.Name] = &types.Value{Data: nil, Type: col.Type}
			continue
		}

		converted, err := m.convertValue(val, col.Type)
		if err != nil {
			return nil, fmt.Errorf("convert column %s: %w", col.Name, err)
		}

		record.Data[col.Name] = converted
	}

	return record, nil
}

func (m *dataManager) convertValue(val interface{}, colType types.ColumnType) (*types.Value, error) {
	if val == nil {
		return &types.Value{Data: nil, Type: colType}, nil
	}

	switch colType {
	case types.ColumnTypeString, types.ColumnTypeText:
		if s, ok := val.(string); ok {
			return &types.Value{Data: s, Type: colType}, nil
		}
		return nil, fmt.Errorf("expected string, got %T", val)

	case types.ColumnTypeUUID:
		switch v := val.(type) {
		case string:
			if v == "" {
				return &types.Value{Data: nil, Type: colType}, nil
			}
			if _, err := uuid.Parse(v); err != nil {
				return nil, fmt.Errorf("invalid UUID string: %w", err)
			}
			return &types.Value{Data: v, Type: colType}, nil
		case []byte:
			if len(v) == 0 {
				return &types.Value{Data: nil, Type: colType}, nil
			}
			if len(v) != 16 {
				return nil, fmt.Errorf("UUID byte slice must be 16 bytes")
			}
			u, err := uuid.FromBytes(v)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID bytes: %w", err)
			}
			return &types.Value{Data: u.String(), Type: colType}, nil
		case uuid.UUID:
			return &types.Value{Data: v.String(), Type: colType}, nil
		default:
			return nil, fmt.Errorf("expected UUID string, []byte, or uuid.UUID, got %T", val)
		}

	case types.ColumnTypeNumber:
		switch v := val.(type) {
		case int:
			return &types.Value{Data: int64(v), Type: colType}, nil
		case int32:
			return &types.Value{Data: int64(v), Type: colType}, nil
		case int64:
			return &types.Value{Data: v, Type: colType}, nil
		case float32:
			return &types.Value{Data: float64(v), Type: colType}, nil
		case float64:
			return &types.Value{Data: v, Type: colType}, nil
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return &types.Value{Data: f, Type: colType}, nil
			}
			return nil, fmt.Errorf("cannot parse string '%s' as number", v)
		default:
			return nil, fmt.Errorf("expected number, got %T", val)
		}

	case types.ColumnTypeBoolean:
		switch v := val.(type) {
		case bool:
			return &types.Value{Data: v, Type: colType}, nil
		case string:
			switch v {
			case "true", "1", "TRUE", "True":
				return &types.Value{Data: true, Type: colType}, nil
			case "false", "0", "FALSE", "False":
				return &types.Value{Data: false, Type: colType}, nil
			default:
				return nil, fmt.Errorf("cannot parse string '%s' as boolean", v)
			}
		case int, int64:
			return &types.Value{Data: v != 0, Type: colType}, nil
		default:
			return nil, fmt.Errorf("expected boolean, got %T", val)
		}

	case types.ColumnTypeTimestamp, types.ColumnTypeDate:
		switch v := val.(type) {
		case time.Time:
			return &types.Value{Data: v, Type: colType}, nil
		case int64:
			return &types.Value{Data: time.Unix(v, 0), Type: colType}, nil
		case string:
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				return &types.Value{Data: t, Type: colType}, nil
			}
			if t, err := time.Parse("2006-01-02", v); err == nil {
				return &types.Value{Data: t, Type: colType}, nil
			}
			return nil, fmt.Errorf("cannot parse string '%s' as timestamp", v)
		default:
			return nil, fmt.Errorf("expected time.Time, int64, or string, got %T", val)
		}

	case types.ColumnTypeBinary:
		switch v := val.(type) {
		case []byte:
			return &types.Value{Data: v, Type: colType}, nil
		case string:
			return &types.Value{Data: []byte(v), Type: colType}, nil
		default:
			return nil, fmt.Errorf("expected []byte or string, got %T", val)
		}

	case types.ColumnTypeJSON:
		if val == nil {
			return &types.Value{Data: nil, Type: colType}, nil
		}
		if _, err := json.Marshal(val); err != nil {
			return nil, fmt.Errorf("invalid JSON value: %w", err)
		}
		return &types.Value{Data: val, Type: colType}, nil

	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}
}

func (m *dataManager) getColumnType(schema *types.TableDefinition, colName string) (types.ColumnType, error) {
	for _, col := range schema.Columns {
		if col.Name == colName {
			return col.Type, nil
		}
	}
	return "", types.ErrColumnNotFound
}

func (m *dataManager) inferSchemaFromData(tableName string, data map[string]interface{}) *types.TableDefinition {
	columns := make([]types.ColumnDefinition, 0, len(data))
	indexes := []types.IndexDefinition{}
	
	for key, value := range data {
		colType := m.inferColumnType(value)
		columns = append(columns, types.ColumnDefinition{
			Name:     key,
			Type:     colType,
			Nullable: true,
		})
		
		if colType == types.ColumnTypeNumber {
			indexes = append(indexes, types.IndexDefinition{
				Columns: []string{key},
			})
		}
	}
	
	return &types.TableDefinition{
		ID:      uuid.New().String(),
		Name:    tableName,
		Version: 1,
		Columns: columns,
		Indexes: indexes,
	}
}

func (m *dataManager) inferColumnType(data interface{}) types.ColumnType {
	if data == nil {
		return types.ColumnTypeString
	}
	switch data.(type) {
	case string:
		return types.ColumnTypeString
	case int, int32, int64:
		return types.ColumnTypeNumber
	case float32, float64:
		return types.ColumnTypeNumber
	case bool:
		return types.ColumnTypeBoolean
	case time.Time:
		return types.ColumnTypeTimestamp
	case []byte:
		return types.ColumnTypeBinary
	default:
		return types.ColumnTypeJSON
	}
}
