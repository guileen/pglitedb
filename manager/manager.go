package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/guileen/pqlitedb/engine"
	"github.com/guileen/pqlitedb/table"
)

type tableManager struct {
	engine engine.StorageEngine

	schemas     map[string]*table.TableDefinition
	tableIDs    map[string]int64
	schemaMutex sync.RWMutex
}

func NewTableManager(eng engine.StorageEngine) Manager {
	return &tableManager{
		engine:   eng,
		schemas:  make(map[string]*table.TableDefinition),
		tableIDs: make(map[string]int64),
	}
}

func (m *tableManager) CreateTable(ctx context.Context, tenantID int64, def *table.TableDefinition) error {
	m.schemaMutex.Lock()
	defer m.schemaMutex.Unlock()

	key := m.makeTableKey(tenantID, def.Name)
	if _, exists := m.schemas[key]; exists {
		return fmt.Errorf("table %s already exists", def.Name)
	}

	// Validate column types
	for _, col := range def.Columns {
		if !table.IsValidColumnType(col.Type) {
			return fmt.Errorf("invalid column type '%s' for column '%s'", col.Type, col.Name)
		}
	}

	tableID, err := m.engine.NextTableID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("generate table id: %w", err)
	}

	def.CreatedAt = time.Now()
	def.UpdatedAt = time.Now()
	if def.Version == 0 {
		def.Version = 1
	}

	m.schemas[key] = def
	m.tableIDs[key] = tableID

	return nil
}

func (m *tableManager) DropTable(ctx context.Context, tenantID int64, tableName string) error {
	m.schemaMutex.Lock()
	defer m.schemaMutex.Unlock()

	key := m.makeTableKey(tenantID, tableName)
	if _, exists := m.schemas[key]; !exists {
		return table.ErrTableNotFound
	}

	delete(m.schemas, key)
	delete(m.tableIDs, key)

	return nil
}

func (m *tableManager) GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*table.TableDefinition, error) {
	m.schemaMutex.RLock()
	defer m.schemaMutex.RUnlock()

	key := m.makeTableKey(tenantID, tableName)
	def, exists := m.schemas[key]
	if !exists {
		return nil, table.ErrTableNotFound
	}

	return def, nil
}

func (m *tableManager) AlterTable(ctx context.Context, tenantID int64, tableName string, changes *AlterTableChanges) error {
	m.schemaMutex.Lock()
	defer m.schemaMutex.Unlock()

	key := m.makeTableKey(tenantID, tableName)
	def, exists := m.schemas[key]
	if !exists {
		return table.ErrTableNotFound
	}

	if changes.AddColumns != nil {
		// Validate new column types
		for _, col := range changes.AddColumns {
			if !table.IsValidColumnType(col.Type) {
				return fmt.Errorf("invalid column type '%s' for column '%s'", col.Type, col.Name)
			}
		}
		def.Columns = append(def.Columns, changes.AddColumns...)
	}

	if changes.DropColumns != nil {
		newColumns := make([]table.ColumnDefinition, 0, len(def.Columns))
		for _, col := range def.Columns {
			drop := false
			for _, dropName := range changes.DropColumns {
				if col.Name == dropName {
					drop = true
					break
				}
			}
			if !drop {
				newColumns = append(newColumns, col)
			}
		}
		def.Columns = newColumns
	}

	if changes.AddIndexes != nil {
		def.Indexes = append(def.Indexes, changes.AddIndexes...)
	}

	def.Version++
	def.UpdatedAt = time.Now()

	return nil
}

func (m *tableManager) ListTables(ctx context.Context, tenantID int64) ([]*table.TableDefinition, error) {
	m.schemaMutex.RLock()
	defer m.schemaMutex.RUnlock()

	var tables []*table.TableDefinition
	prefix := fmt.Sprintf("%d:", tenantID)

	for key, def := range m.schemas {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			tables = append(tables, def)
		}
	}

	return tables, nil
}

func (m *tableManager) Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*table.Record, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
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

func (m *tableManager) InsertBatch(ctx context.Context, tenantID int64, tableName string, rows []map[string]interface{}) ([]*table.Record, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return nil, err
	}

	records := make([]*table.Record, 0, len(rows))

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

func (m *tableManager) Update(ctx context.Context, tenantID int64, tableName string, rowID int64, data map[string]interface{}) (*table.Record, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return nil, err
	}

	updates := make(map[string]*table.Value)
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

func (m *tableManager) Delete(ctx context.Context, tenantID int64, tableName string, rowID int64) error {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return err
	}

	if err := m.engine.DeleteRow(ctx, tenantID, tableID, rowID, schema); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	return nil
}

func (m *tableManager) Get(ctx context.Context, tenantID int64, tableName string, rowID int64) (*table.Record, error) {
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

func (m *tableManager) Query(ctx context.Context, tenantID int64, tableName string, opts *table.QueryOptions) (*table.QueryResult, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()

	scanOpts := &engine.ScanOptions{}
	if opts != nil {
		if opts.Limit != nil {
			scanOpts.Limit = *opts.Limit
		}
		if opts.Offset != nil {
			scanOpts.Offset = *opts.Offset
		}
		if opts.Columns != nil {
			scanOpts.Projection = opts.Columns
		}
	}

	iter, err := m.engine.ScanRows(ctx, tenantID, tableID, schema, scanOpts)
	if err != nil {
		return nil, fmt.Errorf("scan rows: %w", err)
	}
	defer iter.Close()

	var records []*table.Record
	for iter.Next() {
		record := iter.Row()

		if opts != nil && opts.Where != nil {
			if !m.matchFilter(record, opts.Where) {
				continue
			}
		}

		records = append(records, record)

		if opts != nil && opts.Limit != nil && len(records) >= *opts.Limit {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	result := &table.QueryResult{
		Records:  records,
		Count:    int64(len(records)),
		Duration: time.Since(startTime),
	}

	if opts != nil {
		result.Limit = opts.Limit
		result.Offset = opts.Offset

		if opts.Limit != nil {
			result.HasMore = len(records) == *opts.Limit
		}
	}

	return result, nil
}

func (m *tableManager) Count(ctx context.Context, tenantID int64, tableName string, filter map[string]interface{}) (int64, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return 0, err
	}

	iter, err := m.engine.ScanRows(ctx, tenantID, tableID, schema, nil)
	if err != nil {
		return 0, fmt.Errorf("scan rows: %w", err)
	}
	defer iter.Close()

	var count int64
	for iter.Next() {
		if filter != nil {
			record := iter.Row()
			if !m.matchFilter(record, filter) {
				continue
			}
		}
		count++
	}

	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterator error: %w", err)
	}

	return count, nil
}

func (m *tableManager) CreateIndex(ctx context.Context, tenantID int64, tableName string, indexDef *table.IndexDefinition) error {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return err
	}

	if err := m.engine.CreateIndex(ctx, tenantID, tableID, indexDef); err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	m.schemaMutex.Lock()
	defer m.schemaMutex.Unlock()

	schema.Indexes = append(schema.Indexes, *indexDef)
	schema.UpdatedAt = time.Now()

	return nil
}

func (m *tableManager) DropIndex(ctx context.Context, tenantID int64, tableName string, indexName string) error {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return err
	}

	var indexID int64 = -1
	for i, idx := range schema.Indexes {
		if idx.Name == indexName {
			indexID = int64(i + 1)
			break
		}
	}

	if indexID < 0 {
		return fmt.Errorf("index %s not found", indexName)
	}

	if err := m.engine.DropIndex(ctx, tenantID, tableID, indexID); err != nil {
		return fmt.Errorf("drop index: %w", err)
	}

	m.schemaMutex.Lock()
	defer m.schemaMutex.Unlock()

	newIndexes := make([]table.IndexDefinition, 0, len(schema.Indexes)-1)
	for _, idx := range schema.Indexes {
		if idx.Name != indexName {
			newIndexes = append(newIndexes, idx)
		}
	}
	schema.Indexes = newIndexes
	schema.UpdatedAt = time.Now()

	return nil
}

func (m *tableManager) makeTableKey(tenantID int64, tableName string) string {
	return fmt.Sprintf("%d:%s", tenantID, tableName)
}

func (m *tableManager) getTableSchema(tenantID int64, tableName string) (*table.TableDefinition, int64, error) {
	m.schemaMutex.RLock()
	defer m.schemaMutex.RUnlock()

	key := m.makeTableKey(tenantID, tableName)
	schema, exists := m.schemas[key]
	if !exists {
		return nil, 0, table.ErrTableNotFound
	}

	tableID, exists := m.tableIDs[key]
	if !exists {
		return nil, 0, table.ErrTableNotFound
	}

	return schema, tableID, nil
}

func (m *tableManager) validateAndConvert(data map[string]interface{}, schema *table.TableDefinition) (*table.Record, error) {
	record := &table.Record{
		Data: make(map[string]*table.Value),
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
			record.Data[col.Name] = &table.Value{Data: nil, Type: col.Type}
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

func (m *tableManager) convertValue(val interface{}, colType table.ColumnType) (*table.Value, error) {
	if val == nil {
		return &table.Value{Data: nil, Type: colType}, nil
	}

	switch colType {
	case table.ColumnTypeString, table.ColumnTypeText:
		if s, ok := val.(string); ok {
			return &table.Value{Data: s, Type: colType}, nil
		}
		return nil, fmt.Errorf("expected string, got %T", val)

	case table.ColumnTypeUUID:
		// Handle UUID conversion
		switch v := val.(type) {
		case string:
			if v == "" {
				return &table.Value{Data: nil, Type: colType}, nil
			}
			// Validate UUID format
			if _, err := uuid.Parse(v); err != nil {
				return nil, fmt.Errorf("invalid UUID string: %w", err)
			}
			return &table.Value{Data: v, Type: colType}, nil
		case []byte:
			if len(v) == 0 {
				return &table.Value{Data: nil, Type: colType}, nil
			}
			if len(v) != 16 {
				return nil, fmt.Errorf("UUID byte slice must be 16 bytes")
			}
			u, err := uuid.FromBytes(v)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID bytes: %w", err)
			}
			return &table.Value{Data: u.String(), Type: colType}, nil
		case uuid.UUID:
			return &table.Value{Data: v.String(), Type: colType}, nil
		default:
			return nil, fmt.Errorf("expected UUID string, []byte, or uuid.UUID, got %T", val)
		}

	case table.ColumnTypeNumber:
		switch v := val.(type) {
		case int:
			return &table.Value{Data: int64(v), Type: colType}, nil
		case int32:
			return &table.Value{Data: int64(v), Type: colType}, nil
		case int64:
			return &table.Value{Data: v, Type: colType}, nil
		case float32:
			return &table.Value{Data: float64(v), Type: colType}, nil
		case float64:
			return &table.Value{Data: v, Type: colType}, nil
		case string:
			// Try to parse string as number
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return &table.Value{Data: f, Type: colType}, nil
			}
			return nil, fmt.Errorf("cannot parse string '%s' as number", v)
		default:
			return nil, fmt.Errorf("expected number, got %T", val)
		}

	case table.ColumnTypeBoolean:
		switch v := val.(type) {
		case bool:
			return &table.Value{Data: v, Type: colType}, nil
		case string:
			switch v {
			case "true", "1", "TRUE", "True":
				return &table.Value{Data: true, Type: colType}, nil
			case "false", "0", "FALSE", "False":
				return &table.Value{Data: false, Type: colType}, nil
			default:
				return nil, fmt.Errorf("cannot parse string '%s' as boolean", v)
			}
		case int, int64:
			// 0 = false, non-zero = true
			return &table.Value{Data: v != 0, Type: colType}, nil
		default:
			return nil, fmt.Errorf("expected boolean, got %T", val)
		}

	case table.ColumnTypeTimestamp, table.ColumnTypeDate:
		switch v := val.(type) {
		case time.Time:
			return &table.Value{Data: v, Type: colType}, nil
		case int64:
			return &table.Value{Data: time.Unix(v, 0), Type: colType}, nil
		case string:
			// Try to parse as time
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				return &table.Value{Data: t, Type: colType}, nil
			}
			if t, err := time.Parse("2006-01-02", v); err == nil {
				return &table.Value{Data: t, Type: colType}, nil
			}
			return nil, fmt.Errorf("cannot parse string '%s' as timestamp", v)
		default:
			return nil, fmt.Errorf("expected time.Time, int64, or string, got %T", val)
		}

	case table.ColumnTypeBinary:
		switch v := val.(type) {
		case []byte:
			return &table.Value{Data: v, Type: colType}, nil
		case string:
			return &table.Value{Data: []byte(v), Type: colType}, nil
		default:
			return nil, fmt.Errorf("expected []byte or string, got %T", val)
		}

	case table.ColumnTypeJSON:
		// Validate that value is JSON serializable
		if val == nil {
			return &table.Value{Data: nil, Type: colType}, nil
		}
		// Try to marshal to JSON to validate
		if _, err := json.Marshal(val); err != nil {
			return nil, fmt.Errorf("invalid JSON value: %w", err)
		}
		return &table.Value{Data: val, Type: colType}, nil

	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}
}

func (m *tableManager) getColumnType(schema *table.TableDefinition, colName string) (table.ColumnType, error) {
	for _, col := range schema.Columns {
		if col.Name == colName {
			return col.Type, nil
		}
	}
	return "", table.ErrColumnNotFound
}

func (m *tableManager) matchFilter(record *table.Record, filter map[string]interface{}) bool {
	for key, expectedVal := range filter {
		recordVal, exists := record.Data[key]
		if !exists {
			return false
		}

		if recordVal.Data == nil {
			if expectedVal != nil {
				return false
			}
			continue
		}

		switch expected := expectedVal.(type) {
		case string:
			if s, ok := recordVal.Data.(string); !ok || s != expected {
				return false
			}
		case int, int32, int64:
			var expectedInt int64
			switch v := expected.(type) {
			case int:
				expectedInt = int64(v)
			case int32:
				expectedInt = int64(v)
			case int64:
				expectedInt = v
			}
			if i, ok := recordVal.Data.(int64); !ok || i != expectedInt {
				return false
			}
		case bool:
			if b, ok := recordVal.Data.(bool); !ok || b != expected {
				return false
			}
		default:
			return false
		}
	}

	return true
}
