package catalog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/types"
)

type queryManager struct {
	engine  engine.StorageEngine
	cache   *internal.SchemaCache
	manager Manager // Add reference to the full manager
}

func newQueryManager(eng engine.StorageEngine, cache *internal.SchemaCache, manager Manager) QueryManager {
	return &queryManager{
		engine:  eng,
		cache:   cache,
		manager: manager,
	}
}

func (m *queryManager) Query(ctx context.Context, tenantID int64, tableName string, opts *types.QueryOptions) (*types.QueryResult, error) {
	// Check if this is a system table query
	if m.isSystemTable(tableName) {
		// Convert opts.Where to the filter format expected by QuerySystemTable
		filter := make(map[string]interface{})
		if opts != nil && opts.Where != nil {
			for key, value := range opts.Where {
				filter[key] = value
			}
		}
		
		// Delegate to system table query handler
		return m.systemTableQuery(ctx, tenantID, tableName, filter)
	}
	
	// Handle regular user table query
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
		
		if opts.Where != nil && len(opts.Where) > 0 {
			scanOpts.Filter = m.buildFilterExpression(opts.Where)
		}
	}

	var iter engine.RowIterator
	if opts != nil && len(opts.OrderBy) > 0 {
		optimizer := NewQueryOptimizer(nil)
		candidate, err := optimizer.SelectBestIndex(opts, schema)
		
		if err == nil && candidate != nil {
			iter, err = m.engine.ScanIndex(ctx, tenantID, tableID, candidate.IndexID, schema, scanOpts)
		} else {
			iter, err = m.engine.ScanRows(ctx, tenantID, tableID, schema, scanOpts)
		}
	} else {
		iter, err = m.engine.ScanRows(ctx, tenantID, tableID, schema, scanOpts)
	}

	if err != nil {
		return nil, fmt.Errorf("scan rows: %w", err)
	}
	defer iter.Close()

	var records []*types.Record
	for iter.Next() {
		record := iter.Row()
		if opts != nil && opts.Where != nil {
			if !m.matchFilter(record, opts.Where) {
				continue
			}
		}
		records = append(records, record)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	// Build column info from schema
	columns := make([]types.ColumnInfo, len(schema.Columns))
	for i, col := range schema.Columns {
		columns[i] = types.ColumnInfo{
			Name: col.Name,
			Type: col.Type,
		}
	}

	// Convert records to rows
	rows := make([][]interface{}, len(records))
	for i, record := range records {
		row := make([]interface{}, len(schema.Columns))
		for j, col := range schema.Columns {
			if value, ok := record.Data[col.Name]; ok && value != nil {
				row[j] = value.Data
			} else {
				row[j] = nil
			}
		}
		rows[i] = row
	}

	result := &types.QueryResult{
		Rows:     rows,
		Columns:  columns,
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

func (m *queryManager) Count(ctx context.Context, tenantID int64, tableName string, filter map[string]interface{}) (int64, error) {
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

func (m *queryManager) isSystemTable(tableName string) bool {
	// Normalize table name by removing any extra whitespace and converting to lowercase
	normalized := strings.TrimSpace(strings.ToLower(tableName))
	
	// Check for system table prefixes
	if strings.HasPrefix(normalized, "information_schema.") || 
	   strings.HasPrefix(normalized, "pg_catalog.") {
		return true
	}
	
	// Also check for common system table names without schema prefix
	if strings.HasPrefix(normalized, "pg_") {
		return true
	}
	
	return false
}

func (m *queryManager) systemTableQuery(ctx context.Context, tenantID int64, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	// Normalize the table name
	normalizedTableName := strings.TrimSpace(fullTableName)
	
	// If no schema is specified, try to infer it
	if !strings.Contains(normalizedTableName, ".") {
		if strings.HasPrefix(strings.ToLower(normalizedTableName), "pg_") {
			normalizedTableName = "pg_catalog." + normalizedTableName
		} else {
			normalizedTableName = "information_schema." + normalizedTableName
		}
	}
	
	// Use the manager's QuerySystemTable method
	return m.manager.QuerySystemTable(ctx, normalizedTableName, filter)
}

func (m *queryManager) getTableSchema(tenantID int64, tableName string) (*types.TableDefinition, int64, error) {
	key := makeTableKey(tenantID, tableName)

	schema, tableID, exists := m.cache.Get(key)
	if !exists {
		return nil, 0, types.ErrTableNotFound
	}

	return schema, tableID, nil
}

func (m *queryManager) matchFilter(record *types.Record, filter map[string]interface{}) bool {
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
		case int:
			expectedInt := int64(expected)
			switch v := recordVal.Data.(type) {
			case int64:
				if v != expectedInt {
					return false
				}
			case int:
				if int64(v) != expectedInt {
					return false
				}
			case float64:
				if int64(v) != expectedInt {
					return false
				}
			default:
				return false
			}
		case int32:
			expectedInt := int64(expected)
			switch v := recordVal.Data.(type) {
			case int64:
				if v != expectedInt {
					return false
				}
			case int:
				if int64(v) != expectedInt {
					return false
				}
			case float64:
				if int64(v) != expectedInt {
					return false
				}
			default:
				return false
			}
		case int64:
			switch v := recordVal.Data.(type) {
			case int64:
				if v != expected {
					return false
				}
			case int:
				if int64(v) != expected {
					return false
				}
			case float64:
				if int64(v) != expected {
					return false
				}
			default:
				return false
			}
		case float64:
			switch v := recordVal.Data.(type) {
			case float64:
				if v != expected {
					return false
				}
			case int64:
				if float64(v) != expected {
					return false
				}
			case int:
				if float64(v) != expected {
					return false
				}
			default:
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

func (m *queryManager) findIndexForColumn(schema *types.TableDefinition, columnName string) int64 {
	for i, index := range schema.Indexes {
		if len(index.Columns) == 1 && index.Columns[0] == columnName {
			return int64(i + 1)
		}
	}
	return 0
}

// buildFilterExpression converts a simple map filter to a complex FilterExpression
func (m *queryManager) buildFilterExpression(where map[string]interface{}) *engine.FilterExpression {
	if len(where) == 0 {
		return nil
	}
	
	if len(where) == 1 {
		// Single condition
		for col, val := range where {
			return &engine.FilterExpression{
				Type:     "simple",
				Column:   col,
				Operator: "=",
				Value:    val,
			}
		}
	}
	
	// Multiple conditions - combine with AND
	children := make([]*engine.FilterExpression, 0, len(where))
	for col, val := range where {
		children = append(children, &engine.FilterExpression{
			Type:     "simple",
			Column:   col,
			Operator: "=",
			Value:    val,
		})
	}
	
	return &engine.FilterExpression{
		Type:     "and",
		Children: children,
	}
}