package catalog

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/guileen/pglitedb/catalog/internal"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/types"
)

type queryManager struct {
	scanOps engineTypes.ScanOperations
	cache   *internal.SchemaCache
	manager Manager // Add reference to the full manager
}

func newQueryManager(scanOps engineTypes.ScanOperations, cache *internal.SchemaCache, manager Manager) QueryManager {
	return &queryManager{
		scanOps: scanOps,
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

	scanOpts := &engineTypes.ScanOptions{}
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

	var iter engineTypes.RowIterator
	if opts != nil && len(opts.OrderBy) > 0 {
		optimizer := NewQueryOptimizer(nil)
		candidate, err := optimizer.SelectBestIndex(opts, schema)
		
		if err == nil && candidate != nil {
			iter, err = m.scanOps.ScanIndex(ctx, tenantID, tableID, candidate.IndexID, schema, scanOpts)
		} else {
			iter, err = m.scanOps.ScanRows(ctx, tenantID, tableID, schema, scanOpts)
		}
	} else {
		iter, err = m.scanOps.ScanRows(ctx, tenantID, tableID, schema, scanOpts)
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

	// Apply sorting if ORDER BY is specified
	if opts != nil && len(opts.OrderBy) > 0 {
		records = m.sortRecords(records, opts.OrderBy, schema)
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

	iter, err := m.scanOps.ScanRows(ctx, tenantID, tableID, schema, nil)
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

// sortRecords sorts records according to the ORDER BY specification
func (m *queryManager) sortRecords(records []*types.Record, orderBy []string, schema *types.TableDefinition) []*types.Record {
	if len(records) <= 1 {
		return records
	}

	// Create a copy to avoid modifying the original slice
	sortedRecords := make([]*types.Record, len(records))
	copy(sortedRecords, records)

	// Parse order by specifications (field ASC/DESC)
	type orderSpec struct {
		field string
		desc  bool
	}
	orderSpecs := make([]orderSpec, len(orderBy))
	for i, ob := range orderBy {
		spec := orderSpec{field: ob, desc: false}
		// Check for DESC suffix
		if strings.HasSuffix(strings.ToUpper(ob), " DESC") {
			spec.field = strings.TrimSpace(ob[:len(ob)-5])
			spec.desc = true
		} else if strings.HasSuffix(strings.ToUpper(ob), " ASC") {
			spec.field = strings.TrimSpace(ob[:len(ob)-4])
		}
		orderSpecs[i] = spec
	}

	sort.SliceStable(sortedRecords, func(i, j int) bool {
		for _, spec := range orderSpecs {
			valI := m.getFieldValue(sortedRecords[i], spec.field)
			valJ := m.getFieldValue(sortedRecords[j], spec.field)

			// Handle nil values
			if valI == nil && valJ == nil {
				continue
			}
			if valI == nil {
				return spec.desc // nil goes to end if DESC, beginning if ASC
			}
			if valJ == nil {
				return !spec.desc // nil goes to end if DESC, beginning if ASC
			}

			// Compare values based on their types
			result := m.compareValues(valI, valJ)
			if result == 0 {
				continue // Equal, check next field
			}

			if spec.desc {
				return result > 0 // Descending order
			}
			return result < 0 // Ascending order
		}
		return false // All fields equal
	})

	return sortedRecords
}

// getFieldValue extracts a field value from a record
func (m *queryManager) getFieldValue(record *types.Record, fieldName string) interface{} {
	if record == nil || record.Data == nil {
		return nil
	}
	
	if value, ok := record.Data[fieldName]; ok && value != nil {
		return value.Data
	}
	return nil
}

// compareValues compares two values of potentially different types
func (m *queryManager) compareValues(a, b interface{}) int {
	// Handle nil values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try to convert both to strings for comparison if types differ
	switch aVal := a.(type) {
	case int:
		if bVal, ok := b.(int); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
		// Try int64
		if bVal, ok := b.(int64); ok {
			a64 := int64(aVal)
			if a64 < bVal {
				return -1
			} else if a64 > bVal {
				return 1
			}
			return 0
		}
		// Convert b to string for comparison
		return strings.Compare(fmt.Sprintf("%v", aVal), fmt.Sprintf("%v", b))
	case int64:
		if bVal, ok := b.(int64); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
		// Try int
		if bVal, ok := b.(int); ok {
			b64 := int64(bVal)
			if aVal < b64 {
				return -1
			} else if aVal > b64 {
				return 1
			}
			return 0
		}
		// Convert b to string for comparison
		return strings.Compare(fmt.Sprintf("%v", aVal), fmt.Sprintf("%v", b))
	case float32:
		if bVal, ok := b.(float32); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
		// Try float64
		if bVal, ok := b.(float64); ok {
			a64 := float64(aVal)
			if a64 < bVal {
				return -1
			} else if a64 > bVal {
				return 1
			}
			return 0
		}
		// Convert b to string for comparison
		return strings.Compare(fmt.Sprintf("%v", aVal), fmt.Sprintf("%v", b))
	case float64:
		if bVal, ok := b.(float64); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
		// Try float32
		if bVal, ok := b.(float32); ok {
			b64 := float64(bVal)
			if aVal < b64 {
				return -1
			} else if aVal > b64 {
				return 1
			}
			return 0
		}
		// Convert b to string for comparison
		return strings.Compare(fmt.Sprintf("%v", aVal), fmt.Sprintf("%v", b))
	case bool:
		if bVal, ok := b.(bool); ok {
			if !aVal && bVal {
				return -1
			} else if aVal && !bVal {
				return 1
			}
			return 0
		}
		// Convert b to string for comparison
		return strings.Compare(fmt.Sprintf("%v", aVal), fmt.Sprintf("%v", b))
	case string:
		if bVal, ok := b.(string); ok {
			return strings.Compare(aVal, bVal)
		}
		// Convert b to string for comparison
		return strings.Compare(aVal, fmt.Sprintf("%v", b))
	case []byte:
		if bVal, ok := b.([]byte); ok {
			return bytes.Compare(aVal, bVal)
		}
		// Convert both to strings for comparison
		return strings.Compare(fmt.Sprintf("%v", aVal), fmt.Sprintf("%v", b))
	default:
		// Convert both to strings for comparison
		return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	}
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
func (m *queryManager) buildFilterExpression(where map[string]interface{}) *engineTypes.FilterExpression {
	if len(where) == 0 {
		return nil
	}
	
	if len(where) == 1 {
		// Single condition
		for col, val := range where {
			return &engineTypes.FilterExpression{
				Type:     "simple",
				Column:   col,
				Operator: "=",
				Value:    val,
			}
		}
	}
	
	// Multiple conditions - combine with AND
	children := make([]*engineTypes.FilterExpression, 0, len(where))
	for col, val := range where {
		children = append(children, &engineTypes.FilterExpression{
			Type:     "simple",
			Column:   col,
			Operator: "=",
			Value:    val,
		})
	}
	
	return &engineTypes.FilterExpression{
		Type:     "and",
		Children: children,
	}
}