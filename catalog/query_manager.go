package catalog

import (
	"context"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/types"
)

type queryManager struct {
	engine engine.StorageEngine
	cache  *internal.SchemaCache
}

func newQueryManager(eng engine.StorageEngine, cache *internal.SchemaCache) QueryManager {
	return &queryManager{
		engine: eng,
		cache:  cache,
	}
}

func (m *queryManager) Query(ctx context.Context, tenantID int64, tableName string, opts *types.QueryOptions) (*types.QueryResult, error) {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return nil, err
	}
	startTime := time.Now()

	var iter engine.RowIterator
	if opts != nil && len(opts.OrderBy) > 0 && len(opts.OrderBy) == 1 {
		indexID := m.findIndexForColumn(schema, opts.OrderBy[0])
		if indexID > 0 {
			iter, err = m.engine.ScanIndex(ctx, tenantID, tableID, indexID, schema, nil)
		} else {
			iter, err = m.engine.ScanRows(ctx, tenantID, tableID, schema, nil)
		}
	} else {
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
		if opts != nil && opts.Limit != nil && len(records) >= *opts.Limit {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	result := &types.QueryResult{
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

func (m *queryManager) findIndexForColumn(schema *types.TableDefinition, columnName string) int64 {
	for i, index := range schema.Indexes {
		if len(index.Columns) == 1 && index.Columns[0] == columnName {
			return int64(i + 1)
		}
	}
	return 0
}
