package pebble

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// IsIndexOnlyIterator checks if the given iterator is an index-only iterator
func IsIndexOnlyIterator(iter engineTypes.RowIterator) bool {
	_, ok := iter.(*scan.IndexOnlyIterator)
	return ok
}

// IsIndexIterator checks if the given iterator is an index iterator
func IsIndexIterator(iter engineTypes.RowIterator) bool {
	_, ok := iter.(*scan.IndexIterator)
	return ok
}

// IsRowIterator checks if the given iterator is a row iterator
func IsRowIterator(iter engineTypes.RowIterator) bool {
	_, ok := iter.(*scan.RowIterator)
	return ok
}

type pebbleEngine struct {
	kv          storage.KV
	codec       codec.Codec
	idGenerator *dbTypes.SnowflakeIDGenerator

	tableIDCounters map[int64]*int64
	indexIDCounters map[string]*int64
}

func NewPebbleEngine(kvStore storage.KV, c codec.Codec) engineTypes.StorageEngine {
	// Initialize the indexIDCounters map with a default counter for each tenant:table combination
	indexIDCounters := make(map[string]*int64)
	
	return &pebbleEngine{
		kv:              kvStore,
		codec:           c,
		idGenerator:     dbTypes.NewSnowflakeIDGenerator(0),
		tableIDCounters: make(map[int64]*int64),
		indexIDCounters: indexIDCounters,
	}
}

func (e *pebbleEngine) Close() error {
	return e.kv.Close()
}

func (e *pebbleEngine) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return e.idGenerator.Next()
}

func (e *pebbleEngine) NextTableID(ctx context.Context, tenantID int64) (int64, error) {
	if counter, exists := e.tableIDCounters[tenantID]; exists {
		return atomic.AddInt64(counter, 1), nil
	}

	var startID int64 = 0
	counter := &startID
	e.tableIDCounters[tenantID] = counter

	return atomic.AddInt64(counter, 1), nil
}

func (e *pebbleEngine) NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	key := fmt.Sprintf("%d:%d", tenantID, tableID)

	if counter, exists := e.indexIDCounters[key]; exists {
		return atomic.AddInt64(counter, 1), nil
	}

	var startID int64 = 0
	counter := &startID
	e.indexIDCounters[key] = counter

	return atomic.AddInt64(counter, 1), nil
}

func (e *pebbleEngine) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	// Start a transaction
	tx, err := e.BeginTx(ctx)
	if err != nil {
		return 0, err
	}
	
	// Delegate to transaction's DeleteRows method
	affected, err := tx.DeleteRows(ctx, tenantID, tableID, conditions, schemaDef)
	if err != nil {
		// Rollback the transaction on error
		tx.Rollback()
		return 0, err
	}
	
	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	
	return affected, nil
}

func (e *pebbleEngine) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*dbTypes.Value, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	// Start a transaction
	tx, err := e.BeginTx(ctx)
	if err != nil {
		return 0, err
	}
	
	// Delegate to transaction's UpdateRows method
	affected, err := tx.UpdateRows(ctx, tenantID, tableID, updates, conditions, schemaDef)
	if err != nil {
		// Rollback the transaction on error
		tx.Rollback()
		return 0, err
	}
	
	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	
	return affected, nil
}

func (e *pebbleEngine) ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	var startKey, endKey []byte

	if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
	} else {
		startKey = e.codec.EncodeTableKey(tenantID, tableID, 0)
	}

	if opts != nil && opts.EndKey != nil {
		endKey = opts.EndKey
	} else {
		endKey = e.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := e.kv.NewIterator(iterOpts)

	return scan.NewRowIterator(iter, e.codec, schemaDef, opts, e), nil
}

func (e *pebbleEngine) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	// Find the index definition to get column types
	var indexDef *dbTypes.IndexDefinition
	for i, idx := range schemaDef.Indexes {
		if int64(i+1) == indexID {
			indexDef = &idx
			break
		}
	}

	if indexDef == nil {
		return nil, fmt.Errorf("index not found: %d", indexID)
	}

	// Get column types for the index columns
	columnTypes := make([]dbTypes.ColumnType, len(indexDef.Columns))
	for i, colName := range indexDef.Columns {
		found := false
		for _, col := range schemaDef.Columns {
			if col.Name == colName {
				columnTypes[i] = col.Type
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column not found in schema: %s", colName)
		}
	}

	var startKey, endKey []byte

	if opts != nil && opts.Filter != nil {
		startKey, endKey = e.buildIndexRangeFromFilter(tenantID, tableID, indexID, opts.Filter, indexDef)
	} else if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
		if opts.EndKey != nil {
			endKey = opts.EndKey
		} else {
			endKey = e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		}
	} else {
		startKey = e.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		endKey = e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := e.kv.NewIterator(iterOpts)
	
	// Check if this is an index-only scan (covering index)
	isCovering := false
	if opts != nil && opts.Projection != nil && len(opts.Projection) > 0 {
		isCovering = e.isIndexCovering(indexDef, opts.Projection)
	}
	
	if isCovering {
		return scan.NewIndexOnlyIterator(
			iter,
			e.codec,
			indexDef,
			opts.Projection,
			opts,
			columnTypes,
			tenantID,
			tableID,
			indexID,
			e,
		), nil
	}

	return scan.NewIndexIterator(
		iter,
		e.codec,
		schemaDef,
		opts,
		columnTypes,
		tenantID,
		tableID,
		e,
	), nil
}

// buildIndexRangeFromFilter constructs index scan range based on filter expression
func (e *pebbleEngine) buildIndexRangeFromFilter(tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression, indexDef *dbTypes.IndexDefinition) ([]byte, []byte) {
	if filter == nil {
		return e.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
	
	// For complex filters (AND/OR/NOT), do full index scan and filter in iterator
	// TODO: Support multi-column index range optimization for AND filters
	if filter.Type == "and" || filter.Type == "or" || filter.Type == "not" {
		// Do full index scan, filtering happens in indexOnlyIterator.Next()
		return e.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
	
	// Simple filter
	return e.buildRangeFromSimpleFilter(tenantID, tableID, indexID, filter)
}

// extractSimpleFilter finds a simple filter for the given column in a complex filter tree
func (e *pebbleEngine) extractSimpleFilter(filter *engineTypes.FilterExpression, columnName string) *engineTypes.FilterExpression {
	if filter.Type == "simple" && filter.Column == columnName {
		return filter
	}
	
	if filter.Type == "and" {
		// For AND, we can use any matching condition
		for _, child := range filter.Children {
			if result := e.extractSimpleFilter(child, columnName); result != nil {
				return result
			}
		}
	}
	
	// For OR/NOT, we cannot safely extract a simple filter
	return nil
}

// buildRangeFromSimpleFilter constructs index range for a simple filter
func (e *pebbleEngine) buildRangeFromSimpleFilter(tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression) ([]byte, []byte) {
	maxRowID := int64(^uint64(0) >> 1)
	
	switch filter.Operator {
	case "=":
		start, _ := e.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end, _ := e.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	case ">":
		start, _ := e.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		end := e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case ">=":
		start, _ := e.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		end := e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
		return start, end
		
	case "<":
		start := e.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := e.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, 0)
		return start, end
		
	case "<=":
		start := e.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
		end, _ := e.codec.EncodeIndexKey(tenantID, tableID, indexID, filter.Value, maxRowID)
		return start, end
		
	default:
		// Unsupported operator, full scan
		return e.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID),
			e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}
}

// isIndexCovering checks if an index covers all projection columns
func (e *pebbleEngine) isIndexCovering(indexDef *dbTypes.IndexDefinition, projection []string) bool {
	indexColSet := make(map[string]bool)
	for _, col := range indexDef.Columns {
		indexColSet[col] = true
	}
	
	for _, col := range projection {
		if col != "_rowid" && !indexColSet[col] {
			return false
		}
	}
	
	return true
}

func (e *pebbleEngine) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *dbTypes.IndexDefinition) error {
	// Generate a new index ID
	indexID, err := e.NextIndexID(ctx, tenantID, tableID)
	if err != nil {
		return fmt.Errorf("generate index id: %w", err)
	}

	// Store index metadata
	// TODO: Implement proper index metadata storage
	_ = indexID // Placeholder to avoid unused variable error

	return nil
}

func (e *pebbleEngine) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	// TODO: Remove index metadata
	// TODO: Remove all index entries from storage

	return nil
}

func (e *pebbleEngine) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	startKey, err := e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValue, 0)
	if err != nil {
		return nil, fmt.Errorf("encode start key: %w", err)
	}

	endKey, err := e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValue, int64(^uint64(0)>>1))
	if err != nil {
		return nil, fmt.Errorf("encode end key: %w", err)
	}

	iter := e.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer iter.Close()

	var rowIDs []int64
	for iter.First(); iter.Valid(); iter.Next() {
		// Extract rowID from the index key
		_, _, _, _, rowID, err := e.codec.DecodeIndexKey(iter.Key())
		if err != nil {
			return nil, fmt.Errorf("decode index key: %w", err)
		}
		rowIDs = append(rowIDs, rowID)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return rowIDs, nil
}

func (e *pebbleEngine) updateIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition, isInsert bool) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

		indexValues := make([]interface{}, 0, len(indexDef.Columns))
		allValuesPresent := true

		for _, colName := range indexDef.Columns {
			if val, ok := row.Data[colName]; ok && val != nil {
				indexValues = append(indexValues, val.Data)
			} else {
				allValuesPresent = false
				break
			}
		}

		if allValuesPresent && len(indexValues) > 0 {
			var indexKey []byte
			var err error

			if len(indexValues) == 1 {
				indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := e.kv.Set(ctx, indexKey, []byte{}); err != nil {
				return fmt.Errorf("set index: %w", err)
			}
		}
	}

	return nil
}

func (e *pebbleEngine) batchUpdateIndexes(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

		indexValues := make([]interface{}, 0, len(indexDef.Columns))
		allValuesPresent := true

		for _, colName := range indexDef.Columns {
			if val, ok := row.Data[colName]; ok && val != nil {
				indexValues = append(indexValues, val.Data)
			} else {
				allValuesPresent = false
				break
			}
		}

		if allValuesPresent && len(indexValues) > 0 {
			var indexKey []byte
			var err error

			if len(indexValues) == 1 {
				indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := batch.Set(indexKey, []byte{}); err != nil {
				return fmt.Errorf("batch set index: %w", err)
			}
		}
	}

	return nil
}

func (e *pebbleEngine) batchUpdateIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil || len(rows) == 0 {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

		for rowID, row := range rows {
			indexValues := make([]interface{}, 0, len(indexDef.Columns))
			allValuesPresent := true

			for _, colName := range indexDef.Columns {
				if val, ok := row.Data[colName]; ok && val != nil {
					indexValues = append(indexValues, val.Data)
				} else {
					allValuesPresent = false
					break
				}
			}

			if allValuesPresent && len(indexValues) > 0 {
				var indexKey []byte
				var err error

				if len(indexValues) == 1 {
					indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
				} else {
					indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
				}

				if err != nil {
					return fmt.Errorf("encode index key: %w", err)
				}

				if err := batch.Set(indexKey, []byte{}); err != nil {
					return fmt.Errorf("batch set index: %w", err)
				}
			}
		}
	}

	return nil
}

func (e *pebbleEngine) deleteIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

		// Collect index values for all columns in this index
		indexValues := make([]interface{}, 0, len(indexDef.Columns))
		allValuesPresent := true

		for _, colName := range indexDef.Columns {
			if val, ok := row.Data[colName]; ok && val != nil {
				indexValues = append(indexValues, val.Data)
			} else {
				// If any indexed column is null, we don't have an index entry for this row
				allValuesPresent = false
				break
			}
		}

		// Only delete index entry if all values were present
		if allValuesPresent && len(indexValues) > 0 {
			var indexKey []byte
			var err error

			if len(indexValues) == 1 {
				// Single column index
				indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				// Composite index
				indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := e.kv.Delete(ctx, indexKey); err != nil {
				return fmt.Errorf("delete index: %w", err)
			}
		}
	}

	return nil
}

func (e *pebbleEngine) deleteIndexesInBatch(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

		indexValues := make([]interface{}, 0, len(indexDef.Columns))
		allValuesPresent := true

		for _, colName := range indexDef.Columns {
			if val, ok := row.Data[colName]; ok && val != nil {
				indexValues = append(indexValues, val.Data)
			} else {
				allValuesPresent = false
				break
			}
		}

		if allValuesPresent && len(indexValues) > 0 {
			var indexKey []byte
			var err error

			if len(indexValues) == 1 {
				indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := batch.Delete(indexKey); err != nil {
				return fmt.Errorf("batch delete index: %w", err)
			}
		}
	}

	return nil
}

func (e *pebbleEngine) deleteIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil || len(rows) == 0 {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

		for rowID, row := range rows {
			indexValues := make([]interface{}, 0, len(indexDef.Columns))
			allValuesPresent := true

			for _, colName := range indexDef.Columns {
				if val, ok := row.Data[colName]; ok && val != nil {
					indexValues = append(indexValues, val.Data)
				} else {
					allValuesPresent = false
					break
				}
			}

			if allValuesPresent && len(indexValues) > 0 {
				var indexKey []byte
				var err error

				if len(indexValues) == 1 {
					indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
				} else {
					indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
				}

				if err != nil {
					return fmt.Errorf("encode index key: %w", err)
				}

				if err := batch.Delete(indexKey); err != nil {
					return fmt.Errorf("batch delete index: %w", err)
				}
			}
		}
	}

	return nil
}

// EvaluateFilter evaluates a filter expression against a record
func (e *pebbleEngine) EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool {
	if filter == nil {
		return true
	}
	
	switch filter.Type {
	case "simple":
		return e.evaluateSimpleFilter(filter, record)
		
	case "and":
		for _, child := range filter.Children {
			if !e.EvaluateFilter(child, record) {
				return false
			}
		}
		return true
		
	case "or":
		for _, child := range filter.Children {
			if e.EvaluateFilter(child, record) {
				return true
			}
		}
		return false
		
	case "not":
		if len(filter.Children) > 0 {
			return !e.EvaluateFilter(filter.Children[0], record)
		}
		return true
		
	default:
		return true
	}
}

// evaluateSimpleFilter evaluates a simple filter condition
func (e *pebbleEngine) evaluateSimpleFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool {
	val, exists := record.Data[filter.Column]
	if !exists {
		return false
	}

	if val.Data == nil {
		return filter.Value == nil
	}

	switch filter.Operator {
	case "=":
		return e.compareValues(val.Data, filter.Value) == 0
	case ">":
		return e.compareValues(val.Data, filter.Value) > 0
	case ">=":
		return e.compareValues(val.Data, filter.Value) >= 0
	case "<":
		return e.compareValues(val.Data, filter.Value) < 0
	case "<=":
		return e.compareValues(val.Data, filter.Value) <= 0
	case "IN":
		for _, v := range filter.Values {
			if e.compareValues(val.Data, v) == 0 {
				return true
			}
		}
		return false
	case "BETWEEN":
		if len(filter.Values) >= 2 {
			return e.compareValues(val.Data, filter.Values[0]) >= 0 &&
				e.compareValues(val.Data, filter.Values[1]) <= 0
		}
		return false
	default:
		return true
	}
}

// compareValues compares two values, returns -1/0/1 like strcmp
func (e *pebbleEngine) compareValues(a, b interface{}) int {
	// Handle nil cases
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	
	// Type-specific comparison
	switch av := a.(type) {
	case int64:
		bv := toInt64(b)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
		
	case int:
		return e.compareValues(int64(av), b)
		
	case float64:
		bv := toFloat64(b)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
		
	case string:
		bv, ok := b.(string)
		if !ok {
			return 1
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
		
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 1
		}
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1
		
	default:
		return 0
	}
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}

// buildFilterExpression converts a simple map filter to a complex FilterExpression
func (e *pebbleEngine) buildFilterExpression(conditions map[string]interface{}) *engineTypes.FilterExpression {
	if len(conditions) == 0 {
		return nil
	}
	
	if len(conditions) == 1 {
		// Single condition
		for col, val := range conditions {
			return &engineTypes.FilterExpression{
				Type:     "simple",
				Column:   col,
				Operator: "=",
				Value:    val,
			}
		}
	}
	
	// Multiple conditions - combine with AND
	children := make([]*engineTypes.FilterExpression, 0, len(conditions))
	for col, val := range conditions {
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