package engine

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type pebbleEngine struct {
	kv          storage.KV
	codec       codec.Codec
	idGenerator *types.SnowflakeIDGenerator

	tableIDCounters map[int64]*int64
	indexIDCounters map[string]*int64
}

func NewPebbleEngine(kvStore storage.KV, c codec.Codec) StorageEngine {
	return &pebbleEngine{
		kv:              kvStore,
		codec:           c,
		idGenerator:     types.NewSnowflakeIDGenerator(0),
		tableIDCounters: make(map[int64]*int64),
		indexIDCounters: make(map[string]*int64),
	}
}

func (e *pebbleEngine) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error) {
	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := e.kv.Get(ctx, key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, types.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := e.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

func (e *pebbleEngine) GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) (map[int64]*types.Record, error) {
	if len(rowIDs) == 0 {
		return make(map[int64]*types.Record), nil
	}

	result := make(map[int64]*types.Record, len(rowIDs))
	
	sorted := make([]int64, len(rowIDs))
	copy(sorted, rowIDs)
	sortInt64Slice(sorted)
	
	startKey := e.codec.EncodeTableKey(tenantID, tableID, sorted[0])
	endKey := e.codec.EncodeTableKey(tenantID, tableID, sorted[len(sorted)-1]+1)
	
	iter := e.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer iter.Close()
	
	targetIdx := 0
	for iter.First(); iter.Valid() && targetIdx < len(sorted); iter.Next() {
		_, _, rowID, err := e.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return nil, fmt.Errorf("decode table key: %w", err)
		}
		
		for targetIdx < len(sorted) && sorted[targetIdx] < rowID {
			targetIdx++
		}
		
		if targetIdx < len(sorted) && sorted[targetIdx] == rowID {
			record, err := e.codec.DecodeRow(iter.Value(), schemaDef)
			if err != nil {
				return nil, fmt.Errorf("decode row %d: %w", rowID, err)
			}
			result[rowID] = record
			targetIdx++
		}
	}

	return result, nil
}

func sortInt64Slice(arr []int64) {
	for i := 0; i < len(arr)-1; i++ {
		for j := i + 1; j < len(arr); j++ {
			if arr[i] > arr[j] {
				arr[i], arr[j] = arr[j], arr[i]
			}
		}
	}
}

func (e *pebbleEngine) InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error) {
	rowID, err := e.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}
	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := e.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}
	if err := e.kv.Set(ctx, key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}
	if err := e.updateIndexes(ctx, tenantID, tableID, rowID, row, schemaDef, true); err != nil {
		return 0, fmt.Errorf("update indexes: %w", err)
	}
	return rowID, nil
}

func (e *pebbleEngine) InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*types.Record, schemaDef *types.TableDefinition) ([]int64, error) {
	if len(rows) == 0 {
		return []int64{}, nil
	}

	rowIDs := make([]int64, len(rows))
	for i := range rows {
		rowID, err := e.NextRowID(ctx, tenantID, tableID)
		if err != nil {
			return nil, fmt.Errorf("generate row id: %w", err)
		}
		rowIDs[i] = rowID
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	for i, row := range rows {
		key := e.codec.EncodeTableKey(tenantID, tableID, rowIDs[i])
		value, err := e.codec.EncodeRow(row, schemaDef)
		if err != nil {
			return nil, fmt.Errorf("encode row %d: %w", i, err)
		}

		if err := batch.Set(key, value); err != nil {
			return nil, fmt.Errorf("batch set row %d: %w", i, err)
		}

		if err := e.batchUpdateIndexes(batch, tenantID, tableID, rowIDs[i], row, schemaDef); err != nil {
			return nil, fmt.Errorf("batch update indexes for row %d: %w", i, err)
		}
	}

	if err := e.kv.Commit(ctx, batch); err != nil {
		return nil, fmt.Errorf("commit batch: %w", err)
	}

	return rowIDs, nil
}

func (e *pebbleEngine) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error {
	oldRow, err := e.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	if err := e.deleteIndexesInBatch(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("delete old indexes in batch: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := e.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := batch.Set(key, value); err != nil {
		return fmt.Errorf("batch set row: %w", err)
	}

	if err := e.batchUpdateIndexes(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("update indexes in batch: %w", err)
	}

	if err := e.kv.CommitBatch(ctx, batch); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

func (e *pebbleEngine) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error {
	oldRow, err := e.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get row: %w", err)
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	if err := batch.Delete(key); err != nil {
		return fmt.Errorf("batch delete row: %w", err)
	}

	if err := e.deleteIndexesInBatch(batch, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("delete indexes in batch: %w", err)
	}

	if err := e.kv.CommitBatch(ctx, batch); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

func (e *pebbleEngine) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *types.IndexDefinition) error {
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

func (e *pebbleEngine) ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *types.TableDefinition, opts *ScanOptions) (RowIterator, error) {
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

	return &rowIterator{
		iter:      iter,
		codec:     e.codec,
		schemaDef: schemaDef,
		opts:      opts,
		count:     0,
		engine:    e,
	}, nil
}

func (e *pebbleEngine) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *types.TableDefinition, opts *ScanOptions) (RowIterator, error) {
	// Find the index definition to get column types
	var indexDef *types.IndexDefinition
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
	columnTypes := make([]types.ColumnType, len(indexDef.Columns))
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
		return &indexOnlyIterator{
			iter:        iter,
			codec:       e.codec,
			indexDef:    indexDef,
			projection:  opts.Projection,
			opts:        opts,
			columnTypes: columnTypes,
			tenantID:    tenantID,
			tableID:     tableID,
			indexID:     indexID,
			engine:      e,
		}, nil
	}

	return &indexIterator{
		iter:        iter,
		codec:       e.codec,
		schemaDef:   schemaDef,
		opts:        opts,
		count:       0,
		columnTypes: columnTypes,
		engine:      e,
		tenantID:    tenantID,
		tableID:     tableID,
	}, nil
}

func (e *pebbleEngine) BeginTx(ctx context.Context) (Transaction, error) {
	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	return &transaction{
		kvTxn:     kvTxn,
		codec:     e.codec,
		engine:    e,
		isolation: storage.ReadCommitted,
	}, nil
}

func (e *pebbleEngine) BeginTxWithIsolation(ctx context.Context, level storage.IsolationLevel) (Transaction, error) {
	if level >= storage.RepeatableRead {
		return e.newSnapshotTx(ctx, level)
	}

	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	if err := kvTxn.SetIsolation(level); err != nil {
		kvTxn.Rollback()
		return nil, fmt.Errorf("set isolation level: %w", err)
	}

	return &transaction{
		kvTxn:     kvTxn,
		codec:     e.codec,
		engine:    e,
		isolation: level,
	}, nil
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

func (e *pebbleEngine) Close() error {
	return e.kv.Close()
}

// buildIndexRangeFromFilter constructs index scan range based on filter expression
func (e *pebbleEngine) buildIndexRangeFromFilter(tenantID, tableID, indexID int64, filter *FilterExpression, indexDef *types.IndexDefinition) ([]byte, []byte) {
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
func (e *pebbleEngine) extractSimpleFilter(filter *FilterExpression, columnName string) *FilterExpression {
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
func (e *pebbleEngine) buildRangeFromSimpleFilter(tenantID, tableID, indexID int64, filter *FilterExpression) ([]byte, []byte) {
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

// EvaluateFilter evaluates a filter expression against a record
func (e *pebbleEngine) EvaluateFilter(filter *FilterExpression, record *types.Record) bool {
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
func (e *pebbleEngine) evaluateSimpleFilter(filter *FilterExpression, record *types.Record) bool {
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

// isIndexCovering checks if an index covers all projection columns
func (e *pebbleEngine) isIndexCovering(indexDef *types.IndexDefinition, projection []string) bool {
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

func (e *pebbleEngine) updateIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *types.Record, schemaDef *types.TableDefinition, isInsert bool) error {
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

func (e *pebbleEngine) batchUpdateIndexes(batch storage.Batch, tenantID, tableID, rowID int64, row *types.Record, schemaDef *types.TableDefinition) error {
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

func (e *pebbleEngine) batchUpdateIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*types.Record, schemaDef *types.TableDefinition) error {
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

func (e *pebbleEngine) deleteIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *types.Record, schemaDef *types.TableDefinition) error {
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

func (e *pebbleEngine) deleteIndexesInBatch(batch storage.Batch, tenantID, tableID, rowID int64, row *types.Record, schemaDef *types.TableDefinition) error {
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

func (e *pebbleEngine) deleteIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*types.Record, schemaDef *types.TableDefinition) error {
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

type indexIterator struct {
	iter        storage.Iterator
	codec       codec.Codec
	schemaDef   *types.TableDefinition
	opts        *ScanOptions
	current     *types.Record
	err         error
	count       int
	started     bool
	columnTypes []types.ColumnType
	engine      *pebbleEngine
	tenantID    int64
	tableID     int64
	
	batchSize   int
	rowIDBuffer []int64
	rowCache    map[int64]*types.Record
	cacheIdx    int
}

// indexOnlyIterator implements covering index scan without table access
type indexOnlyIterator struct {
	iter        storage.Iterator
	codec       codec.Codec
	indexDef    *types.IndexDefinition
	projection  []string
	opts        *ScanOptions
	columnTypes []types.ColumnType
	tenantID    int64
	tableID     int64
	indexID     int64
	engine      *pebbleEngine
	current     *types.Record
	err         error
	count       int
	started     bool
}

func (io *indexOnlyIterator) Next() bool {
	if io.opts != nil && io.opts.Limit > 0 && io.count >= io.opts.Limit {
		return false
	}

	// Use loop instead of recursion to avoid stack overflow and correctly advance iterator
	for {
		var hasNext bool
		if !io.started {
			hasNext = io.iter.First()
			io.started = true

			if io.opts != nil && io.opts.Offset > 0 {
				for i := 0; i < io.opts.Offset && hasNext; i++ {
					hasNext = io.iter.Next()
				}
			}
		} else {
			hasNext = io.iter.Next()
		}

		if !hasNext {
			return false
		}

		// Decode index key to extract column values using schema-aware decoder
		_, _, _, indexValues, rowID, err := io.codec.DecodeIndexKeyWithSchema(io.iter.Key(), io.columnTypes)
		if err != nil {
			io.err = err
			return false
		}

		// Build record from index values only
		record := &types.Record{
			Data: make(map[string]*types.Value),
		}

		// Map ALL index values to columns (needed for filter evaluation)
		// We'll filter to projection columns when returning the final result
		for i, colName := range io.indexDef.Columns {
			if i < len(indexValues) {
				record.Data[colName] = &types.Value{
					Type: io.columnTypes[i],
					Data: indexValues[i],
				}
			}
		}

		// Add _rowid if needed
		record.Data["_rowid"] = &types.Value{
			Type: types.ColumnTypeNumber,
			Data: rowID,
		}

		// Apply filter if present
		if io.opts != nil && io.opts.Filter != nil {
			if !io.engine.EvaluateFilter(io.opts.Filter, record) {
				// Filter doesn't match, continue to next row
				continue
			}
		}

		// Now filter to only projection columns
		if io.projection != nil && len(io.projection) > 0 {
			filteredData := make(map[string]*types.Value)
			for _, projCol := range io.projection {
				if val, ok := record.Data[projCol]; ok {
					filteredData[projCol] = val
				}
			}
			record.Data = filteredData
		}

		io.current = record
		io.count++
		return true
	}
}

func (io *indexOnlyIterator) Row() *types.Record {
	return io.current
}

func (io *indexOnlyIterator) Error() error {
	return io.err
}

func (io *indexOnlyIterator) Close() error {
	return io.iter.Close()
}

func (ii *indexIterator) Next() bool {
	if ii.opts != nil && ii.opts.Limit > 0 && ii.count >= ii.opts.Limit {
		return false
	}

	// Use loop instead of recursion to handle filter rejection
	for {
		// Need to fetch new batch
		if len(ii.rowIDBuffer) == 0 || ii.cacheIdx >= len(ii.rowIDBuffer) {
			ii.rowIDBuffer = ii.rowIDBuffer[:0]
			ii.cacheIdx = 0

			var hasNext bool
			if !ii.started {
				// First call: initialize and position iterator
				ii.started = true
				ii.batchSize = 100
				ii.rowCache = make(map[int64]*types.Record)
				
				hasNext = ii.iter.First()
				
				// Skip offset rows
				if ii.opts != nil && ii.opts.Offset > 0 && hasNext {
					for i := 0; i < ii.opts.Offset; i++ {
						if !ii.iter.Next() {
							return false
						}
					}
					hasNext = ii.iter.Valid()
				}
			} else {
				// Subsequent batches: iterator already positioned by previous batch's loop
				hasNext = ii.iter.Valid()
			}

			// Collect rowIDs for batch fetch
			for len(ii.rowIDBuffer) < ii.batchSize && hasNext {
				_, _, _, _, rowID, err := ii.codec.DecodeIndexKey(ii.iter.Key())
				if err != nil {
					ii.err = fmt.Errorf("decode index key: %w", err)
					return false
				}
				ii.rowIDBuffer = append(ii.rowIDBuffer, rowID)
				hasNext = ii.iter.Next()
			}

			if len(ii.rowIDBuffer) == 0 {
				return false
			}

			// Fetch batch
			rowCache, err := ii.engine.GetRowBatch(context.Background(), ii.tenantID, ii.tableID, ii.rowIDBuffer, ii.schemaDef)
			if err != nil {
				ii.err = fmt.Errorf("fetch row batch: %w", err)
				return false
			}
			ii.rowCache = rowCache
		}

		// Process current batch
		if ii.cacheIdx < len(ii.rowIDBuffer) {
			rowID := ii.rowIDBuffer[ii.cacheIdx]
			ii.cacheIdx++

			row, ok := ii.rowCache[rowID]
			if !ok {
				// Row not found in cache, continue to next
				continue
			}
			
			// Apply filter evaluation
			if ii.opts != nil && ii.opts.Filter != nil {
				if !ii.engine.EvaluateFilter(ii.opts.Filter, row) {
					// Filter doesn't match, continue to next row
					continue
				}
			}

			row.Data["_rowid"] = &types.Value{
				Type: types.ColumnTypeNumber,
				Data: rowID,
			}

			ii.current = row
			ii.count++
			return true
		}

		// Batch exhausted, loop will fetch next batch
	}
}

func (ii *indexIterator) Row() *types.Record {
	return ii.current
}

func (ii *indexIterator) Error() error {
	if ii.err != nil {
		return ii.err
	}
	return ii.iter.Error()
}

func (ii *indexIterator) Close() error {
	return ii.iter.Close()
}

type rowIterator struct {
	iter      storage.Iterator
	codec     codec.Codec
	schemaDef *types.TableDefinition
	opts      *ScanOptions
	current   *types.Record
	err       error
	count     int
	started   bool
	engine    *pebbleEngine
}

func (ri *rowIterator) Next() bool {
	if ri.opts != nil && ri.opts.Limit > 0 && ri.count >= ri.opts.Limit {
		return false
	}

	var hasNext bool
	if !ri.started {
		hasNext = ri.iter.First()
		ri.started = true

		if ri.opts != nil && ri.opts.Offset > 0 {
			for i := 0; i < ri.opts.Offset && hasNext; i++ {
				hasNext = ri.iter.Next()
			}
		}
	} else {
		hasNext = ri.iter.Next()
	}

	if !hasNext {
		return false
	}

	_, _, rowID, err := ri.codec.DecodeTableKey(ri.iter.Key())
	if err != nil {
		ri.err = fmt.Errorf("decode table key: %w", err)
		return false
	}

	value := ri.iter.Value()
	record, err := ri.codec.DecodeRow(value, ri.schemaDef)
	if err != nil {
		ri.err = fmt.Errorf("decode row: %w", err)
		return false
	}

	record.Data["_rowid"] = &types.Value{
		Type: types.ColumnTypeNumber,
		Data: rowID,
	}
	
	// Apply filter if present
	if ri.opts != nil && ri.opts.Filter != nil && ri.engine != nil {
		if !ri.engine.EvaluateFilter(ri.opts.Filter, record) {
			return ri.Next() // Skip this row and try next
		}
	}

	ri.current = record
	ri.count++
	return true
}

func (ri *rowIterator) Row() *types.Record {
	return ri.current
}

func (ri *rowIterator) Error() error {
	if ri.err != nil {
		return ri.err
	}
	return ri.iter.Error()
}

func (ri *rowIterator) Close() error {
	return ri.iter.Close()
}

type transaction struct {
	kvTxn     storage.Transaction
	codec     codec.Codec
	engine    *pebbleEngine
	isolation storage.IsolationLevel
}

func (t *transaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error) {
	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := t.kvTxn.Get(key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, types.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := t.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

func (t *transaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error) {
	rowID, err := t.engine.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return 0, fmt.Errorf("conflict check failed: %w", err)
	}

	value, err := t.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}

	return rowID, nil
}

func (t *transaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error {
	oldRow, err := t.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return fmt.Errorf("conflict check failed: %w", err)
	}

	value, err := t.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return fmt.Errorf("update row: %w", err)
	}

	return nil
}

func (t *transaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error {
	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before deleting
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return fmt.Errorf("conflict check failed: %w", err)
	}

	if err := t.kvTxn.Delete(key); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	return nil
}

func (t *transaction) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []RowUpdate, schemaDef *types.TableDefinition) error {
	for _, update := range updates {
		if err := t.UpdateRow(ctx, tenantID, tableID, update.RowID, update.Updates, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) error {
	for _, rowID := range rowIDs {
		if err := t.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) Isolation() storage.IsolationLevel {
	return t.isolation
}

func (t *transaction) SetIsolation(level storage.IsolationLevel) error {
	if err := t.kvTxn.SetIsolation(level); err != nil {
		return err
	}
	t.isolation = level

	// Additional logic for different isolation levels can be added here
	switch level {
	case storage.ReadUncommitted:
		// Minimal consistency guarantees
	case storage.ReadCommitted:
		// Default behavior
	case storage.RepeatableRead:
		// Need to track snapshot
	case storage.SnapshotIsolation:
		// Need to create a snapshot
	case storage.Serializable:
		// Highest isolation level
	}

	return nil
}

func (t *transaction) Commit() error {
	return t.kvTxn.Commit()
}

func (t *transaction) Rollback() error {
	return t.kvTxn.Rollback()
}
