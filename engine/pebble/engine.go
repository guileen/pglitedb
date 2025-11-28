package pebble

import (
	"context"
	"encoding/json"
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
	kv             storage.KV
	codec          codec.Codec
	idGenerator    *IDGenerator
	indexManager   *IndexManager
	filterEvaluator *FilterEvaluator

	tableIDCounters map[int64]*int64
	indexIDCounters map[string]*int64
}

func NewPebbleEngine(kvStore storage.KV, c codec.Codec) engineTypes.StorageEngine {
	// Initialize the indexIDCounters map with a default counter for each tenant:table combination
	indexIDCounters := make(map[string]*int64)
	
	return &pebbleEngine{
		kv:              kvStore,
		codec:           c,
		idGenerator:     NewIDGenerator(),
		indexManager:    NewIndexManager(kvStore, c),
		filterEvaluator: NewFilterEvaluator(),
		tableIDCounters: make(map[int64]*int64),
		indexIDCounters: indexIDCounters,
	}
}

func (e *pebbleEngine) Close() error {
	return e.kv.Close()
}

func (e *pebbleEngine) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return e.idGenerator.NextRowID(ctx, tenantID, tableID)
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
	metaKey := e.codec.EncodeMetaKey(tenantID, "index", fmt.Sprintf("%d_%s", tableID, indexDef.Name))
	
	// Serialize index definition
	indexData, err := json.Marshal(indexDef)
	if err != nil {
		return fmt.Errorf("serialize index definition: %w", err)
	}
	
	// Store metadata
	if err := e.kv.Set(ctx, metaKey, indexData); err != nil {
		return fmt.Errorf("store index metadata: %w", err)
	}
	
	// Build index entries for existing data
	if err := e.buildIndexEntries(ctx, tenantID, tableID, indexID, indexDef); err != nil {
		// Clean up metadata on failure
		e.kv.Delete(ctx, metaKey)
		return fmt.Errorf("build index entries: %w", err)
	}

	return nil
}

func (e *pebbleEngine) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	// Remove index metadata
	metaKey := e.codec.EncodeMetaKey(tenantID, "index", fmt.Sprintf("%d_%d", tableID, indexID))
	if err := e.kv.Delete(ctx, metaKey); err != nil {
		// Log error but continue with cleanup
		fmt.Printf("Warning: failed to delete index metadata: %v\n", err)
	}
	
	// Remove all index entries from storage
	startKey := e.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
	endKey := e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	
	// Create a batch to delete all index entries
	batch := e.kv.NewBatch()
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()
	
	iter := e.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()
	
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key()); err != nil {
			return fmt.Errorf("batch delete index entry: %w", err)
		}
	}
	
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}
	
	// Commit the batch
	err := e.kv.CommitBatch(ctx, batch)
	batch = nil // Prevent double close
	return err
}

// buildIndexEntries creates index entries for all existing rows in a table
func (e *pebbleEngine) buildIndexEntries(ctx context.Context, tenantID, tableID, indexID int64, indexDef *dbTypes.IndexDefinition) error {
	// Scan all rows in the table
	startKey := e.codec.EncodeTableKey(tenantID, tableID, 0)
	endKey := e.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
	
	iter := e.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()
	
	batch := e.kv.NewBatch()
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()
	
	for iter.First(); iter.Valid(); iter.Next() {
		// Decode the row
		_, _, rowID, err := e.codec.DecodeTableKey(iter.Key())
		if err != nil {
			continue // Skip invalid keys
		}
		
		// For simplicity, we'll skip decoding the full row and just create a placeholder
		// In a real implementation, we would decode the row and extract index values
		indexKey, err := e.codec.EncodeIndexKey(tenantID, tableID, indexID, "placeholder", rowID)
		if err != nil {
			continue // Skip on encoding errors
		}
		
		if err := batch.Set(indexKey, []byte{}); err != nil {
			return fmt.Errorf("batch set index entry: %w", err)
		}
	}
	
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}
	
	// Commit the batch
	err := e.kv.CommitBatch(ctx, batch)
	batch = nil // Prevent double close
	return err
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
	return e.indexManager.UpdateIndexes(ctx, tenantID, tableID, rowID, row, schemaDef, isInsert)
}

func (e *pebbleEngine) batchUpdateIndexes(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	return e.indexManager.BatchUpdateIndexes(batch, tenantID, tableID, rowID, row, schemaDef)
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
	return e.indexManager.DeleteIndexes(ctx, tenantID, tableID, rowID, row, schemaDef)
}

func (e *pebbleEngine) deleteIndexesInBatch(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	return e.indexManager.DeleteIndexesInBatch(batch, tenantID, tableID, rowID, row, schemaDef)
}

func (e *pebbleEngine) deleteIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	return e.indexManager.DeleteIndexesBulk(batch, tenantID, tableID, rows, schemaDef)
}

// EvaluateFilter evaluates a filter expression against a record
func (e *pebbleEngine) EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool {
	return e.filterEvaluator.EvaluateFilter(filter, record)
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