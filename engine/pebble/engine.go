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
	kv                  storage.KV
	codec               codec.Codec
	idGenerator         *IDGenerator
	indexManager        *IndexManager
	filterEvaluator     *FilterEvaluator
	multiColumnOptimizer *MultiColumnOptimizer

	tableIDCounters map[int64]*int64
	indexIDCounters map[string]*int64
}

func NewPebbleEngine(kvStore storage.KV, c codec.Codec) engineTypes.StorageEngine {
	// Initialize the indexIDCounters map with a default counter for each tenant:table combination
	indexIDCounters := make(map[string]*int64)
	
	return &pebbleEngine{
		kv:                  kvStore,
		codec:               c,
		idGenerator:         NewIDGenerator(),
		indexManager:        NewIndexManager(kvStore, c),
		filterEvaluator:     NewFilterEvaluator(),
		multiColumnOptimizer: NewMultiColumnOptimizer(c),
		tableIDCounters:     make(map[int64]*int64),
		indexIDCounters:     indexIDCounters,
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
	scanner := NewScanner(e.kv, e.codec, e)
	return scanner.ScanRows(ctx, tenantID, tableID, schemaDef, opts)
}

func (e *pebbleEngine) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	scanner := NewScanner(e.kv, e.codec, e)
	return scanner.ScanIndex(ctx, tenantID, tableID, indexID, schemaDef, opts)
}

// buildIndexRangeFromFilter constructs index scan range based on filter expression
func (e *pebbleEngine) buildIndexRangeFromFilter(tenantID, tableID, indexID int64, filter *engineTypes.FilterExpression, indexDef *dbTypes.IndexDefinition) ([]byte, []byte) {
	return e.multiColumnOptimizer.buildIndexRangeFromFilter(tenantID, tableID, indexID, filter, indexDef)
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









func (e *pebbleEngine) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *dbTypes.IndexDefinition) error {
	handler := NewIndexHandler(e.kv, e.codec)
	return handler.CreateIndex(ctx, tenantID, tableID, indexDef, e.NextIndexID)
}

func (e *pebbleEngine) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	handler := NewIndexHandler(e.kv, e.codec)
	return handler.DropIndex(ctx, tenantID, tableID, indexID)
}



func (e *pebbleEngine) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	handler := NewIndexHandler(e.kv, e.codec)
	return handler.LookupIndex(ctx, tenantID, tableID, indexID, indexValue)
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