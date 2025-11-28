package pebble

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/indexes"
	"github.com/guileen/pglitedb/engine/pebble/operations/query"
	"github.com/guileen/pglitedb/idgen"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)



type pebbleEngine struct {
	kv                  storage.KV
	codec               codec.Codec
	idGenerator         idgen.IDGeneratorInterface
	indexManager        *indexes.Handler
	filterEvaluator     *FilterEvaluator
	multiColumnOptimizer *MultiColumnOptimizer
	queryOperations     *query.QueryOperations
	insertOperations    *query.InsertOperations
	updateOperations    *query.UpdateOperations
	deleteOperations    *query.DeleteOperations
}

func NewPebbleEngine(kvStore storage.KV, c codec.Codec) engineTypes.StorageEngine {
	queryOps := query.NewQueryOperations(kvStore, c)
	insertOps := query.NewInsertOperations(kvStore, c)
	updateOps := query.NewUpdateOperations(kvStore, c)
	deleteOps := query.NewDeleteOperations(kvStore, c)
	
	return &pebbleEngine{
		kv:                  kvStore,
		codec:               c,
		idGenerator:         idgen.NewIDGenerator(),
		indexManager:        indexes.NewHandler(kvStore, c),
		filterEvaluator:     NewFilterEvaluator(),
		multiColumnOptimizer: NewMultiColumnOptimizer(c),
		queryOperations:     queryOps,
		insertOperations:    insertOps,
		updateOperations:    updateOps,
		deleteOperations:    deleteOps,
	}
}

func (e *pebbleEngine) Close() error {
	return e.kv.Close()
}

// GetCodec returns the codec used by the engine
func (e *pebbleEngine) GetCodec() codec.Codec {
	return e.codec
}

// GetKV returns the KV store used by the engine
func (e *pebbleEngine) GetKV() storage.KV {
	return e.kv
}

// CheckForConflicts checks for conflicts with the given key
func (e *pebbleEngine) CheckForConflicts(txn storage.Transaction, key []byte) error {
	return e.kv.CheckForConflicts(txn, key)
}

func (e *pebbleEngine) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return e.idGenerator.NextRowID(ctx, tenantID, tableID)
}

func (e *pebbleEngine) NextTableID(ctx context.Context, tenantID int64) (int64, error) {
	return e.idGenerator.NextTableID(ctx, tenantID)
}

func (e *pebbleEngine) NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return e.idGenerator.NextIndexID(ctx, tenantID, tableID)
}

// DeleteRowsBatch deletes multiple rows in a single batch operation
func (e *pebbleEngine) DeleteRowsBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	// Start a transaction
	tx, err := e.BeginTx(ctx)
	if err != nil {
		return err
	}
	
	// Delegate to transaction's DeleteRowsBatch method
	err = tx.DeleteRowsBatch(ctx, tenantID, tableID, rowIDs, schemaDef)
	if err != nil {
		// Rollback the transaction on error
		tx.Rollback()
		return err
	}
	
	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return err
	}
	
	return nil
}

// DeleteRows deletes multiple rows that match the given conditions
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

// UpdateRowsBatch updates multiple rows in a single batch operation
func (e *pebbleEngine) UpdateRowsBatch(ctx context.Context, tenantID, tableID int64, rowUpdates map[int64]map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	// Start a transaction
	tx, err := e.BeginTx(ctx)
	if err != nil {
		return err
	}
	
	// Delegate to transaction's UpdateRowsBatch method
	err = tx.UpdateRowsBatch(ctx, tenantID, tableID, rowUpdates, schemaDef)
	if err != nil {
		// Rollback the transaction on error
		tx.Rollback()
		return err
	}
	
	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return err
	}
	
	return nil
}

// UpdateRows updates multiple rows that match the given conditions
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











func (e *pebbleEngine) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *dbTypes.IndexDefinition) error {
	return e.indexManager.CreateIndex(ctx, tenantID, tableID, indexDef, e.NextIndexID)
}

func (e *pebbleEngine) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	return e.indexManager.DropIndex(ctx, tenantID, tableID, indexID)
}



func (e *pebbleEngine) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	return e.indexManager.LookupIndex(ctx, tenantID, tableID, indexID, indexValue)
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

// BuildFilterExpression builds a filter expression from conditions
func (e *pebbleEngine) BuildFilterExpression(conditions map[string]interface{}) *engineTypes.FilterExpression {
	return e.buildFilterExpression(conditions)
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

// GetRow retrieves a single row by its ID
func (e *pebbleEngine) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	return e.queryOperations.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
}

// InsertRow inserts a single row
func (e *pebbleEngine) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	return e.insertOperations.InsertRow(ctx, tenantID, tableID, row, schemaDef, e.NextRowID, e.updateIndexes)
}

// UpdateRow updates a single row
func (e *pebbleEngine) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	return e.updateOperations.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef, e.GetRow, e.deleteIndexesInBatch, e.batchUpdateIndexes, e.kv.CommitBatch)
}

// DeleteRow deletes a single row
func (e *pebbleEngine) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	return e.deleteOperations.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef, e.GetRow, e.deleteIndexesInBatch, e.kv.CommitBatch)
}

// GetRowBatch retrieves multiple rows by their IDs
func (e *pebbleEngine) GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error) {
	return e.queryOperations.GetRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)
}

// InsertRowBatch inserts multiple rows in a batch
func (e *pebbleEngine) InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]int64, error) {
	return e.insertOperations.InsertRowBatch(ctx, tenantID, tableID, rows, schemaDef, e.NextRowID, e.batchUpdateIndexes, e.kv.Commit)
}

// UpdateRowBatch updates multiple rows in a batch
func (e *pebbleEngine) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	return e.updateOperations.UpdateRowBatch(ctx, tenantID, tableID, updates, schemaDef, e.GetRowBatch, e.deleteIndexesBulk, e.batchUpdateIndexesBulk, e.kv.CommitBatchWithOptions)
}

// DeleteRowBatch deletes multiple rows in a batch
func (e *pebbleEngine) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	return e.deleteOperations.DeleteRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef, e.GetRowBatch, e.deleteIndexesBulk, e.kv.CommitBatchWithOptions)
}

var _ engineTypes.StorageEngine = (*pebbleEngine)(nil)