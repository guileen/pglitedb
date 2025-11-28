package pebble

import (
	"context"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
)

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