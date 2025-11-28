package pebble

import (
	"context"
	"fmt"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
)

// RowHandler handles row operations
type RowHandler struct {
	buildFilterExpression func(map[string]interface{}) *engineTypes.FilterExpression
}

// NewRowHandler creates a new RowHandler
func NewRowHandler(buildFilterExpression func(map[string]interface{}) *engineTypes.FilterExpression) *RowHandler {
	return &RowHandler{
		buildFilterExpression: buildFilterExpression,
	}
}

// UpdateRows updates multiple rows that match the given conditions
func (rh *RowHandler) UpdateRows(
	ctx context.Context,
	tenantID, tableID int64,
	updates map[string]*dbTypes.Value,
	conditions map[string]interface{},
	schemaDef *dbTypes.TableDefinition,
	scanRows func(context.Context, int64, int64, *dbTypes.TableDefinition, *engineTypes.ScanOptions) (engineTypes.RowIterator, error),
	updateRowImpl func(context.Context, int64, int64, int64, map[string]*dbTypes.Value, *dbTypes.TableDefinition) error,
	updateRowsBatchImpl func(context.Context, int64, int64, map[int64]map[string]*dbTypes.Value, *dbTypes.TableDefinition) error,
) (int64, error) {
	// Create a scan options with the conditions as filter
	scanOpts := &engineTypes.ScanOptions{
		Filter: rh.buildFilterExpression(conditions),
	}
	
	// Scan for matching rows
	iter, err := scanRows(ctx, tenantID, tableID, schemaDef, scanOpts)
	if err != nil {
		return 0, fmt.Errorf("scan rows: %w", err)
	}
	defer iter.Close()
	
	// Collect all matching row IDs and their updates
	rowUpdates := make(map[int64]map[string]*dbTypes.Value)
	for iter.Next() {
		record := iter.Row()
		// Extract row ID from the record's _rowid field
		rowIDVal, ok := record.Data["_rowid"]
		if !ok {
			return 0, fmt.Errorf("missing _rowid in record")
		}
		rowID, ok := rowIDVal.Data.(int64)
		if !ok {
			return 0, fmt.Errorf("_rowid is not an int64")
		}
		rowUpdates[rowID] = updates
	}
	
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterator error: %w", err)
	}
	
	// If no rows match, return early
	if len(rowUpdates) == 0 {
		return 0, nil
	}
	
	// Use batch update if available and there are multiple rows
	if updateRowsBatchImpl != nil && len(rowUpdates) > 1 {
		if err := updateRowsBatchImpl(ctx, tenantID, tableID, rowUpdates, schemaDef); err != nil {
			return 0, fmt.Errorf("batch update rows: %w", err)
		}
		return int64(len(rowUpdates)), nil
	}
	
	// Fallback to individual updates
	var count int64
	for rowID, rowUpdates := range rowUpdates {
		if err := updateRowImpl(ctx, tenantID, tableID, rowID, rowUpdates, schemaDef); err != nil {
			return count, fmt.Errorf("update row %d: %w", rowID, err)
		}
		count++
	}
	
	return count, nil
}

// DeleteRows deletes multiple rows that match the given conditions
func (rh *RowHandler) DeleteRows(
	ctx context.Context,
	tenantID, tableID int64,
	conditions map[string]interface{},
	schemaDef *dbTypes.TableDefinition,
	scanRows func(context.Context, int64, int64, *dbTypes.TableDefinition, *engineTypes.ScanOptions) (engineTypes.RowIterator, error),
	deleteRowImpl func(context.Context, int64, int64, int64, *dbTypes.TableDefinition) error,
	deleteRowsBatchImpl func(context.Context, int64, int64, []int64, *dbTypes.TableDefinition) error,
) (int64, error) {
	// Create a scan options with the conditions as filter
	scanOpts := &engineTypes.ScanOptions{
		Filter: rh.buildFilterExpression(conditions),
	}
	
	// Scan for matching rows
	iter, err := scanRows(ctx, tenantID, tableID, schemaDef, scanOpts)
	if err != nil {
		return 0, fmt.Errorf("scan rows: %w", err)
	}
	defer iter.Close()
	
	// Collect all matching row IDs
	var rowIDs []int64
	for iter.Next() {
		record := iter.Row()
		// Extract row ID from the record's _rowid field
		rowIDVal, ok := record.Data["_rowid"]
		if !ok {
			return 0, fmt.Errorf("missing _rowid in record")
		}
		rowID, ok := rowIDVal.Data.(int64)
		if !ok {
			return 0, fmt.Errorf("_rowid is not an int64")
		}
		rowIDs = append(rowIDs, rowID)
	}
	
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("iterator error: %w", err)
	}
	
	// If no rows match, return early
	if len(rowIDs) == 0 {
		return 0, nil
	}
	
	// Use batch delete if available and there are multiple rows
	if deleteRowsBatchImpl != nil && len(rowIDs) > 1 {
		if err := deleteRowsBatchImpl(ctx, tenantID, tableID, rowIDs, schemaDef); err != nil {
			return 0, fmt.Errorf("batch delete rows: %w", err)
		}
		return int64(len(rowIDs)), nil
	}
	
	// Fallback to individual deletions
	var count int64
	for _, rowID := range rowIDs {
		if err := deleteRowImpl(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return count, fmt.Errorf("delete row %d: %w", rowID, err)
		}
		count++
	}
	
	return count, nil
}