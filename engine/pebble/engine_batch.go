package pebble

import (
	"context"

	dbTypes "github.com/guileen/pglitedb/types"
)

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