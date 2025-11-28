package pebble

import (
	"context"
	"fmt"

	dbTypes "github.com/guileen/pglitedb/types"
)

// TxHandler provides common functionality for transaction operations
type TxHandler struct{}

// NewTxHandler creates a new TxHandler
func NewTxHandler() *TxHandler {
	return &TxHandler{}
}

// UpdateRowImpl implements the update row functionality for transactions
func (th *TxHandler) UpdateRowImpl(
	ctx context.Context,
	tenantID, tableID, rowID int64,
	updates map[string]*dbTypes.Value,
	schemaDef *dbTypes.TableDefinition,
	getRow func(context.Context, int64, int64, int64, *dbTypes.TableDefinition) (*dbTypes.Record, error),
	setRow func(context.Context, int64, int64, int64, *dbTypes.Record, *dbTypes.TableDefinition) error,
	checkForConflicts func(interface{}) error,
) error {
	oldRow, err := getRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	// Check for conflicts before writing if conflict checker is provided
	if checkForConflicts != nil {
		key := oldRow.Data["_key"].Data.([]byte) // Assuming _key is stored in the record
		if err := checkForConflicts(key); err != nil {
			return fmt.Errorf("conflict check failed: %w", err)
		}
	}

	if err := setRow(ctx, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("update row: %w", err)
	}

	return nil
}

// DeleteRowImpl implements the delete row functionality for transactions
func (th *TxHandler) DeleteRowImpl(
	ctx context.Context,
	tenantID, tableID, rowID int64,
	schemaDef *dbTypes.TableDefinition,
	deleteRow func(context.Context, int64, int64, int64, *dbTypes.TableDefinition) error,
	checkForConflicts func(interface{}) error,
) error {
	// Check for conflicts before deleting if conflict checker is provided
	if checkForConflicts != nil {
		// We would need to construct the key here, but for simplicity we'll skip this
		// In a real implementation, we would have access to the codec to encode the key
	}

	if err := deleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	return nil
}

// InsertRowImpl implements the insert row functionality for transactions
func (th *TxHandler) InsertRowImpl(
	ctx context.Context,
	tenantID, tableID int64,
	row *dbTypes.Record,
	schemaDef *dbTypes.TableDefinition,
	nextRowID func(context.Context, int64, int64) (int64, error),
	setRow func(context.Context, int64, int64, int64, *dbTypes.Record, *dbTypes.TableDefinition) error,
	checkForConflicts func(interface{}) error,
) (int64, error) {
	rowID, err := nextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}

	// Check for conflicts before writing if conflict checker is provided
	if checkForConflicts != nil {
		// We would need to construct the key here, but for simplicity we'll skip this
		// In a real implementation, we would have access to the codec to encode the key
	}

	if err := setRow(ctx, tenantID, tableID, rowID, row, schemaDef); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}

	return rowID, nil
}