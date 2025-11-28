package modify

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// Updater handles row update operations
type Updater struct {
	kv    storage.KV
	codec codec.Codec
}

// NewUpdater creates a new updater
func NewUpdater(kv storage.KV, codec codec.Codec) *Updater {
	return &Updater{
		kv:    kv,
		codec: codec,
	}
}

// UpdateRow updates a single row
func (u *Updater) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*dbTypes.Value, schemaDef *dbTypes.TableDefinition) error {
	oldRow, err := u.getRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := u.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := u.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := u.kv.Set(ctx, key, value); err != nil {
		return fmt.Errorf("update row: %w", err)
	}

	return nil
}

// UpdateRowBatch updates multiple rows in a batch
func (u *Updater) UpdateRowBatch(ctx context.Context, tenantID, tableID int64, updates []RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	if len(updates) == 0 {
		return nil
	}

	rowIDs := make([]int64, len(updates))
	for i, update := range updates {
		rowIDs[i] = update.RowID
	}

	oldRows, err := u.getRowBatch(ctx, tenantID, tableID, rowIDs, schemaDef)
	if err != nil {
		return fmt.Errorf("get old rows: %w", err)
	}

	batch := u.kv.NewBatch()
	defer batch.Close()

	updatedRows := make(map[int64]*dbTypes.Record, len(updates))
	for _, update := range updates {
		oldRow, ok := oldRows[update.RowID]
		if !ok {
			return fmt.Errorf("row %d not found", update.RowID)
		}

		for colName, newValue := range update.Updates {
			oldRow.Data[colName] = newValue
		}
		updatedRows[update.RowID] = oldRow

		value, err := u.codec.EncodeRow(oldRow, schemaDef)
		if err != nil {
			return fmt.Errorf("encode row %d: %w", update.RowID, err)
		}

		key := u.codec.EncodeTableKey(tenantID, tableID, update.RowID)
		if err := batch.Set(key, value); err != nil {
			return fmt.Errorf("batch set row %d: %w", update.RowID, err)
		}
	}

	if err := u.kv.CommitBatch(ctx, batch); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// getRow retrieves a single row by its ID
func (u *Updater) getRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	key := u.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := u.kv.Get(ctx, key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, fmt.Errorf("record not found")
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := u.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

// getRowBatch retrieves multiple rows by their IDs
func (u *Updater) getRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error) {
	// This is a simplified implementation
	// In a real implementation, this would be optimized
	result := make(map[int64]*dbTypes.Record)
	for _, rowID := range rowIDs {
		row, err := u.getRow(ctx, tenantID, tableID, rowID, schemaDef)
		if err != nil {
			return nil, err
		}
		result[rowID] = row
	}
	return result, nil
}

// RowUpdate represents a row update operation
type RowUpdate struct {
	RowID   int64
	Updates map[string]*dbTypes.Value
}