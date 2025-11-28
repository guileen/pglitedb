package modify

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// Inserter handles row insertion operations
type Inserter struct {
	kv    storage.KV
	codec codec.Codec
}

// NewInserter creates a new inserter
func NewInserter(kv storage.KV, codec codec.Codec) *Inserter {
	return &Inserter{
		kv:    kv,
		codec: codec,
	}
}

// InsertRow inserts a single row
func (i *Inserter) InsertRow(ctx context.Context, tenantID, tableID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) (int64, error) {
	rowID, err := i.generateRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}
	key := i.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := i.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}
	if err := i.kv.Set(ctx, key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}
	return rowID, nil
}

// InsertRowBatch inserts multiple rows in a batch
func (i *Inserter) InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]int64, error) {
	if len(rows) == 0 {
		return []int64{}, nil
	}

	// Pre-allocate rowIDs slice with exact capacity
	rowIDs := make([]int64, len(rows))
	
	// Generate all row IDs first to minimize lock contention
	for j := range rows {
		rowID, err := i.generateRowID(ctx, tenantID, tableID)
		if err != nil {
			return nil, fmt.Errorf("generate row id: %w", err)
		}
		rowIDs[j] = rowID
	}

	batch := i.kv.NewBatch()
	defer batch.Close()

	// Process rows in batch with reduced allocations
	for j, row := range rows {
		key := i.codec.EncodeTableKey(tenantID, tableID, rowIDs[j])
		value, err := i.codec.EncodeRow(row, schemaDef)
		if err != nil {
			return nil, fmt.Errorf("encode row %d: %w", j, err)
		}

		if err := batch.Set(key, value); err != nil {
			return nil, fmt.Errorf("batch set row %d: %w", j, err)
		}
	}

	if err := i.kv.Commit(ctx, batch); err != nil {
		return nil, fmt.Errorf("commit batch: %w", err)
	}

	return rowIDs, nil
}

// generateRowID generates a new row ID
func (i *Inserter) generateRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	// This is a placeholder implementation
	// In a real implementation, this would use a proper ID generator
	return 0, fmt.Errorf("not implemented")
}