package modify

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// Deleter handles row deletion operations
type Deleter struct {
	kv    storage.KV
	codec codec.Codec
}

// NewDeleter creates a new deleter
func NewDeleter(kv storage.KV, codec codec.Codec) *Deleter {
	return &Deleter{
		kv:    kv,
		codec: codec,
	}
}

// DeleteRow deletes a single row
func (d *Deleter) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) error {
	key := d.codec.EncodeTableKey(tenantID, tableID, rowID)
	if err := d.kv.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}
	return nil
}

// DeleteRowBatch deletes multiple rows in a batch
func (d *Deleter) DeleteRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	if len(rowIDs) == 0 {
		return nil
	}

	batch := d.kv.NewBatch()
	defer batch.Close()

	for _, rowID := range rowIDs {
		key := d.codec.EncodeTableKey(tenantID, tableID, rowID)
		if err := batch.Delete(key); err != nil {
			return fmt.Errorf("batch delete row %d: %w", rowID, err)
		}
	}

	if err := d.kv.CommitBatch(ctx, batch); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}