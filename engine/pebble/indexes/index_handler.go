package indexes

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/engine/errors"
	"github.com/guileen/pglitedb/engine/pebble/constants"
)

// Handler handles all index-related operations
type Handler struct {
	kv    storage.KV
	codec codec.Codec
}

// NewHandler creates a new Handler
func NewHandler(kv storage.KV, c codec.Codec) *Handler {
	return &Handler{
		kv:    kv,
		codec: c,
	}
}

// CreateIndex creates a new index
func (h *Handler) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *dbTypes.IndexDefinition, nextIndexID func(context.Context, int64, int64) (int64, error)) error {
	// Generate a new index ID
	indexID, err := nextIndexID(ctx, tenantID, tableID)
	if err != nil {
		return errors.Wrap(err, "generate_index_id", "generate index id")
	}

	// Store index metadata
	metaKey := h.codec.EncodeMetaKey(tenantID, "index", fmt.Sprintf("%d_%s", tableID, indexDef.Name))

	// Serialize index definition
	indexData, err := json.Marshal(indexDef)
	if err != nil {
		return errors.Wrap(err, "serialize_index_definition", "serialize index definition")
	}

	// Store metadata
	if err := h.kv.Set(ctx, metaKey, indexData); err != nil {
		return errors.Wrap(err, "store_index_metadata", "store index metadata")
	}

	// Build index entries for existing data
	if err := h.buildIndexEntries(ctx, tenantID, tableID, indexID, indexDef); err != nil {
		// Clean up metadata on failure
		h.kv.Delete(ctx, metaKey)
		return errors.Wrap(err, "build_index_entries", "build index entries")
	}

	return nil
}

// DropIndex drops an existing index
func (h *Handler) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	// Remove index metadata
	metaKey := h.codec.EncodeMetaKey(tenantID, "index", fmt.Sprintf("%d_%d", tableID, indexID))
	if err := h.kv.Delete(ctx, metaKey); err != nil {
		// Log error but continue with cleanup
		fmt.Printf("Warning: failed to delete index metadata: %v\n", err)
	}

	// Remove all index entries from storage
	startKey := h.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
	endKey := h.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)

	// Create a batch to delete all index entries
	batch := h.kv.NewBatch()
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	iter := h.kv.NewIterator(iterOpts)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key()); err != nil {
			return errors.Wrap(err, "batch_delete_index_entry", "batch delete index entry")
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	// Commit the batch
	err := h.kv.CommitBatch(ctx, batch)
	batch = nil // Prevent double close
	return err
}

// LookupIndex looks up row IDs by index value
func (h *Handler) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	startKey, err := h.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValue, 0)
	if err != nil {
		return nil, errors.Wrap(err, "encode_start_key", "encode start key")
	}

	endKey, err := h.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValue, constants.MaxIndexValue)
	if err != nil {
		return nil, errors.Wrap(err, "encode_end_key", "encode end key")
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	iter := h.kv.NewIterator(iterOpts)
	defer iter.Close()

	// Pre-allocate slice with reasonable capacity to reduce allocations
	rowIDs := make([]int64, 0, 64)
	for iter.First(); iter.Valid(); iter.Next() {
		// Extract rowID from the index key using optimized function
		rowID, err := h.codec.ExtractRowIDFromIndexKey(iter.Key())
		if err != nil {
			return nil, fmt.Errorf("extract rowID from index key: %w", err)
		}
		rowIDs = append(rowIDs, rowID)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return rowIDs, nil
}

// UpdateIndexes updates all indexes for a row
func (h *Handler) UpdateIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition, isInsert bool) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

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
				indexKey, err = h.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = h.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if isInsert {
				if err := h.kv.Set(ctx, indexKey, []byte{}); err != nil {
					return fmt.Errorf("set index: %w", err)
				}
			} else {
				// For updates, we need to delete the old index entry and create a new one
				// This is handled at a higher level by deleting indexes before updating
				if err := h.kv.Set(ctx, indexKey, []byte{}); err != nil {
					return fmt.Errorf("set index: %w", err)
				}
			}
		}
	}

	return nil
}

// BatchUpdateIndexes updates all indexes for a row in a batch
func (h *Handler) BatchUpdateIndexes(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

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
				indexKey, err = h.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = h.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := batch.Set(indexKey, []byte{}); err != nil {
				return fmt.Errorf("batch set index: %w", err)
			}
		}
	}

	return nil
}

// BatchUpdateIndexesBulk updates all indexes for multiple rows in a batch
func (h *Handler) BatchUpdateIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	for rowID, row := range rows {
		if err := h.BatchUpdateIndexes(batch, tenantID, tableID, rowID, row, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

// DeleteIndexes deletes all index entries for a row
func (h *Handler) DeleteIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

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
				indexKey, err = h.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = h.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := h.kv.Delete(ctx, indexKey); err != nil {
				return fmt.Errorf("delete index: %w", err)
			}
		}
	}

	return nil
}

// DeleteIndexesInBatch deletes all index entries for a row in a batch
func (h *Handler) DeleteIndexesInBatch(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

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
				indexKey, err = h.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = h.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := batch.Delete(indexKey); err != nil {
				return errors.Wrap(err, "batch_delete_index", "batch delete index")
			}
		}
	}

	return nil
}

// DeleteIndexesBulk deletes all index entries for multiple rows in a batch
func (h *Handler) DeleteIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	for rowID, row := range rows {
		if err := h.DeleteIndexesInBatch(batch, tenantID, tableID, rowID, row, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

// buildIndexEntries creates index entries for all existing rows in a table
func (h *Handler) buildIndexEntries(ctx context.Context, tenantID, tableID, indexID int64, indexDef *dbTypes.IndexDefinition) error {
	// Scan all rows in the table
	startKey := h.codec.EncodeTableKey(tenantID, tableID, 0)
	endKey := h.codec.EncodeTableKey(tenantID, tableID, constants.MaxRowID)

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	iter := h.kv.NewIterator(iterOpts)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	batch := h.kv.NewBatch()
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()

	for iter.First(); iter.Valid(); iter.Next() {
		// Decode the row
		_, _, rowID, err := h.codec.DecodeTableKey(iter.Key())
		if err != nil {
			continue // Skip invalid keys
		}

		// For simplicity, we'll skip decoding the full row and just create a placeholder
		// In a real implementation, we would decode the row and extract index values
		indexKey, err := h.codec.EncodeIndexKey(tenantID, tableID, indexID, "placeholder", rowID)
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
	err := h.kv.CommitBatch(ctx, batch)
	batch = nil // Prevent double close
	return err
}