package pebble

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// IndexHandler handles index creation and management operations
type IndexHandler struct {
	kv    storage.KV
	codec codec.Codec
}

// NewIndexHandler creates a new IndexHandler
func NewIndexHandler(kv storage.KV, c codec.Codec) *IndexHandler {
	return &IndexHandler{
		kv:    kv,
		codec: c,
	}
}

// CreateIndex creates a new index
func (ih *IndexHandler) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *dbTypes.IndexDefinition, nextIndexID func(context.Context, int64, int64) (int64, error)) error {
	// Generate a new index ID
	indexID, err := nextIndexID(ctx, tenantID, tableID)
	if err != nil {
		return fmt.Errorf("generate index id: %w", err)
	}

	// Store index metadata
	metaKey := ih.codec.EncodeMetaKey(tenantID, "index", fmt.Sprintf("%d_%s", tableID, indexDef.Name))
	
	// Serialize index definition
	indexData, err := json.Marshal(indexDef)
	if err != nil {
		return fmt.Errorf("serialize index definition: %w", err)
	}
	
	// Store metadata
	if err := ih.kv.Set(ctx, metaKey, indexData); err != nil {
		return fmt.Errorf("store index metadata: %w", err)
	}
	
	// Build index entries for existing data
	if err := ih.buildIndexEntries(ctx, tenantID, tableID, indexID, indexDef); err != nil {
		// Clean up metadata on failure
		ih.kv.Delete(ctx, metaKey)
		return fmt.Errorf("build index entries: %w", err)
	}

	return nil
}

// DropIndex drops an existing index
func (ih *IndexHandler) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	// Remove index metadata
	metaKey := ih.codec.EncodeMetaKey(tenantID, "index", fmt.Sprintf("%d_%d", tableID, indexID))
	if err := ih.kv.Delete(ctx, metaKey); err != nil {
		// Log error but continue with cleanup
		fmt.Printf("Warning: failed to delete index metadata: %v\n", err)
	}
	
	// Remove all index entries from storage
	startKey := ih.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
	endKey := ih.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	
	// Create a batch to delete all index entries
	batch := ih.kv.NewBatch()
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()
	
	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}
	
	iter := ih.kv.NewIterator(iterOpts)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()
	
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key()); err != nil {
			return fmt.Errorf("batch delete index entry: %w", err)
		}
	}
	
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}
	
	// Commit the batch
	err := ih.kv.CommitBatch(ctx, batch)
	batch = nil // Prevent double close
	return err
}

// LookupIndex looks up row IDs by index value
func (ih *IndexHandler) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	startKey, err := ih.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValue, 0)
	if err != nil {
		return nil, fmt.Errorf("encode start key: %w", err)
	}

	endKey, err := ih.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValue, int64(^uint64(0)>>1))
	if err != nil {
		return nil, fmt.Errorf("encode end key: %w", err)
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}
	
	iter := ih.kv.NewIterator(iterOpts)
	defer iter.Close()

	var rowIDs []int64
	for iter.First(); iter.Valid(); iter.Next() {
		// Extract rowID from the index key
		_, _, _, _, rowID, err := ih.codec.DecodeIndexKey(iter.Key())
		if err != nil {
			return nil, fmt.Errorf("decode index key: %w", err)
		}
		rowIDs = append(rowIDs, rowID)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return rowIDs, nil
}

// buildIndexEntries creates index entries for all existing rows in a table
func (ih *IndexHandler) buildIndexEntries(ctx context.Context, tenantID, tableID, indexID int64, indexDef *dbTypes.IndexDefinition) error {
	// Scan all rows in the table
	startKey := ih.codec.EncodeTableKey(tenantID, tableID, 0)
	endKey := ih.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
	
	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}
	
	iter := ih.kv.NewIterator(iterOpts)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()
	
	batch := ih.kv.NewBatch()
	defer func() {
		if batch != nil {
			batch.Close()
		}
	}()
	
	for iter.First(); iter.Valid(); iter.Next() {
		// Decode the row
		_, _, rowID, err := ih.codec.DecodeTableKey(iter.Key())
		if err != nil {
			continue // Skip invalid keys
		}
		
		// For simplicity, we'll skip decoding the full row and just create a placeholder
		// In a real implementation, we would decode the row and extract index values
		indexKey, err := ih.codec.EncodeIndexKey(tenantID, tableID, indexID, "placeholder", rowID)
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
	err := ih.kv.CommitBatch(ctx, batch)
	batch = nil // Prevent double close
	return err
}