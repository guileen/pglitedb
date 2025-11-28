package pebble

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// IndexManager handles all index-related operations
type IndexManager struct {
	kv    storage.KV
	codec codec.Codec
}

// NewIndexManager creates a new IndexManager
func NewIndexManager(kv storage.KV, c codec.Codec) *IndexManager {
	return &IndexManager{
		kv:    kv,
		codec: c,
	}
}

// UpdateIndexes updates all indexes for a row
func (im *IndexManager) UpdateIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition, isInsert bool) error {
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
				indexKey, err = im.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = im.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if isInsert {
				if err := im.kv.Set(ctx, indexKey, []byte{}); err != nil {
					return fmt.Errorf("set index: %w", err)
				}
			} else {
				// For updates, we need to delete the old index entry and create a new one
				// This is handled at a higher level by deleting indexes before updating
				if err := im.kv.Set(ctx, indexKey, []byte{}); err != nil {
					return fmt.Errorf("set index: %w", err)
				}
			}
		}
	}

	return nil
}

// BatchUpdateIndexes updates all indexes for a row in a batch
func (im *IndexManager) BatchUpdateIndexes(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
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
				indexKey, err = im.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = im.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
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
func (im *IndexManager) BatchUpdateIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	for rowID, row := range rows {
		if err := im.BatchUpdateIndexes(batch, tenantID, tableID, rowID, row, schemaDef); err != nil {
			return err
		}
	}
	return nil
}

// DeleteIndexes deletes all index entries for a row
func (im *IndexManager) DeleteIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
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
				indexKey, err = im.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = im.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := im.kv.Delete(ctx, indexKey); err != nil {
				return fmt.Errorf("delete index: %w", err)
			}
		}
	}

	return nil
}

// DeleteIndexesInBatch deletes all index entries for a row in a batch
func (im *IndexManager) DeleteIndexesInBatch(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
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
				indexKey, err = im.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = im.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := batch.Delete(indexKey); err != nil {
				return fmt.Errorf("batch delete index: %w", err)
			}
		}
	}

	return nil
}

// DeleteIndexesBulk deletes all index entries for multiple rows in a batch
func (im *IndexManager) DeleteIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	for rowID, row := range rows {
		if err := im.DeleteIndexesInBatch(batch, tenantID, tableID, rowID, row, schemaDef); err != nil {
			return err
		}
	}
	return nil
}