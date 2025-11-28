package pebble

import (
	"context"

	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

func (e *pebbleEngine) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *dbTypes.IndexDefinition) error {
	return e.indexManager.CreateIndex(ctx, tenantID, tableID, indexDef, e.NextIndexID)
}

func (e *pebbleEngine) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	return e.indexManager.DropIndex(ctx, tenantID, tableID, indexID)
}

func (e *pebbleEngine) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	return e.indexManager.LookupIndex(ctx, tenantID, tableID, indexID, indexValue)
}

func (e *pebbleEngine) updateIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition, isInsert bool) error {
	return e.indexManager.UpdateIndexes(ctx, tenantID, tableID, rowID, row, schemaDef, isInsert)
}

func (e *pebbleEngine) batchUpdateIndexes(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	return e.indexManager.BatchUpdateIndexes(batch, tenantID, tableID, rowID, row, schemaDef)
}

func (e *pebbleEngine) batchUpdateIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	if schemaDef.Indexes == nil || len(rows) == 0 {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

		for rowID, row := range rows {
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
					indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
				} else {
					indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
				}

				if err != nil {
					return err
				}

				if err := batch.Set(indexKey, []byte{}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (e *pebbleEngine) deleteIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	return e.indexManager.DeleteIndexes(ctx, tenantID, tableID, rowID, row, schemaDef)
}

func (e *pebbleEngine) deleteIndexesInBatch(batch storage.Batch, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	return e.indexManager.DeleteIndexesInBatch(batch, tenantID, tableID, rowID, row, schemaDef)
}

func (e *pebbleEngine) deleteIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	return e.indexManager.DeleteIndexesBulk(batch, tenantID, tableID, rows, schemaDef)
}