package pebble

import (
	"context"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
)

func (e *pebbleEngine) ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	scanner := NewScanner(e.kv, e.codec, e)
	return scanner.ScanRows(ctx, tenantID, tableID, schemaDef, opts)
}

func (e *pebbleEngine) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	scanner := NewScanner(e.kv, e.codec, e)
	return scanner.ScanIndex(ctx, tenantID, tableID, indexID, schemaDef, opts)
}