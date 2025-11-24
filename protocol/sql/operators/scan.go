package operators

import (
	"context"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/types"
)

type TableScanOperator struct {
	ctx       context.Context
	catalog   catalog.Manager
	tenantID  int64
	tableName string
	opts      *types.QueryOptions

	iter   engine.RowIterator
	schema *types.TableDefinition
}

func NewTableScan(ctx context.Context, catalog catalog.Manager, tenantID int64, tableName string, opts *types.QueryOptions) *TableScanOperator {
	return &TableScanOperator{
		ctx:       ctx,
		catalog:   catalog,
		tenantID:  tenantID,
		tableName: tableName,
		opts:      opts,
	}
}

func (op *TableScanOperator) Open() error {
	return nil
}

func (op *TableScanOperator) Next() (*types.Record, error) {
	result, err := op.catalog.Query(op.ctx, op.tenantID, op.tableName, op.opts)
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 {
		return nil, EOF
	}

	record := &types.Record{
		Data: make(map[string]*types.Value),
	}

	// result.Rows is now [][]interface{}, need to use result.Columns
	firstRow := result.Rows[0]
	for i, v := range firstRow {
		if i < len(result.Columns) {
			colName := result.Columns[i].Name
			record.Data[colName] = &types.Value{Data: v}
		}
	}

	return record, nil
}

func (op *TableScanOperator) Close() error {
	if op.iter != nil {
		return op.iter.Close()
	}
	return nil
}
