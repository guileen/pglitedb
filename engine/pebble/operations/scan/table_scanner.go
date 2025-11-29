package scan

import (
	"context"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// TableScanner implements table scanning operations
type TableScanner struct {
	kv    storage.KV
	codec codec.Codec
	iteratorPool interface {
		GetRowIterator(
			iter storage.Iterator,
			codec codec.Codec,
			schemaDef *dbTypes.TableDefinition,
			opts *engineTypes.ScanOptions,
			engine interface {
				EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool
			},
			pool interface {
				Put(interface{})
			},
		) *RowIterator
		Put(interface{})
	}
}

// NewTableScanner creates a new table scanner
func NewTableScanner(kv storage.KV, codec codec.Codec) *TableScanner {
	return &TableScanner{
		kv:    kv,
		codec: codec,
	}
}

// WithIteratorPool sets the iterator pool for the table scanner
func (ts *TableScanner) WithIteratorPool(pool interface {
	GetRowIterator(
		iter storage.Iterator,
		codec codec.Codec,
		schemaDef *dbTypes.TableDefinition,
		opts *engineTypes.ScanOptions,
		engine interface {
			EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool
		},
		pool interface {
			Put(interface{})
		},
	) *RowIterator
	Put(interface{})
}) *TableScanner {
	ts.iteratorPool = pool
	return ts
}

// ScanRows performs a table scan
func (ts *TableScanner) ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	var startKey, endKey []byte

	if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
	} else {
		startKey = ts.codec.EncodeTableKey(tenantID, tableID, 0)
	}

	if opts != nil && opts.EndKey != nil {
		endKey = opts.EndKey
	} else {
		endKey = ts.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := ts.kv.NewIterator(iterOpts)

	// Use the pool if available, otherwise create a new iterator
	if ts.iteratorPool != nil {
		return ts.iteratorPool.GetRowIterator(iter, ts.codec, schemaDef, opts, nil, ts.iteratorPool), nil
	}
	return NewRowIterator(iter, ts.codec, schemaDef, opts, nil, nil), nil
}