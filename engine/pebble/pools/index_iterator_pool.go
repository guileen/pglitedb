package pools

import (
	"context"
	"sync"

	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	dbTypes "github.com/guileen/pglitedb/types"
	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// IteratorPool is a pool of various iterator objects
type IteratorPool struct {
	indexIteratorPool sync.Pool
	indexOnlyIteratorPool sync.Pool
	rowIteratorPool sync.Pool
}

// NewIteratorPool creates a new IteratorPool
func NewIteratorPool() *IteratorPool {
	return &IteratorPool{
		indexIteratorPool: sync.Pool{
			New: func() interface{} {
				return &scan.IndexIterator{}
			},
		},
		indexOnlyIteratorPool: sync.Pool{
			New: func() interface{} {
				return &scan.IndexOnlyIterator{}
			},
		},
		rowIteratorPool: sync.Pool{
			New: func() interface{} {
				return &scan.RowIterator{}
			},
		},
	}
}

// GetIndexIterator retrieves an IndexIterator from the pool
func (p *IteratorPool) GetIndexIterator(
	iter storage.Iterator,
	codec codec.Codec,
	schemaDef *dbTypes.TableDefinition,
	opts *engineTypes.ScanOptions,
	columnTypes []dbTypes.ColumnType,
	tenantID int64,
	tableID int64,
	engine interface {
		EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool
		GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error)
	},
	pool interface {
		Put(interface{})
	},
) *scan.IndexIterator {
	iterator := p.indexIteratorPool.Get().(*scan.IndexIterator)
	// Initialize the iterator with the provided parameters
	iterator.Initialize(iter, codec, schemaDef, opts, columnTypes, tenantID, tableID, engine, pool)
	return iterator
}

// GetIndexOnlyIterator retrieves an IndexOnlyIterator from the pool
func (p *IteratorPool) GetIndexOnlyIterator(
	iter storage.Iterator,
	codec codec.Codec,
	indexDef *dbTypes.IndexDefinition,
	projection []string,
	opts *engineTypes.ScanOptions,
	columnTypes []dbTypes.ColumnType,
	tenantID int64,
	tableID int64,
	indexID int64,
	engine interface {
		EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool
	},
	pool interface {
		Put(interface{})
	},
) *scan.IndexOnlyIterator {
	iterator := p.indexOnlyIteratorPool.Get().(*scan.IndexOnlyIterator)
	// Initialize the iterator with the provided parameters
	iterator.Initialize(iter, codec, indexDef, projection, opts, columnTypes, tenantID, tableID, indexID, engine, pool)
	return iterator
}

// GetRowIterator retrieves a RowIterator from the pool
func (p *IteratorPool) GetRowIterator(
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
) *scan.RowIterator {
	iterator := p.rowIteratorPool.Get().(*scan.RowIterator)
	// Initialize the iterator with the provided parameters
	iterator.Initialize(iter, codec, schemaDef, opts, engine, pool)
	return iterator
}

// Put returns an iterator to the appropriate pool
func (p *IteratorPool) Put(iterator interface{}) {
	if iterator != nil {
		switch iter := iterator.(type) {
		case *scan.IndexIterator:
			iter.Reset()
			p.indexIteratorPool.Put(iter)
		case *scan.IndexOnlyIterator:
			iter.Reset()
			p.indexOnlyIteratorPool.Put(iter)
		case *scan.RowIterator:
			iter.Reset()
			p.rowIteratorPool.Put(iter)
		}
	}
}