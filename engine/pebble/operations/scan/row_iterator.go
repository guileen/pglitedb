package scan

import (
	"fmt"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// RowIterator iterates over rows in a table scan
type RowIterator struct {
	iter      storage.Iterator
	codec     codec.Codec
	schemaDef *dbTypes.TableDefinition
	opts      *engineTypes.ScanOptions
	current   *dbTypes.Record
	err       error
	count     int
	started   bool
	engine    interface{ EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool }
	
	// Reference to the pool for returning the iterator when closed
	pool interface {
		Put(interface{})
	}
	
	// Leak detection tracking
	trackedResource interface {
		MarkReleased()
		Close() error
	}
}

// SetTrackedResource sets the tracked resource for leak detection
func (ri *RowIterator) SetTrackedResource(tr interface {
	MarkReleased()
	Close() error
}) {
	ri.trackedResource = tr
}

// Reset resets the iterator state for reuse
func (ri *RowIterator) Reset() {
	ri.iter = nil
	ri.codec = nil
	ri.schemaDef = nil
	ri.opts = nil
	ri.current = nil
	ri.err = nil
	ri.count = 0
	ri.started = false
	ri.engine = nil
	// Don't reset trackedResource here as it's managed by the ResourceManager
}

// NewRowIterator creates a new RowIterator
func NewRowIterator(iter storage.Iterator, codec codec.Codec, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions, engine interface{ EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool }, pool interface {
	Put(interface{})
}) *RowIterator {
	iterator := &RowIterator{
		iter:      iter,
		codec:     codec,
		schemaDef: schemaDef,
		opts:      opts,
		engine:    engine,
		count:     0,
		pool:      pool,
	}
	return iterator
}

// Initialize sets up an existing RowIterator with the provided parameters
// This is used by the pool to reuse iterators
func (ri *RowIterator) Initialize(iter storage.Iterator, codec codec.Codec, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions, engine interface{ EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool }, pool interface {
	Put(interface{})
}) {
	ri.iter = iter
	ri.codec = codec
	ri.schemaDef = schemaDef
	ri.opts = opts
	ri.engine = engine
	ri.count = 0
	ri.started = false
	ri.err = nil
	ri.current = nil
	ri.pool = pool
}

func (ri *RowIterator) Next() bool {
	if ri.opts != nil && ri.opts.Limit > 0 && ri.count >= ri.opts.Limit {
		return false
	}

	var hasNext bool
	if !ri.started {
		hasNext = ri.iter.First()
		ri.started = true

		if ri.opts != nil && ri.opts.Offset > 0 {
			for i := 0; i < ri.opts.Offset && hasNext; i++ {
				hasNext = ri.iter.Next()
			}
		}
	} else {
		hasNext = ri.iter.Next()
	}

	if !hasNext {
		return false
	}

	_, _, rowID, err := ri.codec.DecodeTableKey(ri.iter.Key())
	if err != nil {
		ri.err = fmt.Errorf("decode table key: %w", err)
		return false
	}

	value := ri.iter.Value()
	record, err := ri.codec.DecodeRow(value, ri.schemaDef)
	if err != nil {
		ri.err = fmt.Errorf("decode row: %w", err)
		return false
	}

	record.Data["_rowid"] = &dbTypes.Value{
		Type: dbTypes.ColumnTypeNumber,
		Data: rowID,
	}
	
	// Apply filter if present
	if ri.opts != nil && ri.opts.Filter != nil && ri.engine != nil {
		if !ri.engine.EvaluateFilter(ri.opts.Filter, record) {
			return ri.Next() // Skip this row and try next
		}
	}

	ri.current = record
	ri.count++
	return true
}

func (ri *RowIterator) Row() *dbTypes.Record {
	return ri.current
}

func (ri *RowIterator) Error() error {
	if ri.err != nil {
		return ri.err
	}
	return ri.iter.Error()
}

func (ri *RowIterator) Close() error {
	err := ri.iter.Close()
	
	// Mark the tracked resource as released for leak detection
	if ri.trackedResource != nil {
		ri.trackedResource.MarkReleased()
		ri.trackedResource.Close()
		ri.trackedResource = nil
	}
	
	if ri.pool != nil {
		ri.pool.Put(ri)
	}
	return err
}