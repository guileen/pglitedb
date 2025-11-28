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
}

// NewRowIterator creates a new RowIterator
func NewRowIterator(iter storage.Iterator, codec codec.Codec, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions, engine interface{ EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool }) *RowIterator {
	return &RowIterator{
		iter:      iter,
		codec:     codec,
		schemaDef: schemaDef,
		opts:      opts,
		engine:    engine,
		count:     0,
	}
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
	return ri.iter.Close()
}