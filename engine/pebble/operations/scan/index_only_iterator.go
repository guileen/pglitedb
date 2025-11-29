package scan

import (
	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// IndexOnlyIterator implements covering index scan without table access
type IndexOnlyIterator struct {
	iter        storage.Iterator
	codec       codec.Codec
	indexDef    *dbTypes.IndexDefinition
	projection  []string
	opts        *engineTypes.ScanOptions
	columnTypes []dbTypes.ColumnType
	tenantID    int64
	tableID     int64
	indexID     int64
	engine      interface{ EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool }
	current     *dbTypes.Record
	err         error
	count       int
	started     bool
}

// NewIndexOnlyIterator creates a new IndexOnlyIterator
func NewIndexOnlyIterator(
	iter storage.Iterator,
	codec codec.Codec,
	indexDef *dbTypes.IndexDefinition,
	projection []string,
	opts *engineTypes.ScanOptions,
	columnTypes []dbTypes.ColumnType,
	tenantID int64,
	tableID int64,
	indexID int64,
	engine interface{ EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool },
) *IndexOnlyIterator {
	return &IndexOnlyIterator{
		iter:        iter,
		codec:       codec,
		indexDef:    indexDef,
		projection:  projection,
		opts:        opts,
		columnTypes: columnTypes,
		tenantID:    tenantID,
		tableID:     tableID,
		indexID:     indexID,
		engine:      engine,
		count:       0,
		started:     false,
	}
}

// Initialize sets up the IndexOnlyIterator with the required parameters
func (io *IndexOnlyIterator) Initialize(
	iter storage.Iterator,
	codec codec.Codec,
	indexDef *dbTypes.IndexDefinition,
	projection []string,
	opts *engineTypes.ScanOptions,
	columnTypes []dbTypes.ColumnType,
	tenantID int64,
	tableID int64,
	indexID int64,
	engine interface{ EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool },
) {
	io.iter = iter
	io.codec = codec
	io.indexDef = indexDef
	io.projection = projection
	io.opts = opts
	io.columnTypes = columnTypes
	io.tenantID = tenantID
	io.tableID = tableID
	io.indexID = indexID
	io.engine = engine
	io.count = 0
	io.started = false
	io.current = nil
	io.err = nil
}

func (io *IndexOnlyIterator) Next() bool {
	if io.opts != nil && io.opts.Limit > 0 && io.count >= io.opts.Limit {
		return false
	}

	// Use loop instead of recursion to avoid stack overflow and correctly advance iterator
	for {
		var hasNext bool
		if !io.started {
			hasNext = io.iter.First()
			io.started = true

			if io.opts != nil && io.opts.Offset > 0 {
				for i := 0; i < io.opts.Offset && hasNext; i++ {
					hasNext = io.iter.Next()
				}
			}
		} else {
			hasNext = io.iter.Next()
		}

		if !hasNext {
			return false
		}

		// Decode index key to extract column values using schema-aware decoder
		_, _, _, indexValues, rowID, err := io.codec.DecodeIndexKeyWithSchema(io.iter.Key(), io.columnTypes)
		if err != nil {
			io.err = err
			return false
		}

		// Build record from index values only
		record := &dbTypes.Record{
			Data: make(map[string]*dbTypes.Value),
		}

		// Map ALL index values to columns (needed for filter evaluation)
		// We'll filter to projection columns when returning the final result
		for i, colName := range io.indexDef.Columns {
			if i < len(indexValues) {
				record.Data[colName] = &dbTypes.Value{
					Type: io.columnTypes[i],
					Data: indexValues[i],
				}
			}
		}

		// Add _rowid if needed
		record.Data["_rowid"] = &dbTypes.Value{
			Type: dbTypes.ColumnTypeNumber,
			Data: rowID,
		}

		// Apply filter if present
		if io.opts != nil && io.opts.Filter != nil {
			if !io.engine.EvaluateFilter(io.opts.Filter, record) {
				// Filter doesn't match, continue to next row
				continue
			}
		}

		// Now filter to only projection columns
		if io.projection != nil && len(io.projection) > 0 {
			filteredData := make(map[string]*dbTypes.Value)
			for _, projCol := range io.projection {
				if val, ok := record.Data[projCol]; ok {
					filteredData[projCol] = val
				}
			}
			record.Data = filteredData
		}

		io.current = record
		io.count++
		return true
	}
}

func (io *IndexOnlyIterator) Row() *dbTypes.Record {
	return io.current
}

func (io *IndexOnlyIterator) Error() error {
	return io.err
}

func (io *IndexOnlyIterator) Close() error {
	if io.iter != nil {
		return io.iter.Close()
	}
	return nil
}

// ResetForReuse resets the iterator for reuse in a pool
func (io *IndexOnlyIterator) ResetForReuse() {
	io.iter = nil
	io.codec = nil
	io.indexDef = nil
	io.projection = nil
	io.opts = nil
	io.columnTypes = nil
	io.tenantID = 0
	io.tableID = 0
	io.indexID = 0
	io.engine = nil
	io.current = nil
	io.err = nil
	io.count = 0
	io.started = false
}