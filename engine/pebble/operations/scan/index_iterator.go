package scan

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// IndexIterator iterates over index entries and fetches corresponding rows
type IndexIterator struct {
	iter        storage.Iterator
	codec       codec.Codec
	schemaDef   *dbTypes.TableDefinition
	opts        *engineTypes.ScanOptions
	current     *dbTypes.Record
	err         error
	count       int
	started     bool
	columnTypes []dbTypes.ColumnType
	engine      interface {
		EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool
		GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error)
	}
	tenantID    int64
	tableID     int64
	
	batchSize   int  // Increased from default to reduce I/O operations
	rowIDBuffer []int64
	rowCache    map[int64]*dbTypes.Record
	cacheIdx    int
	
	// Object pool for reusable row ID values to reduce allocations
	rowIDValuePool *dbTypes.Value
}

// NewIndexIterator creates a new IndexIterator
func NewIndexIterator(
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
) *IndexIterator {
	// Use adaptive batch size based on expected result set size
	initialBatchSize := 200
	if opts != nil {
		if opts.Limit > 0 {
			// If limit is specified, use smaller of limit or initial batch size
			if opts.Limit < initialBatchSize {
				initialBatchSize = opts.Limit
			}
		} else if opts.Offset > 0 {
			// For offset queries, use larger batch size to skip efficiently
			initialBatchSize = 500
		}
	} else {
		// For unlimited scans, start with larger batch size to reduce round trips
		initialBatchSize = 1000
	}
	
	// Cap batch size to prevent excessive memory usage
	if initialBatchSize > 5000 {
		initialBatchSize = 5000
	}
	
	return &IndexIterator{
		iter:        iter,
		codec:       codec,
		schemaDef:   schemaDef,
		opts:        opts,
		columnTypes: columnTypes,
		tenantID:    tenantID,
		tableID:     tableID,
		engine:      engine,
		count:       0,
		started:     false,
		batchSize:   initialBatchSize,
		rowCache:    make(map[int64]*dbTypes.Record, initialBatchSize),
		rowIDValuePool: &dbTypes.Value{
			Type: dbTypes.ColumnTypeNumber,
		},
	}
}

// Initialize sets up the IndexIterator with the required parameters
func (ii *IndexIterator) Initialize(
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
) {
	// Use adaptive batch size based on expected result set size
	initialBatchSize := 200
	if opts != nil {
		if opts.Limit > 0 {
			// If limit is specified, use smaller of limit or initial batch size
			if opts.Limit < initialBatchSize {
				initialBatchSize = opts.Limit
			}
		} else if opts.Offset > 0 {
			// For offset queries, use larger batch size to skip efficiently
			initialBatchSize = 500
		}
	} else {
		// For unlimited scans, start with larger batch size to reduce round trips
		initialBatchSize = 1000
	}
	
	// Cap batch size to prevent excessive memory usage
	if initialBatchSize > 5000 {
		initialBatchSize = 5000
	}
	
	ii.iter = iter
	ii.codec = codec
	ii.schemaDef = schemaDef
	ii.opts = opts
	ii.columnTypes = columnTypes
	ii.tenantID = tenantID
	ii.tableID = tableID
	ii.engine = engine
	ii.count = 0
	ii.started = false
	ii.batchSize = initialBatchSize
	ii.current = nil
	ii.err = nil
	
	// Initialize or reuse rowCache
	if ii.rowCache == nil {
		ii.rowCache = make(map[int64]*dbTypes.Record, initialBatchSize)
	} else {
		// Clear the map without reallocating
		for k := range ii.rowCache {
			delete(ii.rowCache, k)
		}
	}
	
	// Reuse or create rowIDBuffer
	if ii.rowIDBuffer == nil {
		ii.rowIDBuffer = make([]int64, 0, initialBatchSize)
	} else {
		ii.rowIDBuffer = ii.rowIDBuffer[:0]
	}
	
	ii.cacheIdx = 0
	
	// Keep or create rowIDValuePool
	if ii.rowIDValuePool == nil {
		ii.rowIDValuePool = &dbTypes.Value{
			Type: dbTypes.ColumnTypeNumber,
		}
	}
}

func (ii *IndexIterator) Next() bool {
	if ii.opts != nil && ii.opts.Limit > 0 && ii.count >= ii.opts.Limit {
		return false
	}

	// Use loop instead of recursion to handle filter rejection
	for {
		// Need to fetch new batch
		if len(ii.rowIDBuffer) == 0 || ii.cacheIdx >= len(ii.rowIDBuffer) {
			// Reuse buffer slices to reduce allocations
			if ii.rowIDBuffer == nil {
				ii.rowIDBuffer = make([]int64, 0, ii.batchSize)
			} else {
				ii.rowIDBuffer = ii.rowIDBuffer[:0]
			}
			ii.cacheIdx = 0

			var hasNext bool
			if !ii.started {
				// First call: initialize and position iterator
				ii.started = true
				// Reuse rowCache map to reduce allocations
				if ii.rowCache == nil {
					ii.rowCache = make(map[int64]*dbTypes.Record, ii.batchSize)
				} else {
					// Clear the map without reallocating
					for k := range ii.rowCache {
						delete(ii.rowCache, k)
					}
				}
				
				hasNext = ii.iter.First()
				
				// Skip offset rows
				if ii.opts != nil && ii.opts.Offset > 0 && hasNext {
					for i := 0; i < ii.opts.Offset && hasNext; i++ {
						hasNext = ii.iter.Next()
					}
					hasNext = ii.iter.Valid()
				}
			} else {
				// Subsequent batches: iterator already positioned by previous batch's loop
				hasNext = ii.iter.Valid()
				// Clear the map without reallocating
				for k := range ii.rowCache {
					delete(ii.rowCache, k)
				}
			}

			// Collect rowIDs for batch fetch with pre-allocated capacity
			ii.rowIDBuffer = ii.rowIDBuffer[:0]
			collected := 0
			for collected < ii.batchSize && hasNext && ii.iter.Valid() {
				// Use optimized rowID extraction instead of full key decoding
				rowID, err := ii.codec.ExtractRowIDFromIndexKey(ii.iter.Key())
				if err != nil {
					ii.err = fmt.Errorf("extract rowID from index key: %w", err)
					return false
				}
				ii.rowIDBuffer = append(ii.rowIDBuffer, rowID)
				collected++
				if collected < ii.batchSize {
					hasNext = ii.iter.Next()
				}
			}

			if len(ii.rowIDBuffer) == 0 {
				return false
			}

			// Fetch batch
			if ii.engine != nil {
				rowCache, err := ii.engine.GetRowBatch(context.Background(), ii.tenantID, ii.tableID, ii.rowIDBuffer, ii.schemaDef)
				if err != nil {
					ii.err = fmt.Errorf("fetch row batch: %w", err)
					return false
				}
				
				// Efficiently copy results to reuse map
				for k, v := range rowCache {
					ii.rowCache[k] = v
				}
			}
		}

		// Process current batch
		if ii.cacheIdx < len(ii.rowIDBuffer) {
			rowID := ii.rowIDBuffer[ii.cacheIdx]
			ii.cacheIdx++

			row, ok := ii.rowCache[rowID]
			if !ok {
				// Row not found in cache, continue to next
				continue
			}
			
			// Apply filter evaluation
			if ii.opts != nil && ii.opts.Filter != nil {
				if ii.engine != nil && !ii.engine.EvaluateFilter(ii.opts.Filter, row) {
					// Filter doesn't match, continue to next row
					continue
				}
			}

			// Reuse the _rowid value object to reduce allocations
			if rowIDVal, exists := row.Data["_rowid"]; exists {
				// Update existing value
				rowIDVal.Data = rowID
			} else {
				// Reuse pooled value object to reduce allocations
				ii.rowIDValuePool.Data = rowID
				row.Data["_rowid"] = ii.rowIDValuePool
			}

			ii.current = row
			ii.count++
			return true
		}

		// Batch exhausted, loop will fetch next batch
	}
}

func (ii *IndexIterator) Row() *dbTypes.Record {
	return ii.current
}

func (ii *IndexIterator) Error() error {
	if ii.err != nil {
		return ii.err
	}
	return ii.iter.Error()
}

func (ii *IndexIterator) Close() error {
	if ii.iter != nil {
		return ii.iter.Close()
	}
	return nil
}

// ResetForReuse resets the iterator for reuse in a pool
func (ii *IndexIterator) ResetForReuse() {
	ii.iter = nil
	ii.codec = nil
	ii.schemaDef = nil
	ii.opts = nil
	ii.current = nil
	ii.err = nil
	ii.count = 0
	ii.started = false
	ii.columnTypes = nil
	ii.engine = nil
	ii.tenantID = 0
	ii.tableID = 0
	ii.batchSize = 0
	ii.rowIDBuffer = ii.rowIDBuffer[:0]
	if ii.rowCache != nil {
		for k := range ii.rowCache {
			delete(ii.rowCache, k)
		}
	}
	ii.cacheIdx = 0
	// Keep the rowIDValuePool as it's a value object
}