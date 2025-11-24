package engine

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type pebbleEngine struct {
	kv          storage.KV
	codec       codec.Codec
	idGenerator *types.SnowflakeIDGenerator

	tableIDCounters map[int64]*int64
	indexIDCounters map[string]*int64
}

func NewPebbleEngine(kvStore storage.KV, c codec.Codec) StorageEngine {
	return &pebbleEngine{
		kv:              kvStore,
		codec:           c,
		idGenerator:     types.NewSnowflakeIDGenerator(0),
		tableIDCounters: make(map[int64]*int64),
		indexIDCounters: make(map[string]*int64),
	}
}

func (e *pebbleEngine) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error) {
	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := e.kv.Get(ctx, key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, types.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := e.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

func (e *pebbleEngine) GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *types.TableDefinition) (map[int64]*types.Record, error) {
	if len(rowIDs) == 0 {
		return make(map[int64]*types.Record), nil
	}

	result := make(map[int64]*types.Record, len(rowIDs))
	
	for _, rowID := range rowIDs {
		key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
		value, err := e.kv.Get(ctx, key)
		if err != nil {
			if storage.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get row %d: %w", rowID, err)
		}

		record, err := e.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return nil, fmt.Errorf("decode row %d: %w", rowID, err)
		}

		result[rowID] = record
	}

	return result, nil
}

func (e *pebbleEngine) InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error) {
	rowID, err := e.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}
	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := e.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}
	if err := e.kv.Set(ctx, key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}
	if err := e.updateIndexes(ctx, tenantID, tableID, rowID, row, schemaDef, true); err != nil {
		return 0, fmt.Errorf("update indexes: %w", err)
	}
	return rowID, nil
}

func (e *pebbleEngine) InsertRowBatch(ctx context.Context, tenantID, tableID int64, rows []*types.Record, schemaDef *types.TableDefinition) ([]int64, error) {
	if len(rows) == 0 {
		return []int64{}, nil
	}

	rowIDs := make([]int64, len(rows))
	for i := range rows {
		rowID, err := e.NextRowID(ctx, tenantID, tableID)
		if err != nil {
			return nil, fmt.Errorf("generate row id: %w", err)
		}
		rowIDs[i] = rowID
	}

	batch := e.kv.NewBatch()
	defer batch.Close()

	for i, row := range rows {
		key := e.codec.EncodeTableKey(tenantID, tableID, rowIDs[i])
		value, err := e.codec.EncodeRow(row, schemaDef)
		if err != nil {
			return nil, fmt.Errorf("encode row %d: %w", i, err)
		}

		if err := batch.Set(key, value); err != nil {
			return nil, fmt.Errorf("batch set row %d: %w", i, err)
		}

		if err := e.batchUpdateIndexes(batch, tenantID, tableID, rowIDs[i], row, schemaDef); err != nil {
			return nil, fmt.Errorf("batch update indexes for row %d: %w", i, err)
		}
	}

	if err := e.kv.Commit(ctx, batch); err != nil {
		return nil, fmt.Errorf("commit batch: %w", err)
	}

	return rowIDs, nil
}

func (e *pebbleEngine) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error {
	oldRow, err := e.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	if err := e.deleteIndexes(ctx, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("delete old indexes: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	value, err := e.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := e.kv.Set(ctx, key, value); err != nil {
		return fmt.Errorf("update row: %w", err)
	}

	if err := e.updateIndexes(ctx, tenantID, tableID, rowID, oldRow, schemaDef, false); err != nil {
		return fmt.Errorf("update indexes: %w", err)
	}

	return nil
}

func (e *pebbleEngine) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error {
	oldRow, err := e.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get row: %w", err)
	}

	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)
	if err := e.kv.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	if err := e.deleteIndexes(ctx, tenantID, tableID, rowID, oldRow, schemaDef); err != nil {
		return fmt.Errorf("delete indexes: %w", err)
	}

	return nil
}

func (e *pebbleEngine) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *types.IndexDefinition) error {
	// Generate a new index ID
	indexID, err := e.NextIndexID(ctx, tenantID, tableID)
	if err != nil {
		return fmt.Errorf("generate index id: %w", err)
	}

	// Store index metadata
	// TODO: Implement proper index metadata storage
	_ = indexID // Placeholder to avoid unused variable error

	return nil
}

func (e *pebbleEngine) DropIndex(ctx context.Context, tenantID, tableID, indexID int64) error {
	// TODO: Remove index metadata
	// TODO: Remove all index entries from storage

	return nil
}

func (e *pebbleEngine) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	startKey, err := e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValue, 0)
	if err != nil {
		return nil, fmt.Errorf("encode start key: %w", err)
	}

	endKey, err := e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValue, int64(^uint64(0)>>1))
	if err != nil {
		return nil, fmt.Errorf("encode end key: %w", err)
	}

	iter := e.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer iter.Close()

	var rowIDs []int64
	for iter.First(); iter.Valid(); iter.Next() {
		// Extract rowID from the index key
		_, _, _, _, rowID, err := e.codec.DecodeIndexKey(iter.Key())
		if err != nil {
			return nil, fmt.Errorf("decode index key: %w", err)
		}
		rowIDs = append(rowIDs, rowID)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return rowIDs, nil
}

func (e *pebbleEngine) ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *types.TableDefinition, opts *ScanOptions) (RowIterator, error) {
	var startKey, endKey []byte

	if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
	} else {
		startKey = e.codec.EncodeTableKey(tenantID, tableID, 0)
	}

	if opts != nil && opts.EndKey != nil {
		endKey = opts.EndKey
	} else {
		endKey = e.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := e.kv.NewIterator(iterOpts)

	return &rowIterator{
		iter:      iter,
		codec:     e.codec,
		schemaDef: schemaDef,
		opts:      opts,
		count:     0,
	}, nil
}

func (e *pebbleEngine) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *types.TableDefinition, opts *ScanOptions) (RowIterator, error) {
	// Find the index definition to get column types
	var indexDef *types.IndexDefinition
	for i, idx := range schemaDef.Indexes {
		if int64(i+1) == indexID {
			indexDef = &idx
			break
		}
	}

	if indexDef == nil {
		return nil, fmt.Errorf("index not found: %d", indexID)
	}

	// Get column types for the index columns
	columnTypes := make([]types.ColumnType, len(indexDef.Columns))
	for i, colName := range indexDef.Columns {
		found := false
		for _, col := range schemaDef.Columns {
			if col.Name == colName {
				columnTypes[i] = col.Type
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column not found in schema: %s", colName)
		}
	}

	var startKey, endKey []byte

	if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
	} else {
		// Create a start key for scanning the index using the new function
		startKey = e.codec.EncodeIndexScanStartKey(tenantID, tableID, indexID)
	}

	if opts != nil && opts.EndKey != nil {
		endKey = opts.EndKey
	} else {
		// Create an end key for scanning the index using the new function
		endKey = e.codec.EncodeIndexScanEndKey(tenantID, tableID, indexID)
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := e.kv.NewIterator(iterOpts)

	return &indexIterator{
		iter:        iter,
		codec:       e.codec,
		schemaDef:   schemaDef,
		opts:        opts,
		count:       0,
		columnTypes: columnTypes,
		engine:      e,
		tenantID:    tenantID,
		tableID:     tableID,
	}, nil
}

func (e *pebbleEngine) BeginTx(ctx context.Context) (Transaction, error) {
	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	return &transaction{
		kvTxn:     kvTxn,
		codec:     e.codec,
		engine:    e,
		isolation: storage.ReadCommitted,
	}, nil
}

func (e *pebbleEngine) BeginTxWithIsolation(ctx context.Context, level storage.IsolationLevel) (Transaction, error) {
	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	if err := kvTxn.SetIsolation(level); err != nil {
		kvTxn.Rollback()
		return nil, fmt.Errorf("set isolation level: %w", err)
	}

	return &transaction{
		kvTxn:     kvTxn,
		codec:     e.codec,
		engine:    e,
		isolation: level,
	}, nil
}

func (e *pebbleEngine) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return e.idGenerator.Next()
}

func (e *pebbleEngine) NextTableID(ctx context.Context, tenantID int64) (int64, error) {
	if counter, exists := e.tableIDCounters[tenantID]; exists {
		return atomic.AddInt64(counter, 1), nil
	}

	var startID int64 = 0
	counter := &startID
	e.tableIDCounters[tenantID] = counter

	return atomic.AddInt64(counter, 1), nil
}

func (e *pebbleEngine) NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	key := fmt.Sprintf("%d:%d", tenantID, tableID)

	if counter, exists := e.indexIDCounters[key]; exists {
		return atomic.AddInt64(counter, 1), nil
	}

	var startID int64 = 0
	counter := &startID
	e.indexIDCounters[key] = counter

	return atomic.AddInt64(counter, 1), nil
}

func (e *pebbleEngine) Close() error {
	return e.kv.Close()
}

func (e *pebbleEngine) updateIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *types.Record, schemaDef *types.TableDefinition, isInsert bool) error {
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
				indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := e.kv.Set(ctx, indexKey, []byte{}); err != nil {
				return fmt.Errorf("set index: %w", err)
			}
		}
	}

	return nil
}

func (e *pebbleEngine) batchUpdateIndexes(batch storage.Batch, tenantID, tableID, rowID int64, row *types.Record, schemaDef *types.TableDefinition) error {
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
				indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
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

func (e *pebbleEngine) deleteIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *types.Record, schemaDef *types.TableDefinition) error {
	if schemaDef.Indexes == nil {
		return nil
	}

	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)

		// Collect index values for all columns in this index
		indexValues := make([]interface{}, 0, len(indexDef.Columns))
		allValuesPresent := true

		for _, colName := range indexDef.Columns {
			if val, ok := row.Data[colName]; ok && val != nil {
				indexValues = append(indexValues, val.Data)
			} else {
				// If any indexed column is null, we don't have an index entry for this row
				allValuesPresent = false
				break
			}
		}

		// Only delete index entry if all values were present
		if allValuesPresent && len(indexValues) > 0 {
			var indexKey []byte
			var err error

			if len(indexValues) == 1 {
				// Single column index
				indexKey, err = e.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
			} else {
				// Composite index
				indexKey, err = e.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
			}

			if err != nil {
				return fmt.Errorf("encode index key: %w", err)
			}

			if err := e.kv.Delete(ctx, indexKey); err != nil {
				return fmt.Errorf("delete index: %w", err)
			}
		}
	}

	return nil
}

type indexIterator struct {
	iter        storage.Iterator
	codec       codec.Codec
	schemaDef   *types.TableDefinition
	opts        *ScanOptions
	current     *types.Record
	err         error
	count       int
	started     bool
	columnTypes []types.ColumnType
	engine      *pebbleEngine
	tenantID    int64
	tableID     int64
	
	batchSize   int
	rowIDBuffer []int64
	rowCache    map[int64]*types.Record
	cacheIdx    int
}

func (ii *indexIterator) Next() bool {
	if ii.opts != nil && ii.opts.Limit > 0 && ii.count >= ii.opts.Limit {
		return false
	}

	if !ii.started {
		ii.started = true
		ii.batchSize = 100
		ii.rowIDBuffer = make([]int64, 0, ii.batchSize)
		
		if ii.opts != nil && ii.opts.Offset > 0 {
			for i := 0; i < ii.opts.Offset && ii.iter.First() && ii.iter.Valid(); i++ {
				ii.iter.Next()
			}
		}
	}

	if len(ii.rowIDBuffer) == 0 || ii.cacheIdx >= len(ii.rowIDBuffer) {
		ii.rowIDBuffer = ii.rowIDBuffer[:0]
		ii.cacheIdx = 0

		hasNext := false
		if ii.count == 0 && ii.opts != nil && ii.opts.Offset > 0 {
			hasNext = ii.iter.Valid()
		} else if ii.count == 0 {
			hasNext = ii.iter.First()
		} else {
			hasNext = ii.iter.Valid()
		}

		for len(ii.rowIDBuffer) < ii.batchSize && hasNext {
			_, _, _, _, rowID, err := ii.codec.DecodeIndexKey(ii.iter.Key())
			if err != nil {
				ii.err = fmt.Errorf("decode index key: %w", err)
				return false
			}
			ii.rowIDBuffer = append(ii.rowIDBuffer, rowID)
			hasNext = ii.iter.Next()
		}

		if len(ii.rowIDBuffer) == 0 {
			return false
		}

		rowCache, err := ii.engine.GetRowBatch(context.Background(), ii.tenantID, ii.tableID, ii.rowIDBuffer, ii.schemaDef)
		if err != nil {
			ii.err = fmt.Errorf("fetch row batch: %w", err)
			return false
		}
		ii.rowCache = rowCache
	}

	if ii.cacheIdx < len(ii.rowIDBuffer) {
		rowID := ii.rowIDBuffer[ii.cacheIdx]
		ii.cacheIdx++

		row, ok := ii.rowCache[rowID]
		if !ok {
			return ii.Next()
		}

		row.Data["_rowid"] = &types.Value{
			Type: types.ColumnTypeNumber,
			Data: rowID,
		}

		ii.current = row
		ii.count++
		return true
	}

	return false
}

func (ii *indexIterator) Row() *types.Record {
	return ii.current
}

func (ii *indexIterator) Error() error {
	if ii.err != nil {
		return ii.err
	}
	return ii.iter.Error()
}

func (ii *indexIterator) Close() error {
	return ii.iter.Close()
}

type rowIterator struct {
	iter      storage.Iterator
	codec     codec.Codec
	schemaDef *types.TableDefinition
	opts      *ScanOptions
	current   *types.Record
	err       error
	count     int
	started   bool
}

func (ri *rowIterator) Next() bool {
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

	record.Data["_rowid"] = &types.Value{
		Type: types.ColumnTypeNumber,
		Data: rowID,
	}

	ri.current = record
	ri.count++
	return true
}

func (ri *rowIterator) Row() *types.Record {
	return ri.current
}

func (ri *rowIterator) Error() error {
	if ri.err != nil {
		return ri.err
	}
	return ri.iter.Error()
}

func (ri *rowIterator) Close() error {
	return ri.iter.Close()
}

type transaction struct {
	kvTxn     storage.Transaction
	codec     codec.Codec
	engine    *pebbleEngine
	isolation storage.IsolationLevel
}

func (t *transaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) (*types.Record, error) {
	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := t.kvTxn.Get(key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, types.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := t.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

func (t *transaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *types.Record, schemaDef *types.TableDefinition) (int64, error) {
	rowID, err := t.engine.NextRowID(ctx, tenantID, tableID)
	if err != nil {
		return 0, fmt.Errorf("generate row id: %w", err)
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return 0, fmt.Errorf("conflict check failed: %w", err)
	}

	value, err := t.codec.EncodeRow(row, schemaDef)
	if err != nil {
		return 0, fmt.Errorf("encode row: %w", err)
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}

	return rowID, nil
}

func (t *transaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*types.Value, schemaDef *types.TableDefinition) error {
	oldRow, err := t.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
	}

	for k, v := range updates {
		oldRow.Data[k] = v
	}

	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before writing
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return fmt.Errorf("conflict check failed: %w", err)
	}

	value, err := t.codec.EncodeRow(oldRow, schemaDef)
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if err := t.kvTxn.Set(key, value); err != nil {
		return fmt.Errorf("update row: %w", err)
	}

	return nil
}

func (t *transaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *types.TableDefinition) error {
	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	// Check for conflicts before deleting
	if err := t.engine.kv.CheckForConflicts(t.kvTxn, key); err != nil {
		return fmt.Errorf("conflict check failed: %w", err)
	}

	if err := t.kvTxn.Delete(key); err != nil {
		return fmt.Errorf("delete row: %w", err)
	}

	return nil
}

func (t *transaction) Isolation() storage.IsolationLevel {
	return t.isolation
}

func (t *transaction) SetIsolation(level storage.IsolationLevel) error {
	if err := t.kvTxn.SetIsolation(level); err != nil {
		return err
	}
	t.isolation = level

	// Additional logic for different isolation levels can be added here
	switch level {
	case storage.ReadUncommitted:
		// Minimal consistency guarantees
	case storage.ReadCommitted:
		// Default behavior
	case storage.RepeatableRead:
		// Need to track snapshot
	case storage.SnapshotIsolation:
		// Need to create a snapshot
	case storage.Serializable:
		// Highest isolation level
	}

	return nil
}

func (t *transaction) Commit() error {
	return t.kvTxn.Commit()
}

func (t *transaction) Rollback() error {
	return t.kvTxn.Rollback()
}
