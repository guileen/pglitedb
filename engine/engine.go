package engine

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/guileen/pqlitedb/codec"
	"github.com/guileen/pqlitedb/kv"
	"github.com/guileen/pqlitedb/table"
)

type pebbleEngine struct {
	kv    kv.KV
	codec codec.Codec

	rowIDCounters   map[string]*int64
	tableIDCounters map[int64]*int64
	indexIDCounters map[string]*int64
}

func NewPebbleEngine(kvStore kv.KV, c codec.Codec) StorageEngine {
	return &pebbleEngine{
		kv:              kvStore,
		codec:           c,
		rowIDCounters:   make(map[string]*int64),
		tableIDCounters: make(map[int64]*int64),
		indexIDCounters: make(map[string]*int64),
	}
}

func (e *pebbleEngine) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *table.TableDefinition) (*table.Record, error) {
	key := e.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := e.kv.Get(ctx, key)
	if err != nil {
		if kv.IsNotFound(err) {
			return nil, table.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := e.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

func (e *pebbleEngine) InsertRow(ctx context.Context, tenantID, tableID int64, row *table.Record, schemaDef *table.TableDefinition) (int64, error) {
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

func (e *pebbleEngine) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*table.Value, schemaDef *table.TableDefinition) error {
	oldRow, err := e.GetRow(ctx, tenantID, tableID, rowID, schemaDef)
	if err != nil {
		return fmt.Errorf("get old row: %w", err)
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

func (e *pebbleEngine) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *table.TableDefinition) error {
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

func (e *pebbleEngine) CreateIndex(ctx context.Context, tenantID, tableID int64, indexDef *table.IndexDefinition) error {
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

	iter := e.kv.NewIterator(&kv.IteratorOptions{
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

func (e *pebbleEngine) ScanRows(ctx context.Context, tenantID, tableID int64, schemaDef *table.TableDefinition, opts *ScanOptions) (RowIterator, error) {
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

	iterOpts := &kv.IteratorOptions{
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

func (e *pebbleEngine) ScanIndex(ctx context.Context, tenantID, tableID, indexID int64, schemaDef *table.TableDefinition, opts *ScanOptions) (RowIterator, error) {
	// Find the index definition to get column types
	var indexDef *table.IndexDefinition
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
	columnTypes := make([]table.ColumnType, len(indexDef.Columns))
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
		// Create a start key for scanning the index
		startKey = e.codec.EncodeTableKey(tenantID, tableID, 0)
		// Modify to be index key format
		newStartKey := make([]byte, len(startKey))
		copy(newStartKey, startKey)
		if len(newStartKey) > 1 {
			newStartKey[1] = byte(codec.KeyTypeIndex)
		}
		startKey = newStartKey
	}

	if opts != nil && opts.EndKey != nil {
		endKey = opts.EndKey
	} else {
		// Create an end key for scanning the index
		endKey = e.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
		// Modify to be index key format
		newEndKey := make([]byte, len(endKey))
		copy(newEndKey, endKey)
		if len(newEndKey) > 1 {
			newEndKey[1] = byte(codec.KeyTypeIndex)
		}
		endKey = newEndKey
	}

	iterOpts := &kv.IteratorOptions{
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
		isolation: kv.ReadCommitted, // Default isolation level
	}, nil
}

func (e *pebbleEngine) BeginTxWithIsolation(ctx context.Context, level kv.IsolationLevel) (Transaction, error) {
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
	key := fmt.Sprintf("%d:%d", tenantID, tableID)

	if counter, exists := e.rowIDCounters[key]; exists {
		return atomic.AddInt64(counter, 1), nil
	}

	metaKey := e.codec.EncodeTableKey(tenantID, tableID, -1)
	value, err := e.kv.Get(ctx, metaKey)

	var startID int64 = 0
	if err == nil {
		if len(value) >= 8 {
			startID = int64(uint64(value[0])<<56 | uint64(value[1])<<48 |
				uint64(value[2])<<40 | uint64(value[3])<<32 |
				uint64(value[4])<<24 | uint64(value[5])<<16 |
				uint64(value[6])<<8 | uint64(value[7]))
		}
	}

	counter := &startID
	e.rowIDCounters[key] = counter

	return atomic.AddInt64(counter, 1), nil
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

func (e *pebbleEngine) updateIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *table.Record, schemaDef *table.TableDefinition, isInsert bool) error {
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
				// If any indexed column is null, we don't index this row
				allValuesPresent = false
				break
			}
		}

		// Only create index entry if all values are present
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

			if err := e.kv.Set(ctx, indexKey, []byte{}); err != nil {
				return fmt.Errorf("set index: %w", err)
			}
		}
	}

	return nil
}

func (e *pebbleEngine) deleteIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *table.Record, schemaDef *table.TableDefinition) error {
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
	iter        kv.Iterator
	codec       codec.Codec
	schemaDef   *table.TableDefinition
	opts        *ScanOptions
	current     *table.Record
	err         error
	count       int
	started     bool
	columnTypes []table.ColumnType
	engine      *pebbleEngine
	tenantID    int64
	tableID     int64
}

func (ii *indexIterator) Next() bool {
	if ii.opts != nil && ii.opts.Limit > 0 && ii.count >= ii.opts.Limit {
		return false
	}

	var hasNext bool
	if !ii.started {
		hasNext = ii.iter.First()
		ii.started = true

		if ii.opts != nil && ii.opts.Offset > 0 {
			for i := 0; i < ii.opts.Offset && hasNext; i++ {
				hasNext = ii.iter.Next()
			}
		}
	} else {
		hasNext = ii.iter.Next()
	}

	if !hasNext {
		return false
	}

	// Extract rowID from the index key
	_, _, _, _, rowID, err := ii.codec.DecodeIndexKey(ii.iter.Key())
	if err != nil {
		ii.err = fmt.Errorf("decode index key: %w", err)
		return false
	}

	// Fetch the actual row data
	row, err := ii.engine.GetRow(context.Background(), ii.tenantID, ii.tableID, rowID, ii.schemaDef)
	if err != nil {
		ii.err = fmt.Errorf("fetch row data: %w", err)
		return false
	}

	ii.current = row
	ii.count++
	return true
}

func (ii *indexIterator) Row() *table.Record {
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
	iter      kv.Iterator
	codec     codec.Codec
	schemaDef *table.TableDefinition
	opts      *ScanOptions
	current   *table.Record
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

	value := ri.iter.Value()
	record, err := ri.codec.DecodeRow(value, ri.schemaDef)
	if err != nil {
		ri.err = fmt.Errorf("decode row: %w", err)
		return false
	}

	ri.current = record
	ri.count++
	return true
}

func (ri *rowIterator) Row() *table.Record {
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
	kvTxn     kv.Transaction
	codec     codec.Codec
	engine    *pebbleEngine
	isolation kv.IsolationLevel
}

func (t *transaction) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *table.TableDefinition) (*table.Record, error) {
	key := t.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := t.kvTxn.Get(key)
	if err != nil {
		if kv.IsNotFound(err) {
			return nil, table.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := t.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

func (t *transaction) InsertRow(ctx context.Context, tenantID, tableID int64, row *table.Record, schemaDef *table.TableDefinition) (int64, error) {
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

func (t *transaction) UpdateRow(ctx context.Context, tenantID, tableID, rowID int64, updates map[string]*table.Value, schemaDef *table.TableDefinition) error {
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

func (t *transaction) DeleteRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *table.TableDefinition) error {
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

func (t *transaction) Isolation() kv.IsolationLevel {
	return t.isolation
}

func (t *transaction) SetIsolation(level kv.IsolationLevel) error {
	if err := t.kvTxn.SetIsolation(level); err != nil {
		return err
	}
	t.isolation = level

	// Additional logic for different isolation levels can be added here
	switch level {
	case kv.ReadUncommitted:
		// Minimal consistency guarantees
	case kv.ReadCommitted:
		// Default behavior
	case kv.RepeatableRead:
		// Need to track snapshot
	case kv.SnapshotIsolation:
		// Need to create a snapshot
	case kv.Serializable:
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
