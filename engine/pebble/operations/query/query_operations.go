package query

import (
	"context"
	"fmt"
	"sort"

	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// QueryOperations provides query operations for the pebble engine
type QueryOperations struct {
	kv    storage.KV
	codec Codec
}

// Codec interface defines the methods needed for encoding/decoding
type Codec interface {
	EncodeTableKey(tenantID, tableID, rowID int64) []byte
	DecodeTableKey(key []byte) (tenantID, tableID, rowID int64, err error)
	EncodeRow(record *dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]byte, error)
	DecodeRow(data []byte, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error)
}

// NewQueryOperations creates a new QueryOperations instance
func NewQueryOperations(kv storage.KV, codec Codec) *QueryOperations {
	return &QueryOperations{
		kv:    kv,
		codec: codec,
	}
}

// GetRow retrieves a single row by its ID
func (q *QueryOperations) GetRow(ctx context.Context, tenantID, tableID, rowID int64, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	key := q.codec.EncodeTableKey(tenantID, tableID, rowID)

	value, err := q.kv.Get(ctx, key)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, dbTypes.ErrRecordNotFound
		}
		return nil, fmt.Errorf("get row: %w", err)
	}

	record, err := q.codec.DecodeRow(value, schemaDef)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return record, nil
}

// GetRowBatch retrieves multiple rows by their IDs
func (q *QueryOperations) GetRowBatch(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) (map[int64]*dbTypes.Record, error) {
	if len(rowIDs) == 0 {
		return make(map[int64]*dbTypes.Record), nil
	}

	result := make(map[int64]*dbTypes.Record, len(rowIDs))

	sorted := make([]int64, len(rowIDs))
	copy(sorted, rowIDs)
	sortInt64Slice(sorted)

	startKey := q.codec.EncodeTableKey(tenantID, tableID, sorted[0])
	endKey := q.codec.EncodeTableKey(tenantID, tableID, sorted[len(sorted)-1]+1)

	iter := q.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer iter.Close()

	targetIdx := 0
	for iter.First(); iter.Valid() && targetIdx < len(sorted); iter.Next() {
		_, _, rowID, err := q.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return nil, fmt.Errorf("decode table key: %w", err)
		}

		for targetIdx < len(sorted) && sorted[targetIdx] < rowID {
			targetIdx++
		}

		if targetIdx < len(sorted) && sorted[targetIdx] == rowID {
			record, err := q.codec.DecodeRow(iter.Value(), schemaDef)
			if err != nil {
				return nil, fmt.Errorf("decode row %d: %w", rowID, err)
			}
			result[rowID] = record
			targetIdx++
		}
	}

	return result, nil
}

// sortInt64Slice sorts a slice of int64 values
func sortInt64Slice(arr []int64) {
	// Use efficient sorting algorithm instead of bubble sort
	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
}