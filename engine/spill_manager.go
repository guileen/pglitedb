package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type SpillManager struct {
	kv      storage.KV
	codec   codec.Codec
	mu      sync.Mutex
	queryID string
}

func NewSpillManager(kv storage.KV, codec codec.Codec, queryID string) *SpillManager {
	return &SpillManager{
		kv:      kv,
		codec:   codec,
		queryID: queryID,
	}
}

func (s *SpillManager) encodeSpillKey(partitionID int, seq int64) []byte {
	return []byte(fmt.Sprintf("spill/%s/%d/%d", s.queryID, partitionID, seq))
}

func (s *SpillManager) encodeSpillPrefix(partitionID int) []byte {
	return []byte(fmt.Sprintf("spill/%s/%d/", s.queryID, partitionID))
}

func (s *SpillManager) WritePartition(ctx context.Context, partitionID int, records []*types.Record, schemaDef *types.TableDefinition) error {
	batch := s.kv.NewBatch()
	defer batch.Close()

	for i, record := range records {
		key := s.encodeSpillKey(partitionID, int64(i))
		value, err := s.codec.EncodeRow(record, schemaDef)
		if err != nil {
			return fmt.Errorf("encode spill record: %w", err)
		}
		if err := batch.Set(key, value); err != nil {
			return fmt.Errorf("batch set: %w", err)
		}
	}

	return s.kv.Commit(ctx, batch)
}

func (s *SpillManager) ReadPartition(ctx context.Context, partitionID int, schemaDef *types.TableDefinition) ([]*types.Record, error) {
	prefix := s.encodeSpillPrefix(partitionID)
	iter := s.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	defer iter.Close()

	var records []*types.Record
	for iter.First(); iter.Valid(); iter.Next() {
		record, err := s.codec.DecodeRow(iter.Value(), schemaDef)
		if err != nil {
			return nil, fmt.Errorf("decode spill record: %w", err)
		}
		records = append(records, record)
	}

	return records, nil
}

func (s *SpillManager) Cleanup(ctx context.Context) error {
	prefix := []byte(fmt.Sprintf("spill/%s/", s.queryID))
	batch := s.kv.NewBatch()
	defer batch.Close()

	iter := s.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key()); err != nil {
			return fmt.Errorf("batch delete: %w", err)
		}
	}

	return s.kv.Commit(ctx, batch)
}
