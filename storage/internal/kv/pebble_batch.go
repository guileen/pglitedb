package kv

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/pebble"
)

type PebbleBatch struct {
	batch *pebble.Batch
	kv    *PebbleKV
	ops   []batchOp // Store operations for sorting
}

type batchOp struct {
	opType string // "set" or "delete"
	key    []byte
	value  []byte
}

func (b *PebbleBatch) Set(key, value []byte) error {
	// Store operation for later sorted application
	b.ops = append(b.ops, batchOp{
		opType: "set",
		key:    cloneBatchBytes(key),
		value:  cloneBatchBytes(value),
	})
	return nil
}

func (b *PebbleBatch) Delete(key []byte) error {
	// Store operation for later sorted application
	b.ops = append(b.ops, batchOp{
		opType: "delete",
		key:    cloneBatchBytes(key),
	})
	return nil
}

func (b *PebbleBatch) Count() int {
	return len(b.ops)
}

func (b *PebbleBatch) Reset() {
	b.batch.Reset()
	b.ops = b.ops[:0] // Clear operations slice
}

func (b *PebbleBatch) Close() error {
	b.ops = nil // Release operations slice
	return b.batch.Close()
}

// applySorted applies all stored operations in sorted key order
func (b *PebbleBatch) applySorted() error {
	// Sort operations by key
	sort.Slice(b.ops, func(i, j int) bool {
		return bytes.Compare(b.ops[i].key, b.ops[j].key) < 0
	})

	// Apply sorted operations to the underlying batch
	for _, op := range b.ops {
		var err error
		if op.opType == "set" {
			err = b.batch.Set(op.key, op.value, nil)
		} else if op.opType == "delete" {
			err = b.batch.Delete(op.key, nil)
		}
		if err != nil {
			return err
		}
	}
	
	return nil
}

// cloneBatchBytes creates a copy of a byte slice
func cloneBatchBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	result := make([]byte, len(b))
	copy(result, b)
	return result
}