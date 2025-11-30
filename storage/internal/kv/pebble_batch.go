package kv

import (
	"github.com/cockroachdb/pebble"
)

type PebbleBatch struct {
	batch *pebble.Batch
	kv    *PebbleKV
}

func (b *PebbleBatch) Set(key, value []byte) error {
	return b.batch.Set(key, value, nil)
}

func (b *PebbleBatch) Delete(key []byte) error {
	return b.batch.Delete(key, nil)
}

func (b *PebbleBatch) Count() int {
	return int(b.batch.Count())
}

func (b *PebbleBatch) Reset() {
	b.batch.Reset()
}

func (b *PebbleBatch) Close() error {
	return b.batch.Close()
}