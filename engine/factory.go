package engine

import (
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
)

// NewStorageEngine creates a new storage engine
func NewStorageEngine(kvStore storage.KV, c codec.Codec) engineTypes.StorageEngine {
	return pebble.NewPebbleEngine(kvStore, c)
}