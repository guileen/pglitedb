package pebble

import (
	"context"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/indexes"
	"github.com/guileen/pglitedb/engine/pebble/operations/query"
	"github.com/guileen/pglitedb/engine/pebble/resources"
	"github.com/guileen/pglitedb/idgen"
	"github.com/guileen/pglitedb/storage"
)

type pebbleEngine struct {
	kv                  storage.KV
	codec               codec.Codec
	idGenerator         idgen.IDGeneratorInterface
	indexManager        *indexes.Handler
	filterEvaluator     *FilterEvaluator
	multiColumnOptimizer *MultiColumnOptimizer
	queryOperations     *query.QueryOperations
	insertOperations    *query.InsertOperations
	updateOperations    *query.UpdateOperations
	deleteOperations    *query.DeleteOperations
}

func NewPebbleEngine(kvStore storage.KV, c codec.Codec) engineTypes.StorageEngine {
	queryOps := query.NewQueryOperations(kvStore, c)
	insertOps := query.NewInsertOperations(kvStore, c)
	updateOps := query.NewUpdateOperations(kvStore, c)
	deleteOps := query.NewDeleteOperations(kvStore, c)
	
	// Track the KV store connection for leak detection
	rm := resources.GetResourceManager()
	rm.TrackConnection(kvStore)
	
	return &pebbleEngine{
		kv:                  kvStore,
		codec:               c,
		idGenerator:         idgen.NewIDGenerator(),
		indexManager:        indexes.NewHandler(kvStore, c),
		filterEvaluator:     NewFilterEvaluator(),
		multiColumnOptimizer: NewMultiColumnOptimizer(c),
		queryOperations:     queryOps,
		insertOperations:    insertOps,
		updateOperations:    updateOps,
		deleteOperations:    deleteOps,
	}
}

func (e *pebbleEngine) Close() error {
	return e.kv.Close()
}

// GetCodec returns the codec used by the engine
func (e *pebbleEngine) GetCodec() codec.Codec {
	return e.codec
}

// GetKV returns the KV store used by the engine
func (e *pebbleEngine) GetKV() storage.KV {
	return e.kv
}

// CheckForConflicts checks for conflicts with the given key
func (e *pebbleEngine) CheckForConflicts(txn storage.Transaction, key []byte) error {
	return e.kv.CheckForConflicts(txn, key)
}

func (e *pebbleEngine) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return e.idGenerator.NextRowID(ctx, tenantID, tableID)
}

func (e *pebbleEngine) NextTableID(ctx context.Context, tenantID int64) (int64, error) {
	return e.idGenerator.NextTableID(ctx, tenantID)
}

func (e *pebbleEngine) NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return e.idGenerator.NextIndexID(ctx, tenantID, tableID)
}