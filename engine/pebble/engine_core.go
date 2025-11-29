package pebble

import (
	"context"
	"reflect"
	"time"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/indexes"
	"github.com/guileen/pglitedb/engine/pebble/operations/query"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/engine/pebble/resources"
	"github.com/guileen/pglitedb/engine/pebble/utils"
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
	deadlockDetector    *utils.DeadlockDetector
	iteratorPool        *scan.IteratorPool
}

func NewPebbleEngine(kvStore storage.KV, c codec.Codec) engineTypes.StorageEngine {
	queryOps := query.NewQueryOperations(kvStore, c)
	insertOps := query.NewInsertOperations(kvStore, c)
	updateOps := query.NewUpdateOperations(kvStore, c)
	deleteOps := query.NewDeleteOperations(kvStore, c)
	
	// Track the KV store connection for leak detection
	rm := resources.GetResourceManager()
	rm.TrackConnection(kvStore)
	
	// Create deadlock detector with abort callback
	deadlockDetector := utils.NewDeadlockDetector(100*time.Millisecond, func(txnID uint64) {
		// This is a placeholder - in a real implementation we would need to signal
		// the transaction manager to abort this transaction
	})
	
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
		deadlockDetector:    deadlockDetector,
		iteratorPool:        scan.NewIteratorPool(),
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

// getTransactionID extracts the transaction ID from a storage.Transaction
func getTransactionID(txn storage.Transaction) uint64 {
	// Try to get the transaction ID from the extended interface
	if txnWithID, ok := txn.(interface{ TxnID() uint64 }); ok {
		txnID := txnWithID.TxnID()
		return txnID
	}
	
	// Fallback to reflection for other transaction types
	val := reflect.ValueOf(txn)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	
	if val.Kind() == reflect.Struct {
		field := val.FieldByName("txnID")
		if field.IsValid() && field.Kind() == reflect.Uint64 {
			txnID := field.Uint()
			return txnID
		}
	}
	
	return 0
}

// GetDeadlockDetector returns the deadlock detector used by the engine
func (e *pebbleEngine) GetDeadlockDetector() *utils.DeadlockDetector {
	return e.deadlockDetector
}

// CheckForConflicts checks for conflicts with the given key using the underlying KV store
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