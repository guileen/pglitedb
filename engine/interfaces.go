package engine

import (
	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// StorageEngine defines the interface for storage engines
type StorageEngine interface {
	engineTypes.RowOperations
	engineTypes.IndexOperations
	engineTypes.ScanOperations
	engineTypes.TransactionOperations
	engineTypes.IDGeneration
	
	// Resource management
	Close() error
}