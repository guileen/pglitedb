package pools

import (
	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// TxnPool manages transaction resources
type TxnPool struct {
	BasePool
}

// TransactionWrapper wraps a transaction for pooling
type TransactionWrapper struct {
	txn engineTypes.Transaction
}

// NewTxnPool creates a new transaction pool
func NewTxnPool() *TxnPool {
	return &TxnPool{
		BasePool: *NewBasePool("txn", func() interface{} {
			return &TransactionWrapper{}
		}),
	}
}

// Acquire gets a transaction from the pool
func (tp *TxnPool) Acquire() *TransactionWrapper {
	txn := tp.BasePool.pool.Get()
	fromPool := txn != nil
	
	if !fromPool {
		txn = &TransactionWrapper{}
	}
	
	return txn.(*TransactionWrapper)
}

// Release returns a transaction to the pool
func (tp *TxnPool) Release(txn *TransactionWrapper) {
	txn.txn = nil
	tp.BasePool.Put(txn)
}

// TxnIDPool manages transaction ID resources
type TxnIDPool struct {
	BasePool
}

// TxnIDWrapper wraps a transaction ID for pooling
type TxnIDWrapper struct {
	ID uint64
}

// NewTxnIDPool creates a new transaction ID pool
func NewTxnIDPool() *TxnIDPool {
	return &TxnIDPool{
		BasePool: *NewBasePool("txnID", func() interface{} {
			return &TxnIDWrapper{}
		}),
	}
}

// AcquireTxnID gets a transaction ID from the pool
func (tip *TxnIDPool) AcquireTxnID() *TxnIDWrapper {
	id := tip.BasePool.pool.Get()
	fromPool := id != nil
	
	if !fromPool {
		id = &TxnIDWrapper{}
	}
	
	return id.(*TxnIDWrapper)
}

// ReleaseTxnID returns a transaction ID to the pool
func (tip *TxnIDPool) ReleaseTxnID(id *TxnIDWrapper) {
	id.ID = 0
	tip.BasePool.Put(id)
}