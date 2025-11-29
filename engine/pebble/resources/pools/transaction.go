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