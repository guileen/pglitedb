// Package context provides context pooling functionality for PGLiteDB
package context

import (
	"sync"
	"time"
)

// RequestContext represents a request context with all necessary information
type RequestContext struct {
	RequestID  string
	UserID     string
	Timestamp  time.Time
	Metadata   map[string]interface{}
	Headers    map[string]string
}

// Reset clears the state of the RequestContext for reuse
func (rc *RequestContext) Reset() {
	rc.RequestID = ""
	rc.UserID = ""
	rc.Timestamp = time.Time{}
	
	// Reset metadata map
	for k := range rc.Metadata {
		delete(rc.Metadata, k)
	}
	
	// Reset headers map
	for k := range rc.Headers {
		delete(rc.Headers, k)
	}
}

// TransactionContext represents a transaction context
type TransactionContext struct {
	TxID       string
	Isolation  string
	StartTime  time.Time
	ReadOnly   bool
	AutoCommit bool
}

// Reset clears the state of the TransactionContext for reuse
func (tc *TransactionContext) Reset() {
	tc.TxID = ""
	tc.Isolation = ""
	tc.StartTime = time.Time{}
	tc.ReadOnly = false
	tc.AutoCommit = false
}

// QueryContext represents a query execution context
type QueryContext struct {
	QueryID    string
	SQL        string
	Parameters []interface{}
	StartTime  time.Time
}

// Reset clears the state of the QueryContext for reuse
func (qc *QueryContext) Reset() {
	qc.QueryID = ""
	qc.SQL = ""
	qc.Parameters = qc.Parameters[:0]
	qc.StartTime = time.Time{}
}

// RequestContextPool manages pooling of RequestContext objects
type RequestContextPool struct {
	pool *sync.Pool
}

// NewRequestContextPool creates a new RequestContextPool
func NewRequestContextPool() *RequestContextPool {
	return &RequestContextPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &RequestContext{
					Metadata: make(map[string]interface{}),
					Headers:  make(map[string]string),
				}
			},
		},
	}
}

// Get retrieves a RequestContext from the pool
func (rcp *RequestContextPool) Get() *RequestContext {
	ctx := rcp.pool.Get().(*RequestContext)
	return ctx
}

// Put returns a RequestContext to the pool
func (rcp *RequestContextPool) Put(ctx *RequestContext) {
	if ctx != nil {
		ctx.Reset()
		rcp.pool.Put(ctx)
	}
}

// TransactionContextPool manages pooling of TransactionContext objects
type TransactionContextPool struct {
	pool *sync.Pool
}

// NewTransactionContextPool creates a new TransactionContextPool
func NewTransactionContextPool() *TransactionContextPool {
	return &TransactionContextPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &TransactionContext{}
			},
		},
	}
}

// Get retrieves a TransactionContext from the pool
func (tcp *TransactionContextPool) Get() *TransactionContext {
	ctx := tcp.pool.Get().(*TransactionContext)
	return ctx
}

// Put returns a TransactionContext to the pool
func (tcp *TransactionContextPool) Put(ctx *TransactionContext) {
	if ctx != nil {
		ctx.Reset()
		tcp.pool.Put(ctx)
	}
}

// QueryContextPool manages pooling of QueryContext objects
type QueryContextPool struct {
	pool *sync.Pool
}

// NewQueryContextPool creates a new QueryContextPool
func NewQueryContextPool() *QueryContextPool {
	return &QueryContextPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &QueryContext{
					Parameters: make([]interface{}, 0, 8), // Initial capacity of 8
				}
			},
		},
	}
}

// Get retrieves a QueryContext from the pool
func (qcp *QueryContextPool) Get() *QueryContext {
	ctx := qcp.pool.Get().(*QueryContext)
	return ctx
}

// Put returns a QueryContext to the pool
func (qcp *QueryContextPool) Put(ctx *QueryContext) {
	if ctx != nil {
		ctx.Reset()
		qcp.pool.Put(ctx)
	}
}

// Global context pools
var (
	requestContextPool    *RequestContextPool
	transactionContextPool *TransactionContextPool
	queryContextPool      *QueryContextPool
)

// Initialize initializes the global context pools
func Initialize() {
	requestContextPool = NewRequestContextPool()
	transactionContextPool = NewTransactionContextPool()
	queryContextPool = NewQueryContextPool()
}

// GetRequestContext retrieves a RequestContext from the global pool
func GetRequestContext() *RequestContext {
	if requestContextPool == nil {
		Initialize()
	}
	return requestContextPool.Get()
}

// PutRequestContext returns a RequestContext to the global pool
func PutRequestContext(ctx *RequestContext) {
	if requestContextPool != nil {
		requestContextPool.Put(ctx)
	}
}

// GetTransactionContext retrieves a TransactionContext from the global pool
func GetTransactionContext() *TransactionContext {
	if transactionContextPool == nil {
		Initialize()
	}
	return transactionContextPool.Get()
}

// PutTransactionContext returns a TransactionContext to the global pool
func PutTransactionContext(ctx *TransactionContext) {
	if transactionContextPool != nil {
		transactionContextPool.Put(ctx)
	}
}

// GetQueryContext retrieves a QueryContext from the global pool
func GetQueryContext() *QueryContext {
	if queryContextPool == nil {
		Initialize()
	}
	return queryContextPool.Get()
}

// PutQueryContext returns a QueryContext to the global pool
func PutQueryContext(ctx *QueryContext) {
	if queryContextPool != nil {
		queryContextPool.Put(ctx)
	}
}