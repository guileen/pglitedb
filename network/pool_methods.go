package network

import (
	"sync/atomic"
)

// IsClosed returns whether the connection pool is closed
func (p *ConnectionPool) IsClosed() bool {
	return atomic.LoadInt32(&p.closed) == 1
}