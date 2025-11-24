package operators

import (
	"io"

	"github.com/guileen/pglitedb/types"
)

type PhysicalOperator interface {
	Open() error
	Next() (*types.Record, error)
	Close() error
}

var EOF = io.EOF
