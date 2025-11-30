package pgserver

import (
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/jackc/pgx/v5/pgproto3"
)

// PreparedStatementManagerInterface defines the interface for prepared statement management
type PreparedStatementManagerInterface interface {
	Parse(backend *pgproto3.Backend, msg *pgproto3.Parse) bool
	Bind(backend *pgproto3.Backend, msg *pgproto3.Bind) bool
	Describe(backend *pgproto3.Backend, msg *pgproto3.Describe) bool
	Execute(backend *pgproto3.Backend, msg *pgproto3.Execute) bool
}

// NewPreparedStatementManager creates a new prepared statement manager
func NewPreparedStatementManager(parser interface{}) PreparedStatementManagerInterface {
	return components.NewPreparedStatementManager(parser)
}