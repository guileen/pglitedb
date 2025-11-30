package pgserver

import (
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// PreparedStatementManagerInterface defines the interface for prepared statement management
type PreparedStatementManagerInterface interface {
	interfaces.PreparedStatementManagerInterface
}

// NewPreparedStatementManager creates a new prepared statement manager
func NewPreparedStatementManager(parser sql.Parser) interfaces.PreparedStatementManagerInterface {
	return components.NewPreparedStatementManager(parser)
}