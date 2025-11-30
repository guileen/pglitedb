package pgserver

import (
	"net"
	
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// ConnectionHandlerInterface defines the interface for connection handling
type ConnectionHandlerInterface interface {
	HandleConnection(conn net.Conn) error
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(queryProcessor interfaces.QueryProcessorInterface, statementManager interfaces.PreparedStatementManagerInterface, parser sql.Parser) interfaces.ConnectionHandlerInterface {
	// Create the proper connection handler implementation
	return components.NewConnectionHandler(queryProcessor, statementManager, parser)
}