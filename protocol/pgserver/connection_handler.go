package pgserver

import (
	"net"
	
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
)

// ConnectionHandlerInterface defines the interface for connection handling
type ConnectionHandlerInterface interface {
	HandleConnection(conn net.Conn)
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(queryProcessor interface{}, statementManager interface{}, parser interface{}) ConnectionHandlerInterface {
	// Convert parser to sql.Parser type
	sqlParser, ok := parser.(sql.Parser)
	if !ok {
		// Fallback to nil if conversion fails
		return nil
	}
	
	// Create the proper connection handler implementation
	return components.NewConnectionHandler(queryProcessor, statementManager, sqlParser)
}