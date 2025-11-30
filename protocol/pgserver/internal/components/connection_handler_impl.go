package components

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/guileen/pglitedb/logger"
	customctx "github.com/guileen/pglitedb/context"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/guileen/pglitedb/protocol/sql"
)

// PreparedStatement represents a parsed SQL statement
type PreparedStatement struct {
	Name            string
	Query           string
	PreprocessedSQL string
	ParameterOIDs   []uint32
	ReturningColumns []string
}

// Portal represents a bound statement with parameters
type Portal struct {
	Name         string
	Statement    *PreparedStatement
	Params       []interface{}
	ParamFormats []int16
}

// ConnectionHandler handles PostgreSQL client connections
type ConnectionHandler struct {
	queryProcessor   interface{} // Will be QueryProcessorInterface
	statementManager interface{} // Will be PreparedStatementManagerInterface
	parser           sql.Parser
	
	// Extended query protocol state
	preparedStatements map[string]*PreparedStatement
	portals           map[string]*Portal
	psMutex           *sync.RWMutex
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(queryProcessor interface{}, statementManager interface{}, parser sql.Parser) *ConnectionHandler {
	return &ConnectionHandler{
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		parser:            parser,
		preparedStatements: make(map[string]*PreparedStatement),
		portals:           make(map[string]*Portal),
		psMutex:           &sync.RWMutex{},
	}
}

// HandleConnection handles a new client connection
func (ch *ConnectionHandler) HandleConnection(conn net.Conn) {
	logger.Info("Handling new client connection", "remote_addr", conn.RemoteAddr().String(), "local_addr", conn.LocalAddr().String())
	
	// Get a RequestContext from the pool
	reqCtx := customctx.GetRequestContext()
	defer customctx.PutRequestContext(reqCtx)
	
	defer func() {
		conn.Close()
		logger.Info("Client connection closed", "remote_addr", conn.RemoteAddr().String(), "local_addr", conn.LocalAddr().String())
	}()
	
	backend := pgproto3.NewBackend(conn, conn)
	
	// Handle startup message
	startupMessage, err := backend.ReceiveStartupMessage()
	if err != nil {
		logger.Error("Failed to receive startup message", "error", err, "remote_addr", conn.RemoteAddr().String())
		log.Printf("Failed to receive startup message: %v", err)
		return
	}
	
	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		logger.Debug("Received StartupMessage, sending authentication OK", "remote_addr", conn.RemoteAddr().String())
		// Send authentication OK
		backend.Send(&pgproto3.AuthenticationOk{})
		if err := backend.Flush(); err != nil {
			logger.Error("Failed to send AuthenticationOk", "error", err, "remote_addr", conn.RemoteAddr().String())
			log.Printf("Failed to send AuthenticationOk: %v", err)
			return
		}
		
		// Send ParameterStatus messages
		logger.Debug("Sending ParameterStatus messages", "remote_addr", conn.RemoteAddr().String())
		backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0 (PGLiteDB)"})
		backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
		backend.Send(&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"})
		backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
		
		// Send ReadyForQuery
		logger.Debug("Sending ReadyForQuery", "remote_addr", conn.RemoteAddr().String())
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			logger.Error("Failed to send ReadyForQuery", "error", err, "remote_addr", conn.RemoteAddr().String())
			log.Printf("Failed to send ReadyForQuery: %v", err)
			return
		}
	default:
		logger.Warn("Unsupported startup message type", "type", fmt.Sprintf("%T", startupMessage), "remote_addr", conn.RemoteAddr().String())
		log.Printf("Unsupported startup message type: %T", startupMessage)
		return
	}
	
	// Main message loop
	logger.Debug("Entering main message loop", "remote_addr", conn.RemoteAddr().String())
	messageCount := 0
	for {
		msg, err := backend.Receive()
		if err != nil {
			logger.Error("Failed to receive message", "error", err, "remote_addr", conn.RemoteAddr().String())
			log.Printf("Failed to receive message: %v", err)
			return
		}
		
		messageCount++
		logger.Debug("Received message", "type", fmt.Sprintf("%T", msg), "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
		
		shouldClose := false
		switch msg := msg.(type) {
		case *pgproto3.Query:
			logger.Debug("Handling Query message", "query", msg.String, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to query processor
			// For now, we'll keep the original logic but this should be refactored
			shouldClose = ch.handleQuery(backend, msg.String)
		case *pgproto3.Parse:
			logger.Debug("Handling Parse message", "name", msg.Name, "query", msg.Query, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose = ch.handleParse(backend, msg)
		case *pgproto3.Bind:
			logger.Debug("Handling Bind message", "destinationPortal", msg.DestinationPortal, "preparedStatement", msg.PreparedStatement, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose = ch.handleBind(backend, msg)
		case *pgproto3.Describe:
			logger.Debug("Handling Describe message", "objectType", string(msg.ObjectType), "name", msg.Name, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose = ch.handleDescribe(backend, msg)
		case *pgproto3.Execute:
			logger.Debug("Handling Execute message", "portal", msg.Portal, "maxRows", msg.MaxRows, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose = ch.handleExecute(backend, msg)
		case *pgproto3.Sync:
			logger.Debug("Handling Sync message", "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				logger.Error("Failed to flush ReadyForQuery", "error", err, "remote_addr", conn.RemoteAddr().String())
				shouldClose = true
			}
		case *pgproto3.Terminate:
			logger.Debug("Handling Terminate message", "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			return
		default:
			logger.Warn("Unsupported message type", "type", fmt.Sprintf("%T", msg), "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			log.Printf("Unsupported message type: %T", msg)
			shouldClose = true
		}
		
		if shouldClose {
			logger.Debug("Closing connection due to error", "remote_addr", conn.RemoteAddr().String())
			return
		}
	}
}

// Extended Query Protocol handlers
func (ch *ConnectionHandler) handleQuery(backend *pgproto3.Backend, query string) bool {
	// Delegate to query processor if available
	if ch.queryProcessor != nil {
		// Type assert to the expected interface and call ProcessQuery
		if processor, ok := ch.queryProcessor.(interface {
			ProcessQuery(ctx context.Context, backend *pgproto3.Backend, query string) (bool, error)
		}); ok {
			shouldClose, err := processor.ProcessQuery(context.Background(), backend, query)
			if err != nil {
				logger.Error("Query processing failed", "error", err)
				return true
			}
			return shouldClose
		}
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush query response", "error", err)
		return true
	}
	return false
}

func (ch *ConnectionHandler) handleParse(backend *pgproto3.Backend, msg *pgproto3.Parse) bool {
	// Delegate to statement manager if available
	if ch.statementManager != nil {
		// Type assert to the expected interface and call Parse
		if manager, ok := ch.statementManager.(interface {
			Parse(backend *pgproto3.Backend, msg *pgproto3.Parse) bool
		}); ok {
			return manager.Parse(backend, msg)
		}
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.ParseComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Parse response", "error", err)
		return true
	}
	return false
}

func (ch *ConnectionHandler) handleBind(backend *pgproto3.Backend, msg *pgproto3.Bind) bool {
	// Delegate to statement manager if available
	if ch.statementManager != nil {
		// Type assert to the expected interface and call Bind
		if manager, ok := ch.statementManager.(interface {
			Bind(backend *pgproto3.Backend, msg *pgproto3.Bind) bool
		}); ok {
			return manager.Bind(backend, msg)
		}
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.BindComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Bind response", "error", err)
		return true
	}
	return false
}

func (ch *ConnectionHandler) handleDescribe(backend *pgproto3.Backend, msg *pgproto3.Describe) bool {
	// Delegate to statement manager if available
	if ch.statementManager != nil {
		// Type assert to the expected interface and call Describe
		if manager, ok := ch.statementManager.(interface {
			Describe(backend *pgproto3.Backend, msg *pgproto3.Describe) bool
		}); ok {
			return manager.Describe(backend, msg)
		}
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.NoData{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Describe response", "error", err)
		return true
	}
	return false
}

func (ch *ConnectionHandler) handleExecute(backend *pgproto3.Backend, msg *pgproto3.Execute) bool {
	// Delegate to statement manager if available
	if ch.statementManager != nil {
		// Type assert to the expected interface and call Execute
		if manager, ok := ch.statementManager.(interface {
			Execute(backend *pgproto3.Backend, msg *pgproto3.Execute) bool
		}); ok {
			return manager.Execute(backend, msg)
		}
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("EXECUTE")})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Execute response", "error", err)
		return true
	}
	return false
}