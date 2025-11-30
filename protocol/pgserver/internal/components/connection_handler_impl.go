package components

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/guileen/pglitedb/logger"
	customctx "github.com/guileen/pglitedb/context"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// Ensure ConnectionHandler implements ConnectionHandlerInterface
var _ interfaces.ConnectionHandlerInterface = &ConnectionHandler{}

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
	queryProcessor   interfaces.QueryProcessorInterface
	statementManager interfaces.PreparedStatementManagerInterface
	parser           sql.Parser
	
	// Timeout configuration
	connectionTimeout time.Duration
	idleTimeout       time.Duration
	maxLifetime       time.Duration
	
	// Extended query protocol state
	preparedStatements map[string]*PreparedStatement
	portals           map[string]*Portal
	psMutex           *sync.RWMutex
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(queryProcessor interfaces.QueryProcessorInterface, statementManager interfaces.PreparedStatementManagerInterface, parser sql.Parser) *ConnectionHandler {
	return &ConnectionHandler{
		queryProcessor:     queryProcessor,
		statementManager:   statementManager,
		parser:             parser,
		connectionTimeout:  30 * time.Second, // Default timeout
		idleTimeout:        5 * time.Minute,  // Default idle timeout
		maxLifetime:        1 * time.Hour,    // Default max lifetime
		preparedStatements: make(map[string]*PreparedStatement),
		portals:            make(map[string]*Portal),
		psMutex:            &sync.RWMutex{},
	}
}

// NewConnectionHandlerWithTimeout creates a new connection handler with timeout configuration
func NewConnectionHandlerWithTimeout(queryProcessor interfaces.QueryProcessorInterface, statementManager interfaces.PreparedStatementManagerInterface, parser sql.Parser, connectionTimeout, idleTimeout, maxLifetime time.Duration) *ConnectionHandler {
	return &ConnectionHandler{
		queryProcessor:     queryProcessor,
		statementManager:   statementManager,
		parser:             parser,
		connectionTimeout:  connectionTimeout,
		idleTimeout:        idleTimeout,
		maxLifetime:        maxLifetime,
		preparedStatements: make(map[string]*PreparedStatement),
		portals:            make(map[string]*Portal),
		psMutex:            &sync.RWMutex{},
	}
}

// HandleConnection handles a new client connection
func (ch *ConnectionHandler) HandleConnection(conn net.Conn) error {
	logger.Info("Handling new client connection", "remote_addr", conn.RemoteAddr().String(), "local_addr", conn.LocalAddr().String())
	
	// Set connection timeout
	if ch.connectionTimeout > 0 {
		if deadlineErr := conn.SetDeadline(time.Now().Add(ch.connectionTimeout)); deadlineErr != nil {
			logger.Error("Failed to set connection deadline", "error", deadlineErr, "remote_addr", conn.RemoteAddr().String())
		}
	}
	
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
		// Check if this is a timeout error
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			logger.Warn("Connection timeout during startup", "remote_addr", conn.RemoteAddr().String())
			return fmt.Errorf("connection timeout during startup: %w", err)
		}
		
		logger.Error("Failed to receive startup message", "error", err, "remote_addr", conn.RemoteAddr().String())
		log.Printf("Failed to receive startup message: %v", err)
		return err
	}
	
	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		logger.Debug("Received StartupMessage, sending authentication OK", "remote_addr", conn.RemoteAddr().String())
		// Send authentication OK
		backend.Send(&pgproto3.AuthenticationOk{})
		if err := backend.Flush(); err != nil {
			logger.Error("Failed to send AuthenticationOk", "error", err, "remote_addr", conn.RemoteAddr().String())
			log.Printf("Failed to send AuthenticationOk: %v", err)
			return err
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
			return err
		}
	default:
		err := fmt.Errorf("unsupported startup message type: %T", startupMessage)
		logger.Warn("Unsupported startup message type", "type", fmt.Sprintf("%T", startupMessage), "remote_addr", conn.RemoteAddr().String())
		log.Printf("Unsupported startup message type: %T", startupMessage)
		return err
	}
	
	// Main message loop
	logger.Debug("Entering main message loop", "remote_addr", conn.RemoteAddr().String())
	messageCount := 0
	for {
		// Reset idle timeout for each message
		if ch.idleTimeout > 0 {
			if deadlineErr := conn.SetDeadline(time.Now().Add(ch.idleTimeout)); deadlineErr != nil {
				logger.Error("Failed to set idle timeout", "error", deadlineErr, "remote_addr", conn.RemoteAddr().String())
			}
		}
		
		msg, err := backend.Receive()
		if err != nil {
			// Check if this is a timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				logger.Warn("Connection timeout during message receive", "remote_addr", conn.RemoteAddr().String(), "message_count", messageCount)
				// Send error response to client
				backend.Send(&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Code:     "08006", // connection failure
					Message:  "connection timeout",
				})
				backend.Flush()
				return fmt.Errorf("connection timeout: %w", err)
			}
			
			logger.Error("Failed to receive message", "error", err, "remote_addr", conn.RemoteAddr().String())
			log.Printf("Failed to receive message: %v", err)
			return err
		}
		
		messageCount++
		logger.Debug("Received message", "type", fmt.Sprintf("%T", msg), "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
		
		shouldClose := false
		switch msg := msg.(type) {
		case *pgproto3.Query:
			logger.Debug("Handling Query message", "query", msg.String, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to query processor
			shouldClose, err = ch.handleQuery(backend, msg.String)
			if err != nil {
				logger.Error("Failed to handle Query message", "error", err, "remote_addr", conn.RemoteAddr().String())
				return err
			}
		case *pgproto3.Parse:
			logger.Debug("Handling Parse message", "name", msg.Name, "query", msg.Query, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose, err = ch.handleParse(backend, msg)
			if err != nil {
				logger.Error("Failed to handle Parse message", "error", err, "remote_addr", conn.RemoteAddr().String())
				return err
			}
		case *pgproto3.Bind:
			logger.Debug("Handling Bind message", "destinationPortal", msg.DestinationPortal, "preparedStatement", msg.PreparedStatement, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose, err = ch.handleBind(backend, msg)
			if err != nil {
				logger.Error("Failed to handle Bind message", "error", err, "remote_addr", conn.RemoteAddr().String())
				return err
			}
		case *pgproto3.Describe:
			logger.Debug("Handling Describe message", "objectType", string(msg.ObjectType), "name", msg.Name, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose, err = ch.handleDescribe(backend, msg)
			if err != nil {
				logger.Error("Failed to handle Describe message", "error", err, "remote_addr", conn.RemoteAddr().String())
				return err
			}
		case *pgproto3.Execute:
			logger.Debug("Handling Execute message", "portal", msg.Portal, "maxRows", msg.MaxRows, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose, err = ch.handleExecute(backend, msg)
			if err != nil {
				logger.Error("Failed to handle Execute message", "error", err, "remote_addr", conn.RemoteAddr().String())
				return err
			}
		case *pgproto3.Sync:
			logger.Debug("Handling Sync message", "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				logger.Error("Failed to flush ReadyForQuery", "error", err, "remote_addr", conn.RemoteAddr().String())
				shouldClose = true
			}
		case *pgproto3.Terminate:
			logger.Debug("Handling Terminate message", "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			return nil
		default:
			err := fmt.Errorf("unsupported message type: %T", msg)
			logger.Warn("Unsupported message type", "type", fmt.Sprintf("%T", msg), "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			log.Printf("Unsupported message type: %T", msg)
			return err
		}
		
		if shouldClose {
			logger.Debug("Closing connection due to error", "remote_addr", conn.RemoteAddr().String())
			return fmt.Errorf("connection should be closed")
		}
	}
}

// Extended Query Protocol handlers
func (ch *ConnectionHandler) handleQuery(backend *pgproto3.Backend, query string) (bool, error) {
	// Delegate to query processor if available
	if ch.queryProcessor != nil {
		return ch.queryProcessor.ProcessQuery(context.Background(), backend, query)
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush query response", "error", err)
		return true, err
	}
	return false, nil
}

func (ch *ConnectionHandler) handleParse(backend *pgproto3.Backend, msg *pgproto3.Parse) (bool, error) {
	// Delegate to statement manager if available
	if ch.statementManager != nil {
		shouldClose, err := ch.statementManager.Parse(context.Background(), backend, msg)
		return shouldClose, err
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.ParseComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Parse response", "error", err)
		return true, err
	}
	return false, nil
}

func (ch *ConnectionHandler) handleBind(backend *pgproto3.Backend, msg *pgproto3.Bind) (bool, error) {
	// Delegate to statement manager if available
	if ch.statementManager != nil {
		shouldClose, err := ch.statementManager.Bind(context.Background(), backend, msg)
		return shouldClose, err
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.BindComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Bind response", "error", err)
		return true, err
	}
	return false, nil
}

func (ch *ConnectionHandler) handleDescribe(backend *pgproto3.Backend, msg *pgproto3.Describe) (bool, error) {
	// Delegate to statement manager if available
	if ch.statementManager != nil {
		shouldClose, err := ch.statementManager.Describe(context.Background(), backend, msg)
		return shouldClose, err
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.NoData{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Describe response", "error", err)
		return true, err
	}
	return false, nil
}

func (ch *ConnectionHandler) handleExecute(backend *pgproto3.Backend, msg *pgproto3.Execute) (bool, error) {
	// Delegate to statement manager if available
	if ch.statementManager != nil {
		shouldClose, err := ch.statementManager.Execute(context.Background(), backend, msg)
		return shouldClose, err
	}
	
	// Fallback implementation - send a simple response
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("EXECUTE")})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Execute response", "error", err)
		return true, err
	}
	return false, nil
}

// Close closes the connection handler
func (ch *ConnectionHandler) Close() error {
	// Currently no resources to close, but this satisfies the interface
	// In a more complex implementation, this might close connections or clean up resources
	return nil
}

// HealthCheck performs a health check on the connection handler
func (ch *ConnectionHandler) HealthCheck() error {
	// Perform any necessary health checks
	return nil
}

// HandleMessage handles a single message
func (ch *ConnectionHandler) HandleMessage(ctx context.Context, backend *pgproto3.Backend, msg interface{}) (bool, error) {
	switch msg := msg.(type) {
	case *pgproto3.Query:
		return ch.handleQuery(backend, msg.String)
	case *pgproto3.Parse:
		return ch.handleParse(backend, msg)
	case *pgproto3.Bind:
		return ch.handleBind(backend, msg)
	case *pgproto3.Describe:
		return ch.handleDescribe(backend, msg)
	case *pgproto3.Execute:
		return ch.handleExecute(backend, msg)
	case *pgproto3.Sync:
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			return true, err
		}
		return false, nil
	case *pgproto3.Terminate:
		return true, nil
	default:
		return false, fmt.Errorf("unsupported message type: %T", msg)
	}
}