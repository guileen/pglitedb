package connection

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/guileen/pglitedb/logger"
	ctx "github.com/guileen/pglitedb/context"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
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

// Handler handles PostgreSQL client connections
type Handler struct {
	queryProcessor   interfaces.QueryProcessorInterface
	statementManager interfaces.PreparedStatementManagerInterface
	parser           sql.Parser
	
	// Extended query protocol state
	preparedStatements map[string]*PreparedStatement
	portals           map[string]*Portal
	psMutex           *sync.RWMutex
}

// NewHandler creates a new connection handler
func NewHandler(queryProcessor interfaces.QueryProcessorInterface, statementManager interfaces.PreparedStatementManagerInterface, parser sql.Parser) *Handler {
	return &Handler{
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		parser:            parser,
		preparedStatements: make(map[string]*PreparedStatement),
		portals:           make(map[string]*Portal),
		psMutex:           &sync.RWMutex{},
	}
}

// HandleConnection handles a new client connection
func (h *Handler) HandleConnection(conn net.Conn) error {
	logger.Info("Handling new client connection", "remote_addr", conn.RemoteAddr().String(), "local_addr", conn.LocalAddr().String())
	
	// Get a RequestContext from the pool
	reqCtx := ctx.GetRequestContext()
	defer ctx.PutRequestContext(reqCtx)
	
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
		logger.Warn("Unsupported startup message type", "type", fmt.Sprintf("%T", startupMessage), "remote_addr", conn.RemoteAddr().String())
		log.Printf("Unsupported startup message type: %T", startupMessage)
		return fmt.Errorf("unsupported startup message type: %T", startupMessage)
	}
	
	// Main message loop
	logger.Debug("Entering main message loop", "remote_addr", conn.RemoteAddr().String())
	messageCount := 0
	for {
		msg, err := backend.Receive()
		if err != nil {
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
			shouldClose, err = h.queryProcessor.ProcessQuery(context.Background(), backend, msg.String)
			if err != nil {
				logger.Error("Query processing failed", "error", err, "query", msg.String)
				h.sendErrorAndReady(backend, "42000", fmt.Sprintf("Query processing failed: %v", err))
				shouldClose = true
			}
		case *pgproto3.Parse:
			logger.Debug("Handling Parse message", "name", msg.Name, "query", msg.Query, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose, err = h.statementManager.Parse(context.Background(), backend, msg)
			if err != nil {
				logger.Error("Parse failed", "error", err, "name", msg.Name, "query", msg.Query)
				h.sendErrorAndReady(backend, "42000", fmt.Sprintf("Parse failed: %v", err))
				shouldClose = true
			}
		case *pgproto3.Bind:
			logger.Debug("Handling Bind message", "destinationPortal", msg.DestinationPortal, "preparedStatement", msg.PreparedStatement, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose, err = h.statementManager.Bind(context.Background(), backend, msg)
			if err != nil {
				logger.Error("Bind failed", "error", err, "destinationPortal", msg.DestinationPortal, "preparedStatement", msg.PreparedStatement)
				h.sendErrorAndReady(backend, "42000", fmt.Sprintf("Bind failed: %v", err))
				shouldClose = true
			}
		case *pgproto3.Describe:
			logger.Debug("Handling Describe message", "objectType", string(msg.ObjectType), "name", msg.Name, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose, err = h.statementManager.Describe(context.Background(), backend, msg)
			if err != nil {
				logger.Error("Describe failed", "error", err, "objectType", string(msg.ObjectType), "name", msg.Name)
				h.sendErrorAndReady(backend, "42000", fmt.Sprintf("Describe failed: %v", err))
				shouldClose = true
			}
		case *pgproto3.Execute:
			logger.Debug("Handling Execute message", "portal", msg.Portal, "maxRows", msg.MaxRows, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			// Delegate to statement manager
			shouldClose, err = h.statementManager.Execute(context.Background(), backend, msg)
			if err != nil {
				logger.Error("Execute failed", "error", err, "portal", msg.Portal, "maxRows", msg.MaxRows)
				h.sendErrorAndReady(backend, "42000", fmt.Sprintf("Execute failed: %v", err))
				shouldClose = true
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
			logger.Warn("Unsupported message type", "type", fmt.Sprintf("%T", msg), "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			log.Printf("Unsupported message type: %T", msg)
			h.sendErrorAndReady(backend, "0A000", fmt.Sprintf("Unsupported message type: %T", msg))
			shouldClose = true
		}
		
		if shouldClose {
			logger.Debug("Closing connection due to error", "remote_addr", conn.RemoteAddr().String())
			return nil
		}
	}
}

// Close closes the connection handler
func (h *Handler) Close() error {
	// Clean up any resources held by the handler
	h.psMutex.Lock()
	defer h.psMutex.Unlock()
	
	// Clear prepared statements and portals
	h.preparedStatements = make(map[string]*PreparedStatement)
	h.portals = make(map[string]*Portal)
	
	return nil
}

// HealthCheck performs a health check on the connection handler
func (h *Handler) HealthCheck() error {
	// Perform any necessary health checks
	return nil
}

// sendErrorAndReady sends an error response followed by a ReadyForQuery message
func (h *Handler) sendErrorAndReady(backend *pgproto3.Backend, code, message string) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	backend.Flush()
}