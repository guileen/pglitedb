package components

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/guileen/pglitedb/logger"
	ctx "github.com/guileen/pglitedb/context"
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
	// This is a placeholder - in a real implementation, this would delegate to the query processor
	return false
}

func (ch *ConnectionHandler) handleParse(backend *pgproto3.Backend, msg *pgproto3.Parse) bool {
	logger.Debug("Parsing prepared statement", "name", msg.Name, "query", msg.Query, "parameter_count", len(msg.ParameterOIDs))
	
	// Create a prepared statement
	stmt := &PreparedStatement{
		Name:  msg.Name,
		Query: msg.Query,
		ParameterOIDs: msg.ParameterOIDs,
	}
	
	// Parse the query to extract RETURNING columns if present
	startTime := time.Now()
	parsed, err := ch.parser.Parse(msg.Query)
	parseDuration := time.Since(startTime)
	if err == nil {
		stmt.ReturningColumns = parsed.ReturningColumns
		logger.Debug("Query parsed for prepared statement", "parse_duration", parseDuration.String(), "returning_columns", parsed.ReturningColumns)
	} else {
		logger.Warn("Failed to parse query for prepared statement", "error", err, "parse_duration", parseDuration.String())
	}
	
	// Store the prepared statement with mutex protection
	ch.psMutex.Lock()
	if msg.Name == "" {
		// unnamed statement
		ch.preparedStatements[""] = stmt
		logger.Debug("Stored unnamed prepared statement")
	} else {
		ch.preparedStatements[msg.Name] = stmt
		logger.Debug("Stored named prepared statement", "name", msg.Name)
	}
	ch.psMutex.Unlock()
	
	backend.Send(&pgproto3.ParseComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush ParseComplete", "error", err)
		return true
	}
	
	logger.Debug("Parse completed successfully")
	return false
}

func (ch *ConnectionHandler) handleBind(backend *pgproto3.Backend, msg *pgproto3.Bind) bool {
	// Placeholder implementation
	return false
}

func (ch *ConnectionHandler) handleDescribe(backend *pgproto3.Backend, msg *pgproto3.Describe) bool {
	// Placeholder implementation
	return false
}

func (ch *ConnectionHandler) handleExecute(backend *pgproto3.Backend, msg *pgproto3.Execute) bool {
	// Placeholder implementation
	return false
}