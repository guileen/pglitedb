package pgserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/network"
	"github.com/guileen/pglitedb/types"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/guileen/pglitedb/logger"
)

type PostgreSQLServer struct {
	listener net.Listener
	executor *sql.Executor
	parser   sql.Parser
	planner  *sql.Planner
	mu       sync.Mutex
	closed   bool
	connectionPool *network.ConnectionPool
	
	// Extended query protocol state
	preparedStatements map[string]*PreparedStatement
	portals           map[string]*Portal
}

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

func NewPostgreSQLServer(executor *sql.Executor, planner *sql.Planner) *PostgreSQLServer {
	logger.Info("Creating new PostgreSQL server instance")
	parser := sql.NewPGParser()
	
	server := &PostgreSQLServer{
		executor: executor,
		parser:   parser,
		planner:  planner,
		preparedStatements: make(map[string]*PreparedStatement),
		portals:           make(map[string]*Portal),
	}
	
	logger.Info("PostgreSQL server instance created successfully")
	return server
}

func (s *PostgreSQLServer) Start(port string) error {
	logger.Info("Starting PostgreSQL server", "port", port, "protocol", "TCP")
	return s.StartTCP(port)
}

func (s *PostgreSQLServer) StartTCP(port string) error {
	logger.Info("Starting PostgreSQL server TCP listener", "port", port)
	var err error
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Error("Failed to start TCP listener", "error", err, "port", port)
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}
	
	logger.Info("PostgreSQL server listening on TCP port", "port", port)
	log.Printf("PostgreSQL server listening on TCP port %s", port)
	
	connectionCount := 0
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			
			if closed {
				logger.Info("TCP listener closed")
				return nil
			}
			logger.Error("Failed to accept TCP connection", "error", err)
			return fmt.Errorf("failed to accept TCP connection: %w", err)
		}
		
		connectionCount++
		logger.Debug("Accepted new TCP connection", "connection_count", connectionCount, "remote_addr", conn.RemoteAddr().String())
		go s.handleConnection(conn)
	}
}

func (s *PostgreSQLServer) StartUnix(socketPath string) error {
	logger.Info("Starting PostgreSQL server Unix socket listener", "socketPath", socketPath)
	// Remove existing socket file if it exists
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		logger.Warn("Failed to remove existing socket file", "error", err, "socketPath", socketPath)
		log.Printf("Warning: failed to remove existing socket file: %v", err)
	}
	
	var err error
	s.listener, err = net.Listen("unix", socketPath)
	if err != nil {
		logger.Error("Failed to start Unix socket listener", "error", err, "socketPath", socketPath)
		return fmt.Errorf("failed to start Unix socket listener: %w", err)
	}
	
	logger.Info("PostgreSQL server listening on Unix socket", "socketPath", socketPath)
	log.Printf("PostgreSQL server listening on Unix socket %s", socketPath)
	
	connectionCount := 0
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			
			if closed {
				logger.Info("Unix socket listener closed")
				return nil
			}
			logger.Error("Failed to accept Unix connection", "error", err)
			return fmt.Errorf("failed to accept Unix connection: %w", err)
		}
		
		connectionCount++
		logger.Debug("Accepted new Unix connection", "connection_count", connectionCount, "local_addr", conn.LocalAddr().String())
		go s.handleConnection(conn)
	}
}

func (s *PostgreSQLServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	logger.Info("Closing PostgreSQL server", "was_already_closed", s.closed)
	s.closed = true
	if s.listener != nil {
		err := s.listener.Close()
		if err != nil {
			logger.Error("Error closing listener", "error", err)
			return err
		}
		logger.Info("PostgreSQL server listener closed successfully")
	}
	
	logger.Info("PostgreSQL server closed successfully")
	return nil
}

// handleQuery handles the Query message (simple query protocol)
func (s *PostgreSQLServer) handleQuery(backend *pgproto3.Backend, query string) bool {
	ctx := context.Background()
	
	logger.Debug("Processing query", "query", query)
	
	// Handle empty query
	if strings.TrimSpace(query) == "" {
		logger.Debug("Empty query received")
		backend.Send(&pgproto3.EmptyQueryResponse{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			logger.Error("Failed to flush EmptyQueryResponse", "error", err)
			return true
		}
		return false
	}
	
	startTime := time.Now()
	parsed, err := s.parser.Parse(query)
	parseDuration := time.Since(startTime)
	if err != nil {
		logger.Warn("Failed to parse SQL query", "error", err, "query", query, "parse_duration", parseDuration.String())
		s.sendErrorAndReady(backend, "42601", fmt.Sprintf("Syntax error: failed to parse SQL query: %v", err))
		return false
	}
	logger.Debug("Query parsed successfully", "parse_duration", parseDuration.String())
	
	startTime = time.Now()
	result, err := s.planner.Execute(ctx, parsed.Query)
	executeDuration := time.Since(startTime)
	if err != nil {
		logger.Warn("Query execution failed", "error", err, "query", query, "execute_duration", executeDuration.String())
		s.sendErrorAndReady(backend, "42000", fmt.Sprintf("Query execution failed: %v", err))
		return false
	}
	logger.Debug("Query executed successfully", "execute_duration", executeDuration.String(), "row_count", result.Count)
	
	// Handle RETURNING clause for INSERT/UPDATE/DELETE
	if len(parsed.ReturningColumns) > 0 {
		logger.Debug("Processing RETURNING clause", "returning_columns", parsed.ReturningColumns)
		returningResult := s.buildReturningResult(result, parsed.ReturningColumns)
		s.sendReturningResult(backend, returningResult)
		if err := backend.Flush(); err != nil {
			logger.Error("Failed to flush RETURNING result", "error", err)
			return true
		}
		return false
	}
	
	if len(result.Columns) > 0 {
		logger.Debug("Sending result set", "column_count", len(result.Columns), "row_count", result.Count)
		fields := make([]pgproto3.FieldDescription, len(result.Columns))
		for i, col := range result.Columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(col),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          705, // UNKNOWN type
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0, // Text format
			}
		}
		
		backend.Send(&pgproto3.RowDescription{Fields: fields})
		
		for i, row := range result.Rows {
			// Convert row values to strings
			values := make([][]byte, len(row))
			for j, val := range row {
				if val == nil {
					values[j] = nil
				} else {
					values[j] = []byte(fmt.Sprintf("%v", val))
				}
			}
			backend.Send(&pgproto3.DataRow{Values: values})
			if i%1000 == 0 && i > 0 {
				logger.Debug("Sent data rows", "sent_rows", i, "total_rows", len(result.Rows))
			}
		}
		
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", result.Count))})
	} else {
		// For INSERT/UPDATE/DELETE operations without RETURNING
		var commandTag string
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "INSERT") {
			if result.LastInsertID > 0 {
				commandTag = fmt.Sprintf("INSERT 0 %d", result.Count)
			} else {
				commandTag = fmt.Sprintf("INSERT %d %d", result.LastInsertID, result.Count)
			}
		} else if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "UPDATE") {
			commandTag = fmt.Sprintf("UPDATE %d", result.Count)
		} else if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "DELETE") {
			commandTag = fmt.Sprintf("DELETE %d", result.Count)
		} else if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "CREATE") ||
			strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "DROP") ||
			strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "ALTER") {
			commandTag = "DDL"
		} else {
			commandTag = fmt.Sprintf("SELECT %d", result.Count)
		}
		logger.Debug("Sending command complete", "command_tag", commandTag, "affected_rows", result.Count)
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	}
	
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush query response", "error", err)
		return true
	}
	
	return false
}

// sendErrorAndReady sends an error message followed by ReadyForQuery
func (s *PostgreSQLServer) sendErrorAndReady(backend *pgproto3.Backend, code, message string) {
	logger.Debug("Sending error response", "code", code, "message", message)
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush error response", "error", err)
	}
}

// buildReturningResult constructs a result set for RETURNING clauses
func (s *PostgreSQLServer) buildReturningResult(baseResult *types.ResultSet, returningColumns []string) *types.ResultSet {
	logger.Debug("Building RETURNING result", "returning_columns", returningColumns, "base_row_count", baseResult.Count)
	// For now, we'll just return the base result
	// In a full implementation, we would construct the result based on the RETURNING columns
	// We need to convert from sql.ResultSet to types.ResultSet
	result := &types.ResultSet{
		Columns:      baseResult.Columns,
		Rows:         baseResult.Rows,
		Count:        baseResult.Count,
		LastInsertID: baseResult.LastInsertID,
	}
	logger.Debug("RETURNING result built successfully", "result_row_count", result.Count)
	return result
}

// sendReturningResult sends a result set for RETURNING clauses
func (s *PostgreSQLServer) sendReturningResult(backend *pgproto3.Backend, result *types.ResultSet) {
	logger.Debug("Sending RETURNING result", "column_count", len(result.Columns), "row_count", result.Count)
	if len(result.Columns) > 0 {
		fields := make([]pgproto3.FieldDescription, len(result.Columns))
		for i, col := range result.Columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(col),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          705, // UNKNOWN type
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0, // Text format
			}
		}
		
		backend.Send(&pgproto3.RowDescription{Fields: fields})
		
		for i, row := range result.Rows {
			// Convert row values to strings
			values := make([][]byte, len(row))
			for j, val := range row {
				if val == nil {
					values[j] = nil
				} else {
					values[j] = []byte(fmt.Sprintf("%v", val))
				}
			}
			backend.Send(&pgproto3.DataRow{Values: values})
			if i%1000 == 0 && i > 0 {
				logger.Debug("Sent RETURNING data rows", "sent_rows", i, "total_rows", len(result.Rows))
			}
		}
		
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("INSERT 0 %d", result.Count))})
	} else {
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("INSERT 0 %d", result.Count))})
	}
}

// handleConnection handles a new client connection
func (s *PostgreSQLServer) handleConnection(conn net.Conn) {
	logger.Info("Handling new client connection", "remote_addr", conn.RemoteAddr().String(), "local_addr", conn.LocalAddr().String())
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
			shouldClose = s.handleQuery(backend, msg.String)
		case *pgproto3.Parse:
			logger.Debug("Handling Parse message", "name", msg.Name, "query", msg.Query, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			shouldClose = s.handleParse(backend, msg)
		case *pgproto3.Bind:
			logger.Debug("Handling Bind message", "destinationPortal", msg.DestinationPortal, "preparedStatement", msg.PreparedStatement, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			shouldClose = s.handleBind(backend, msg)
		case *pgproto3.Describe:
			logger.Debug("Handling Describe message", "objectType", string(msg.ObjectType), "name", msg.Name, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			shouldClose = s.handleDescribe(backend, msg)
		case *pgproto3.Execute:
			logger.Debug("Handling Execute message", "portal", msg.Portal, "maxRows", msg.MaxRows, "message_count", messageCount, "remote_addr", conn.RemoteAddr().String())
			shouldClose = s.handleExecute(backend, msg)
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
func (s *PostgreSQLServer) handleParse(backend *pgproto3.Backend, msg *pgproto3.Parse) bool {
	logger.Debug("Parsing prepared statement", "name", msg.Name, "query", msg.Query, "parameter_count", len(msg.ParameterOIDs))
	
	// Create a prepared statement
	stmt := &PreparedStatement{
		Name:  msg.Name,
		Query: msg.Query,
		ParameterOIDs: msg.ParameterOIDs,
	}
	
	// Parse the query to extract RETURNING columns if present
	startTime := time.Now()
	parsed, err := s.parser.Parse(msg.Query)
	parseDuration := time.Since(startTime)
	if err == nil {
		stmt.ReturningColumns = parsed.ReturningColumns
		logger.Debug("Query parsed for prepared statement", "parse_duration", parseDuration.String(), "returning_columns", parsed.ReturningColumns)
	} else {
		logger.Warn("Failed to parse query for prepared statement", "error", err, "parse_duration", parseDuration.String())
	}
	
	// Store the prepared statement
	if msg.Name == "" {
		// unnamed statement
		s.preparedStatements[""] = stmt
		logger.Debug("Stored unnamed prepared statement")
	} else {
		s.preparedStatements[msg.Name] = stmt
		logger.Debug("Stored named prepared statement", "name", msg.Name)
	}
	
	backend.Send(&pgproto3.ParseComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush ParseComplete", "error", err)
		return true
	}
	
	logger.Debug("Parse completed successfully")
	return false
}

func (s *PostgreSQLServer) handleBind(backend *pgproto3.Backend, msg *pgproto3.Bind) bool {
	logger.Debug("Binding portal", "destination_portal", msg.DestinationPortal, "prepared_statement", msg.PreparedStatement, "parameter_count", len(msg.Parameters))
	
	// Look up the prepared statement
	stmt, exists := s.preparedStatements[msg.PreparedStatement]
	if !exists {
		logger.Warn("Prepared statement not found", "prepared_statement", msg.PreparedStatement)
		s.sendErrorAndReady(backend, "26000", "prepared statement does not exist")
		return false
	}
	
	// Create a portal
	portal := &Portal{
		Name:         msg.DestinationPortal,
		Statement:    stmt,
		Params:       make([]interface{}, len(msg.Parameters)),
		ParamFormats: msg.ParameterFormatCodes,
	}
	
	// Convert parameters
	for i, param := range msg.Parameters {
		if param == nil {
			portal.Params[i] = nil
		} else {
			// For now, treat all parameters as strings
			// In a full implementation, we would convert based on the parameter OID
			portal.Params[i] = string(param)
		}
	}
	
	// Store the portal
	if msg.DestinationPortal == "" {
		// unnamed portal
		s.portals[""] = portal
		logger.Debug("Stored unnamed portal")
	} else {
		s.portals[msg.DestinationPortal] = portal
		logger.Debug("Stored named portal", "name", msg.DestinationPortal)
	}
	
	backend.Send(&pgproto3.BindComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush BindComplete", "error", err)
		return true
	}
	
	logger.Debug("Bind completed successfully")
	return false
}

func (s *PostgreSQLServer) handleDescribe(backend *pgproto3.Backend, msg *pgproto3.Describe) bool {
	logger.Debug("Describing object", "object_type", string(msg.ObjectType), "name", msg.Name)
	
	// For now, we'll just send an empty RowDescription
	// In a full implementation, we would describe the prepared statement or portal
	backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush RowDescription", "error", err)
		return true
	}
	
	logger.Debug("Describe completed successfully")
	return false
}

func (s *PostgreSQLServer) handleExecute(backend *pgproto3.Backend, msg *pgproto3.Execute) bool {
	logger.Debug("Executing portal", "portal", msg.Portal, "max_rows", msg.MaxRows)
	
	// Look up the portal
	portal, exists := s.portals[msg.Portal]
	if !exists {
		logger.Warn("Portal not found", "portal", msg.Portal)
		s.sendErrorAndReady(backend, "26000", "portal does not exist")
		return false
	}
	
	// For now, we'll just execute the query as-is
	// In a full implementation, we would substitute the parameters
	ctx := context.Background()
	startTime := time.Now()
	result, err := s.planner.Execute(ctx, portal.Statement.Query)
	executeDuration := time.Since(startTime)
	if err != nil {
		logger.Warn("Portal execution failed", "error", err, "query", portal.Statement.Query, "execute_duration", executeDuration.String())
		s.sendErrorAndReady(backend, "42000", fmt.Sprintf("Query execution failed: %v", err))
		return false
	}
	logger.Debug("Portal executed successfully", "execute_duration", executeDuration.String(), "row_count", result.Count)
	
	// Handle RETURNING clause for INSERT/UPDATE/DELETE
	if len(portal.Statement.ReturningColumns) > 0 {
		logger.Debug("Processing RETURNING clause for portal", "returning_columns", portal.Statement.ReturningColumns)
		returningResult := s.buildReturningResult(result, portal.Statement.ReturningColumns)
		s.sendReturningResult(backend, returningResult)
		if err := backend.Flush(); err != nil {
			logger.Error("Failed to flush RETURNING result", "error", err)
			return true
		}
		return false
	}
	
	if len(result.Columns) > 0 {
		logger.Debug("Sending result set for portal", "column_count", len(result.Columns), "row_count", result.Count)
		fields := make([]pgproto3.FieldDescription, len(result.Columns))
		for i, col := range result.Columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(col),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          705, // UNKNOWN type
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0, // Text format
			}
		}
		
		backend.Send(&pgproto3.RowDescription{Fields: fields})
		
		for i, row := range result.Rows {
			// Convert row values to strings
			values := make([][]byte, len(row))
			for j, val := range row {
				if val == nil {
					values[j] = nil
				} else {
					values[j] = []byte(fmt.Sprintf("%v", val))
				}
			}
			backend.Send(&pgproto3.DataRow{Values: values})
			if i%1000 == 0 && i > 0 {
				logger.Debug("Sent data rows for portal", "sent_rows", i, "total_rows", len(result.Rows))
			}
		}
		
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", result.Count))})
	} else {
		// For INSERT/UPDATE/DELETE operations without RETURNING
		var commandTag string
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(portal.Statement.Query)), "INSERT") {
			if result.LastInsertID > 0 {
				commandTag = fmt.Sprintf("INSERT 0 %d", result.Count)
			} else {
				commandTag = fmt.Sprintf("INSERT %d %d", result.LastInsertID, result.Count)
			}
		} else if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(portal.Statement.Query)), "UPDATE") {
			commandTag = fmt.Sprintf("UPDATE %d", result.Count)
		} else if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(portal.Statement.Query)), "DELETE") {
			commandTag = fmt.Sprintf("DELETE %d", result.Count)
		} else {
			commandTag = fmt.Sprintf("SELECT %d", result.Count)
		}
		logger.Debug("Sending command complete for portal", "command_tag", commandTag, "affected_rows", result.Count)
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	}
	
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush execute response", "error", err)
		return true
	}
	
	logger.Debug("Execute completed successfully")
	return false
}