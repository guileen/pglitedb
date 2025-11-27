package pgserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/protocol/executor"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/jackc/pgx/v5/pgproto3"
)

type PostgreSQLServer struct {
	listener net.Listener
	executor executor.QueryExecutor
	parser   sql.Parser
	planner  *sql.Planner
	mu       sync.Mutex
	closed   bool
	
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

func NewPostgreSQLServer(executor executor.QueryExecutor) *PostgreSQLServer {
	parser := sql.NewPGParser()
	planner := sql.NewPlanner(parser)
	// Set catalog for the planner's executor if possible
	if catalogProvider, ok := executor.(interface{ GetCatalog() catalog.Manager }); ok {
		planner.SetCatalog(catalogProvider.GetCatalog())
	}
	
	return &PostgreSQLServer{
		executor: executor,
		parser:   parser,
		planner:  planner,
		preparedStatements: make(map[string]*PreparedStatement),
		portals:           make(map[string]*Portal),
	}
}

func (s *PostgreSQLServer) Start(port string) error {
	return s.StartTCP(port)
}

func (s *PostgreSQLServer) StartTCP(port string) error {
	var err error
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}
	
	log.Printf("PostgreSQL server listening on TCP port %s", port)
	
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			
			if closed {
				return nil
			}
			return fmt.Errorf("failed to accept TCP connection: %w", err)
		}
		
		go s.handleConnection(conn)
	}
}

func (s *PostgreSQLServer) StartUnix(socketPath string) error {
	// Remove existing socket file if it exists
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove existing socket file: %v", err)
	}
	
	var err error
	s.listener, err = net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to start Unix socket listener: %w", err)
	}
	
	log.Printf("PostgreSQL server listening on Unix socket %s", socketPath)
	
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			
			if closed {
				return nil
			}
			return fmt.Errorf("failed to accept Unix connection: %w", err)
		}
		
		go s.handleConnection(conn)
	}
}

func (s *PostgreSQLServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.closed = true
	if s.listener != nil {
		return s.listener.Close()
	}
	
	return nil
}

// handleQuery handles the Query message (simple query protocol)
func (s *PostgreSQLServer) handleQuery(backend *pgproto3.Backend, query string) bool {
	ctx := context.Background()
	
	// Handle empty query
	if strings.TrimSpace(query) == "" {
		backend.Send(&pgproto3.EmptyQueryResponse{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			return true
		}
		return false
	}
	
	parsed, err := s.parser.Parse(query)
	if err != nil {
		s.sendErrorAndReady(backend, "42601", fmt.Sprintf("Syntax error: failed to parse SQL query: %v", err))
		return false
	}
	
	result, err := s.planner.Execute(ctx, parsed.Query)
	if err != nil {
		s.sendErrorAndReady(backend, "42000", fmt.Sprintf("Query execution failed: %v", err))
		return false
	}
	
	// Handle RETURNING clause for INSERT/UPDATE/DELETE
	if len(parsed.ReturningColumns) > 0 {
		returningResult := s.buildReturningResult(result, parsed.ReturningColumns)
		s.sendReturningResult(backend, returningResult)
		if err := backend.Flush(); err != nil {
			return true
		}
		return false
	}
	
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
		
		for _, row := range result.Rows {
			// Convert row values to strings
			values := make([][]byte, len(row))
			for i, val := range row {
				if val == nil {
					values[i] = nil
				} else {
					values[i] = []byte(fmt.Sprintf("%v", val))
				}
			}
			backend.Send(&pgproto3.DataRow{Values: values})
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
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	}
	
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return true
	}
	
	return false
}

// sendErrorAndReady sends an error message followed by ReadyForQuery
func (s *PostgreSQLServer) sendErrorAndReady(backend *pgproto3.Backend, code, message string) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		log.Printf("Failed to flush error response: %v", err)
	}
}

// buildReturningResult constructs a result set for RETURNING clauses
func (s *PostgreSQLServer) buildReturningResult(baseResult *sql.ResultSet, returningColumns []string) *sql.ResultSet {
	// For now, we'll just return the base result
	// In a full implementation, we would construct the result based on the RETURNING columns
	return baseResult
}

// sendReturningResult sends a result set for RETURNING clauses
func (s *PostgreSQLServer) sendReturningResult(backend *pgproto3.Backend, result *sql.ResultSet) {
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
		
		for _, row := range result.Rows {
			// Convert row values to strings
			values := make([][]byte, len(row))
			for i, val := range row {
				if val == nil {
					values[i] = nil
				} else {
					values[i] = []byte(fmt.Sprintf("%v", val))
				}
			}
			backend.Send(&pgproto3.DataRow{Values: values})
		}
		
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("INSERT 0 %d", result.Count))})
	} else {
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("INSERT 0 %d", result.Count))})
	}
}

// handleConnection handles a new client connection
func (s *PostgreSQLServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	
	backend := pgproto3.NewBackend(conn, conn)
	
	// Handle startup message
	startupMessage, err := backend.ReceiveStartupMessage()
	if err != nil {
		log.Printf("Failed to receive startup message: %v", err)
		return
	}
	
	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		// Send authentication OK
		backend.Send(&pgproto3.AuthenticationOk{})
		if err := backend.Flush(); err != nil {
			log.Printf("Failed to send AuthenticationOk: %v", err)
			return
		}
		
		// Send ParameterStatus messages
		backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0 (PGLiteDB)"})
		backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
		backend.Send(&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"})
		backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
		
		// Send ReadyForQuery
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			log.Printf("Failed to send ReadyForQuery: %v", err)
			return
		}
		
	default:
		log.Printf("Unsupported startup message type: %T", startupMessage)
		return
	}
	
	// Main message loop
	for {
		msg, err := backend.Receive()
		if err != nil {
			log.Printf("Failed to receive message: %v", err)
			return
		}
		
		shouldClose := false
		switch msg := msg.(type) {
		case *pgproto3.Query:
			shouldClose = s.handleQuery(backend, msg.String)
		case *pgproto3.Parse:
			shouldClose = s.handleParse(backend, msg)
		case *pgproto3.Bind:
			shouldClose = s.handleBind(backend, msg)
		case *pgproto3.Describe:
			shouldClose = s.handleDescribe(backend, msg)
		case *pgproto3.Execute:
			shouldClose = s.handleExecute(backend, msg)
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				shouldClose = true
			}
		case *pgproto3.Terminate:
			return
		default:
			log.Printf("Unsupported message type: %T", msg)
			shouldClose = true
		}
		
		if shouldClose {
			return
		}
	}
}

// Extended Query Protocol handlers
func (s *PostgreSQLServer) handleParse(backend *pgproto3.Backend, msg *pgproto3.Parse) bool {
	// Create a prepared statement
	stmt := &PreparedStatement{
		Name:  msg.Name,
		Query: msg.Query,
		ParameterOIDs: msg.ParameterOIDs,
	}
	
	// Parse the query to extract RETURNING columns if present
	parsed, err := s.parser.Parse(msg.Query)
	if err == nil {
		stmt.ReturningColumns = parsed.ReturningColumns
	}
	
	// Store the prepared statement
	if msg.Name == "" {
		// unnamed statement
		s.preparedStatements[""] = stmt
	} else {
		s.preparedStatements[msg.Name] = stmt
	}
	
	backend.Send(&pgproto3.ParseComplete{})
	if err := backend.Flush(); err != nil {
		return true
	}
	
	return false
}

func (s *PostgreSQLServer) handleBind(backend *pgproto3.Backend, msg *pgproto3.Bind) bool {
	// Look up the prepared statement
	stmt, exists := s.preparedStatements[msg.PreparedStatement]
	if !exists {
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
	} else {
		s.portals[msg.DestinationPortal] = portal
	}
	
	backend.Send(&pgproto3.BindComplete{})
	if err := backend.Flush(); err != nil {
		return true
	}
	
	return false
}

func (s *PostgreSQLServer) handleDescribe(backend *pgproto3.Backend, msg *pgproto3.Describe) bool {
	// For now, we'll just send an empty RowDescription
	// In a full implementation, we would describe the prepared statement or portal
	backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}})
	if err := backend.Flush(); err != nil {
		return true
	}
	
	return false
}

func (s *PostgreSQLServer) handleExecute(backend *pgproto3.Backend, msg *pgproto3.Execute) bool {
	// Look up the portal
	portal, exists := s.portals[msg.Portal]
	if !exists {
		s.sendErrorAndReady(backend, "26000", "portal does not exist")
		return false
	}
	
	// For now, we'll just execute the query as-is
	// In a full implementation, we would substitute the parameters
	ctx := context.Background()
	result, err := s.planner.Execute(ctx, portal.Statement.Query)
	if err != nil {
		s.sendErrorAndReady(backend, "42000", fmt.Sprintf("Query execution failed: %v", err))
		return false
	}
	
	// Handle RETURNING clause for INSERT/UPDATE/DELETE
	if len(portal.Statement.ReturningColumns) > 0 {
		returningResult := s.buildReturningResult(result, portal.Statement.ReturningColumns)
		s.sendReturningResult(backend, returningResult)
		if err := backend.Flush(); err != nil {
			return true
		}
		return false
	}
	
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
		
		for _, row := range result.Rows {
			// Convert row values to strings
			values := make([][]byte, len(row))
			for i, val := range row {
				if val == nil {
					values[i] = nil
				} else {
					values[i] = []byte(fmt.Sprintf("%v", val))
				}
			}
			backend.Send(&pgproto3.DataRow{Values: values})
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
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	}
	
	if err := backend.Flush(); err != nil {
		return true
	}
	
	return false
}