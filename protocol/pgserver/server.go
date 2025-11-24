package pgserver

import (
	"fmt"
	"log"
	"net"
	"sync"

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
	Name    string
	Query   string
	SQLStmt *sql.ParsedQuery
}

// Portal represents a bound statement with parameters
type Portal struct {
	Name      string
	Statement *PreparedStatement
	Params    []string
}

func NewPostgreSQLServer(executor executor.QueryExecutor) *PostgreSQLServer {
	parser := sql.NewMySQLParser()
	planner := sql.NewPlanner(parser)

	return &PostgreSQLServer{
		executor: executor,
		parser:   parser,
		planner:  planner,
		preparedStatements: make(map[string]*PreparedStatement),
		portals:           make(map[string]*Portal),
	}
}

func (s *PostgreSQLServer) Start(port string) error {
	var err error
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	log.Printf("PostgreSQL server listening on port %s", port)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()

			if closed {
				return nil
			}
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *PostgreSQLServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Initialize PostgreSQL protocol
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

		// Send ready for query
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			log.Printf("Failed to send ReadyForQuery: %v", err)
			return
		}
	default:
		log.Printf("Unsupported startup message type: %T", startupMessage)
		return
	}

	// Handle queries
	for {
		msg, err := backend.Receive()
		if err != nil {
			log.Printf("Failed to receive message: %v", err)
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			if err := s.handleQuery(backend, msg.String); err != nil {
				log.Printf("Failed to handle query: %v", err)
				return
			}
		case *pgproto3.Parse:
			if err := s.handleParse(backend, msg); err != nil {
				log.Printf("Failed to handle parse: %v", err)
				return
			}
		case *pgproto3.Bind:
			if err := s.handleBind(backend, msg); err != nil {
				log.Printf("Failed to handle bind: %v", err)
				return
			}
		case *pgproto3.Describe:
			if err := s.handleDescribe(backend, msg); err != nil {
				log.Printf("Failed to handle describe: %v", err)
				return
			}
		case *pgproto3.Execute:
			if err := s.handleExecute(backend, msg); err != nil {
				log.Printf("Failed to handle execute: %v", err)
				return
			}
		case *pgproto3.Close:
			if err := s.handleClose(backend, msg); err != nil {
				log.Printf("Failed to handle close: %v", err)
				return
			}
		case *pgproto3.Sync:
			if err := s.handleSync(backend); err != nil {
				log.Printf("Failed to handle sync: %v", err)
				return
			}
		case *pgproto3.Terminate:
			return
		default:
			log.Printf("Unsupported message type: %T", msg)
			// Don't return here, just send an error response and continue
			backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "0A000", // Feature not supported
				Message:  fmt.Sprintf("Unsupported message type: %T", msg),
			})
			if err := backend.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *PostgreSQLServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	if s.listener != nil {
		return s.listener.Close()
	}

	return nil
}

// handleQuery handles the Query message (simple query protocol)
func (s *PostgreSQLServer) handleQuery(backend *pgproto3.Backend, query string) error {
	// Parse and execute the query
	// This is a simplified implementation
	// In a real implementation, we would:
	// 1. Parse the SQL query
	// 2. Create an execution plan
	// 3. Execute the plan
	// 4. Return results in PostgreSQL format

	// For now, we'll just send a simple response
	backend.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("result"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          705, // UNKNOWN
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
	})
	if err := backend.Flush(); err != nil {
		return err
	}

	// Send a dummy row
	backend.Send(&pgproto3.DataRow{
		Values: [][]byte{[]byte("Query executed: " + query)},
	})
	if err := backend.Flush(); err != nil {
		return err
	}

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT 1"),
	})
	if err := backend.Flush(); err != nil {
		return err
	}

	// Send ready for query
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return backend.Flush()
}

// handleParse handles the Parse message
func (s *PostgreSQLServer) handleParse(backend *pgproto3.Backend, msg *pgproto3.Parse) error {
	// Parse the SQL statement
	sqlStmt, err := s.parser.Parse(msg.Query)
	if err != nil {
		backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "42601", // Syntax error
			Message:  fmt.Sprintf("Syntax error: %v", err),
		})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return backend.Flush()
	}

	// Store the prepared statement
	stmt := &PreparedStatement{
		Name:    msg.Name,
		Query:   msg.Query,
		SQLStmt: sqlStmt,
	}
	
	if msg.Name == "" {
		// unnamed statement
		s.preparedStatements[""] = stmt
	} else {
		s.preparedStatements[msg.Name] = stmt
	}

	backend.Send(&pgproto3.ParseComplete{})
	return backend.Flush()
}

// handleBind handles the Bind message
func (s *PostgreSQLServer) handleBind(backend *pgproto3.Backend, msg *pgproto3.Bind) error {
	// Find the prepared statement
	stmt, exists := s.preparedStatements[msg.PreparedStatement]
	if !exists {
		backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "26000", // Invalid SQL statement name
			Message:  "Prepared statement not found",
		})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return backend.Flush()
	}

	// Create a portal
	portal := &Portal{
		Name:      msg.DestinationPortal,
		Statement: stmt,
		Params:    make([]string, len(msg.Parameters)),
	}
	
	// Convert parameters to strings (simplified)
	for i, param := range msg.Parameters {
		portal.Params[i] = string(param)
	}

	// Store the portal
	if msg.DestinationPortal == "" {
		// unnamed portal
		s.portals[""] = portal
	} else {
		s.portals[msg.DestinationPortal] = portal
	}

	backend.Send(&pgproto3.BindComplete{})
	return backend.Flush()
}

// handleDescribe handles the Describe message
func (s *PostgreSQLServer) handleDescribe(backend *pgproto3.Backend, msg *pgproto3.Describe) error {
	switch msg.ObjectType {
	case 'S': // Prepared statement
		_, exists := s.preparedStatements[msg.Name]
		if !exists {
			backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "26000", // Invalid SQL statement name
				Message:  "Prepared statement not found",
			})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return backend.Flush()
		}
		
		// Send parameter description
		backend.Send(&pgproto3.ParameterDescription{
			ParameterOIDs: make([]uint32, 0), // No parameters for now
		})
		
		// Send row description (simplified)
		backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("result"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          705, // UNKNOWN
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			},
		})
		
	case 'P': // Portal
		_, exists := s.portals[msg.Name]
		if !exists {
			backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "26000", // Invalid cursor name
				Message:  "Portal not found",
			})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return backend.Flush()
		}
		
		// Send row description (simplified)
		backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("result"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          705, // UNKNOWN
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			},
		})
	}
	
	return backend.Flush()
}

// handleExecute handles the Execute message
func (s *PostgreSQLServer) handleExecute(backend *pgproto3.Backend, msg *pgproto3.Execute) error {
	// Find the portal
	portal, exists := s.portals[msg.Portal]
	if !exists {
		backend.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "26000", // Invalid cursor name
			Message:  "Portal not found",
		})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		return backend.Flush()
	}

	// Execute the query (simplified implementation)
	query := portal.Statement.Query
	
	// Send row description
	backend.Send(&pgproto3.RowDescription{
		Fields: []pgproto3.FieldDescription{
			{
				Name:                 []byte("result"),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          705, // UNKNOWN
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			},
		},
	})
	
	// Send a dummy row
	backend.Send(&pgproto3.DataRow{
		Values: [][]byte{[]byte("Query executed: " + query)},
	})
	
	// Send command complete
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT 1"),
	})
	
	return backend.Flush()
}

// handleClose handles the Close message
func (s *PostgreSQLServer) handleClose(backend *pgproto3.Backend, msg *pgproto3.Close) error {
	switch msg.ObjectType {
	case 'S': // Prepared statement
		delete(s.preparedStatements, msg.Name)
	case 'P': // Portal
		delete(s.portals, msg.Name)
	}
	
	backend.Send(&pgproto3.CloseComplete{})
	return backend.Flush()
}

// handleSync handles the Sync message
func (s *PostgreSQLServer) handleSync(backend *pgproto3.Backend) error {
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return backend.Flush()
}
