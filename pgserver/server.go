package pgserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/guileen/pqlitedb/executor"
	"github.com/guileen/pqlitedb/manager"
	"github.com/guileen/pqlitedb/engine"
	"github.com/guileen/pqlitedb/sql"
	"github.com/jackc/pgx/v5/pgproto3"
)

type PostgreSQLServer struct {
	listener net.Listener
	executor executor.QueryExecutor
	parser   sql.Parser
	planner  *sql.Planner
	mu       sync.Mutex
	closed   bool
}

func NewPostgreSQLServer(executor executor.QueryExecutor) *PostgreSQLServer {
	parser := sql.NewMySQLParser()
	planner := sql.NewPlanner(parser)
	
	return &PostgreSQLServer{
		executor: executor,
		parser:   parser,
		planner:  planner,
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
		if err := backend.Send(&pgproto3.AuthenticationOk{}); err != nil {
			log.Printf("Failed to send AuthenticationOk: %v", err)
			return
		}

		// Send ready for query
		if err := backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}); err != nil {
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
		case *pgproto3.Terminate:
			return
		default:
			log.Printf("Unsupported message type: %T", msg)
			return
		}
	}
}

func (s *PostgreSQLServer) handleQuery(backend *pgproto3.Backend, query string) error {
	// Parse and execute the query
	// This is a simplified implementation
	// In a real implementation, we would:
	// 1. Parse the SQL query
	// 2. Create an execution plan
	// 3. Execute the plan
	// 4. Return results in PostgreSQL format
	
	// For now, we'll just send a simple response
	if err := backend.Send(&pgproto3.RowDescription{
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
	}); err != nil {
		return err
	}

	// Send a dummy row
	if err := backend.Send(&pgproto3.DataRow{
		Values: [][]byte{[]byte("Query executed: " + query)},
	}); err != nil {
		return err
	}

	// Send command complete
	if err := backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte("SELECT 1"),
	}); err != nil {
		return err
	}

	// Send ready for query
	return backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
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

