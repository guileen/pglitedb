package pgserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
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

		// Send ParameterStatus messages
		backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0 (PGLiteDB)"})
		backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
		backend.Send(&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"})
		backend.Send(&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"})
		backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
		backend.Send(&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"})
		
		// Send BackendKeyData
		backend.Send(&pgproto3.BackendKeyData{ProcessID: 12345, SecretKey: 67890})
		
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
			if fatal := s.handleQuery(backend, msg.String); fatal {
				return
			}
		case *pgproto3.Parse:
			if fatal := s.handleParse(backend, msg); fatal {
				return
			}
		case *pgproto3.Bind:
			if fatal := s.handleBind(backend, msg); fatal {
				return
			}
		case *pgproto3.Describe:
			if fatal := s.handleDescribe(backend, msg); fatal {
				return
			}
		case *pgproto3.Execute:
			if fatal := s.handleExecute(backend, msg); fatal {
				return
			}
		case *pgproto3.Close:
			if fatal := s.handleClose(backend, msg); fatal {
				return
			}
		case *pgproto3.Sync:
			if fatal := s.handleSync(backend); fatal {
				return
			}
		case *pgproto3.Terminate:
			return
		default:
			log.Printf("Unsupported message type: %T", msg)
			backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "0A000",
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
func (s *PostgreSQLServer) handleQuery(backend *pgproto3.Backend, query string) bool {
	ctx := context.Background()
	
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
	
	if len(result.Columns) > 0 {
		fields := make([]pgproto3.FieldDescription, len(result.Columns))
		for i, col := range result.Columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(col),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			}
		}
		backend.Send(&pgproto3.RowDescription{Fields: fields})
	}
	
	for _, row := range result.Rows {
		dataRow := &pgproto3.DataRow{Values: make([][]byte, len(row))}
		for i, val := range row {
			if val == nil {
				dataRow.Values[i] = nil
			} else {
				dataRow.Values[i] = []byte(fmt.Sprintf("%v", val))
			}
		}
		backend.Send(dataRow)
	}
	
	var commandTag string
	if len(result.Rows) > 0 {
		commandTag = fmt.Sprintf("SELECT %d", result.Count)
	} else {
		commandTag = "SELECT 0"
	}
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte(commandTag),
	})
	
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return true
	}
	return false
}

// handleParse handles the Parse message
func (s *PostgreSQLServer) handleParse(backend *pgproto3.Backend, msg *pgproto3.Parse) bool {
	mysqlParser, ok := s.parser.(*sql.MySQLParser)
	if !ok {
		s.sendErrorAndReady(backend, "XX000", "Internal error: parser type mismatch")
		return false
	}
	
	returningCols := mysqlParser.ExtractReturningColumns(msg.Query)
	preprocessedSQL := mysqlParser.PreprocessPostgreSQLDDL(msg.Query)
	
	stmt := &PreparedStatement{
		Name:             msg.Name,
		Query:            msg.Query,
		PreprocessedSQL:  preprocessedSQL,
		ParameterOIDs:    msg.ParameterOIDs,
		ReturningColumns: returningCols,
	}
	
	stmtName := msg.Name
	if stmtName == "" {
		stmtName = ""
	}
	s.preparedStatements[stmtName] = stmt

	backend.Send(&pgproto3.ParseComplete{})
	if err := backend.Flush(); err != nil {
		return true
	}
	return false
}

// handleBind handles the Bind message
func (s *PostgreSQLServer) handleBind(backend *pgproto3.Backend, msg *pgproto3.Bind) bool {
	stmtName := msg.PreparedStatement
	if stmtName == "" {
		stmtName = ""
	}
	stmt, exists := s.preparedStatements[stmtName]
	if !exists {
		s.sendErrorAndReady(backend, "26000", 
			fmt.Sprintf("Prepared statement not found: '%s' (available: %d statements)", 
				stmtName, len(s.preparedStatements)))
		return false
	}

	params := make([]interface{}, len(msg.Parameters))
	for i, paramBytes := range msg.Parameters {
		if paramBytes == nil {
			params[i] = nil
			continue
		}

		paramFormat := int16(0)
		if i < len(msg.ParameterFormatCodes) {
			paramFormat = msg.ParameterFormatCodes[i]
		}

		paramOID := uint32(0)
		if i < len(stmt.ParameterOIDs) {
			paramOID = stmt.ParameterOIDs[i]
		}

		if paramFormat == 0 {
			params[i] = s.parseTextParameter(paramBytes, paramOID)
		} else {
			params[i] = paramBytes
		}
	}

	portal := &Portal{
		Name:         msg.DestinationPortal,
		Statement:    stmt,
		Params:       params,
		ParamFormats: msg.ParameterFormatCodes,
	}

	portalName := msg.DestinationPortal
	if portalName == "" {
		portalName = ""
	}
	s.portals[portalName] = portal

	backend.Send(&pgproto3.BindComplete{})
	if err := backend.Flush(); err != nil {
		return true
	}
	return false
}

// handleDescribe handles the Describe message
func (s *PostgreSQLServer) handleDescribe(backend *pgproto3.Backend, msg *pgproto3.Describe) bool {
	switch msg.ObjectType {
	case 'S':
		_, exists := s.preparedStatements[msg.Name]
		if !exists {
			s.sendErrorAndReady(backend, "26000", "Prepared statement not found")
			return false
		}
		
		backend.Send(&pgproto3.ParameterDescription{
			ParameterOIDs: make([]uint32, 0),
		})
		
		backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("result"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          25,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			},
		})
		
	case 'P':
		_, exists := s.portals[msg.Name]
		if !exists {
			s.sendErrorAndReady(backend, "26000", "Portal not found")
			return false
		}
		
		backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("result"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          25,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			},
		})
	}
	
	if err := backend.Flush(); err != nil {
		return true
	}
	return false
}

// handleExecute handles the Execute message
func (s *PostgreSQLServer) handleExecute(backend *pgproto3.Backend, msg *pgproto3.Execute) bool {
	portalName := msg.Portal
	if portalName == "" {
		portalName = ""
	}
	portal, exists := s.portals[portalName]
	if !exists {
		s.sendErrorAndReady(backend, "26000", 
			fmt.Sprintf("Portal not found: '%s' (available: %d portals)", 
				portalName, len(s.portals)))
		return false
	}

	query := s.replacePostgreSQLPlaceholders(portal.Statement.PreprocessedSQL, portal.Params)
	
	ctx := context.Background()
	result, err := s.planner.Execute(ctx, query)
	if err != nil {
		s.sendErrorAndReady(backend, "42000", 
			fmt.Sprintf("Query execution failed: %v", err))
		return false
	}
	
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
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			}
		}
		backend.Send(&pgproto3.RowDescription{Fields: fields})
	}
	
	for _, row := range result.Rows {
		dataRow := &pgproto3.DataRow{Values: make([][]byte, len(row))}
		for i, val := range row {
			if val == nil {
				dataRow.Values[i] = nil
			} else {
				dataRow.Values[i] = []byte(fmt.Sprintf("%v", val))
			}
		}
		backend.Send(dataRow)
	}
	
	var commandTag string
	if len(result.Rows) > 0 {
		commandTag = fmt.Sprintf("SELECT %d", result.Count)
	} else {
		commandTag = "SELECT 0"
	}
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte(commandTag),
	})
	
	if err := backend.Flush(); err != nil {
		return true
	}
	return false
}

// handleClose handles the Close message
func (s *PostgreSQLServer) handleClose(backend *pgproto3.Backend, msg *pgproto3.Close) bool {
	switch msg.ObjectType {
	case 'S':
		delete(s.preparedStatements, msg.Name)
	case 'P':
		delete(s.portals, msg.Name)
	}
	
	backend.Send(&pgproto3.CloseComplete{})
	if err := backend.Flush(); err != nil {
		return true
	}
	return false
}

func (s *PostgreSQLServer) handleSync(backend *pgproto3.Backend) bool {
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return true
	}
	return false
}

func (s *PostgreSQLServer) sendErrorAndReady(backend *pgproto3.Backend, code, message string) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	backend.Flush()
}

// replacePlaceholders replaces $1, $2, ... with actual parameter values
func (s *PostgreSQLServer) replacePlaceholders(query string, params []interface{}) string {
	re := regexp.MustCompile(`\$(\d+)`)
	result := re.ReplaceAllStringFunc(query, func(match string) string {
		numStr := strings.TrimPrefix(match, "$")
		num, err := strconv.Atoi(numStr)
		if err != nil || num < 1 || num > len(params) {
			return match
		}
		
		param := params[num-1]
		
		switch v := param.(type) {
		case nil:
			return "NULL"
		case bool:
			if v {
				return "1"
			}
			return "0"
		case int, int8, int16, int32, int64:
			return fmt.Sprintf("%d", v)
		case uint, uint8, uint16, uint32, uint64:
			return fmt.Sprintf("%d", v)
		case float32, float64:
			return fmt.Sprintf("%f", v)
		case string:
			escaped := strings.ReplaceAll(v, "'", "''")
			escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
			return fmt.Sprintf("'%s'", escaped)
		case []byte:
			escaped := strings.ReplaceAll(string(v), "'", "''")
			escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
			return fmt.Sprintf("'%s'", escaped)
		default:
			str := fmt.Sprintf("%v", v)
			escaped := strings.ReplaceAll(str, "'", "''")
			escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
			return fmt.Sprintf("'%s'", escaped)
		}
	})
	return result
}

func (s *PostgreSQLServer) replacePostgreSQLPlaceholders(query string, params []interface{}) string {
	return s.replacePlaceholders(query, params)
}

func (s *PostgreSQLServer) parseTextParameter(data []byte, oid uint32) interface{} {
	text := string(data)
	
	switch oid {
	case 16:
		return text == "t" || text == "true" || text == "1"
	case 20:
		if val, err := strconv.ParseInt(text, 10, 64); err == nil {
			return val
		}
	case 21:
		if val, err := strconv.ParseInt(text, 10, 16); err == nil {
			return int16(val)
		}
	case 23:
		if val, err := strconv.ParseInt(text, 10, 32); err == nil {
			return int32(val)
		}
	case 700:
		if val, err := strconv.ParseFloat(text, 32); err == nil {
			return float32(val)
		}
	case 701:
		if val, err := strconv.ParseFloat(text, 64); err == nil {
			return val
		}
	case 1043, 25:
		return text
	}
	
	if oid == 0 {
		if text == "t" || text == "true" || text == "f" || text == "false" {
			return text == "t" || text == "true"
		}
		if val, err := strconv.ParseInt(text, 10, 64); err == nil {
			return val
		}
		if val, err := strconv.ParseFloat(text, 64); err == nil {
			return val
		}
	}
	
	return text
}

func (s *PostgreSQLServer) buildReturningResult(result *sql.ResultSet, returningCols []string) *sql.ResultSet {
	if result.LastInsertID == 0 {
		return &sql.ResultSet{
			Columns: returningCols,
			Rows:    [][]interface{}{},
			Count:   0,
		}
	}
	
	returningResult := &sql.ResultSet{
		Columns:      returningCols,
		Rows:         make([][]interface{}, 1),
		Count:        1,
		LastInsertID: result.LastInsertID,
	}
	
	row := make([]interface{}, len(returningCols))
	for i, col := range returningCols {
		if col == "id" || col == "*" {
			row[i] = result.LastInsertID
		} else {
			row[i] = nil
		}
	}
	returningResult.Rows[0] = row
	
	return returningResult
}

func (s *PostgreSQLServer) sendReturningResult(backend *pgproto3.Backend, result *sql.ResultSet) {
	fields := make([]pgproto3.FieldDescription, len(result.Columns))
	for i, col := range result.Columns {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(col),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          23,
			DataTypeSize:         4,
			TypeModifier:         -1,
			Format:               0,
		}
	}
	backend.Send(&pgproto3.RowDescription{Fields: fields})
	
	for _, row := range result.Rows {
		dataRow := &pgproto3.DataRow{Values: make([][]byte, len(row))}
		for i, val := range row {
			if val == nil {
				dataRow.Values[i] = nil
			} else {
				dataRow.Values[i] = []byte(fmt.Sprintf("%v", val))
			}
		}
		backend.Send(dataRow)
	}
	
	backend.Send(&pgproto3.CommandComplete{
		CommandTag: []byte(fmt.Sprintf("INSERT 0 %d", result.Count)),
	})
}
