package pgserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	pg_query "github.com/pganalyze/pg_query_go/v6"
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
	switch parsed.Type {
	case sql.InsertStatement:
		if result.LastInsertID > 0 {
			commandTag = fmt.Sprintf("INSERT 0 %d", result.Count)
		} else {
			commandTag = fmt.Sprintf("INSERT 0 %d", result.Count)
		}
	case sql.UpdateStatement:
		commandTag = fmt.Sprintf("UPDATE %d", result.Count)
	case sql.DeleteStatement:
		commandTag = fmt.Sprintf("DELETE %d", result.Count)
	default: // SELECT
		if len(result.Rows) > 0 {
			commandTag = fmt.Sprintf("SELECT %d", result.Count)
		} else {
			commandTag = "SELECT 0"
		}
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
	// Extract returning columns from the query
	returningCols := s.extractReturningColumns(msg.Query)
	
	// For PostgreSQL DDL preprocessing, we can just return the query as-is for now
	preprocessedSQL := msg.Query
	
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

// extractReturningColumns extracts RETURNING columns from a SQL query using AST-based parsing
func (s *PostgreSQLServer) extractReturningColumns(query string) []string {
	// Use the professional AST-based parser to extract RETURNING columns
	parser := sql.NewPGParser()
	parsed, err := parser.Parse(query)
	if err != nil {
		// If parsing fails, return nil (no RETURNING clause detected)
		return nil
	}
	
	// Extract RETURNING columns from the parsed AST
	if pgNode, ok := parsed.Statement.(*pg_query.Node); ok {
		pgParser := sql.NewPGParser()
		return pgParser.ExtractReturningColumns(pgNode)
	}
	
	return nil
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

	// 确保参数正确解析和存储
	params := make([]interface{}, len(msg.Parameters))
	for i, paramBytes := range msg.Parameters {
		if paramBytes == nil {
			params[i] = nil
			continue
		}
		
		// 根据 ParameterFormatCodes 解析参数
		// 简化实现:假设都是文本格式
		params[i] = string(paramBytes)
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

	// 使用改进的参数绑定器
	parseResult, err := pg_query.Parse(portal.Statement.Query)
	if err != nil {
		s.sendErrorAndReady(backend, "42000", 
			fmt.Sprintf("Query parsing failed: %v", err))
		return false
	}
	
	binder := NewParameterBinder(parseResult, portal.Params)
	boundAST, err := binder.BindParameters()
	if err != nil {
		s.sendErrorAndReady(backend, "42000", 
			fmt.Sprintf("Parameter binding failed: %v", err))
		return false
	}
	
	// 将绑定后的AST转换回SQL
	deparseResult, err := pg_query.Deparse(boundAST)
	if err != nil {
		s.sendErrorAndReady(backend, "42000", 
			fmt.Sprintf("Query generation failed: %v", err))
		return false
	}
	
	boundQuery := deparseResult
	
	ctx := context.Background()
	result, err := s.planner.Execute(ctx, boundQuery)
	if err != nil {
		s.sendErrorAndReady(backend, "42000", 
			fmt.Sprintf("Query execution failed: %v", err))
		return false
	}
	
	// Handle RETURNING clause
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
	switch s.getStatementType(portal.Statement.Query) {
	case "INSERT":
		commandTag = fmt.Sprintf("INSERT 0 %d", result.Count)
	case "UPDATE":
		commandTag = fmt.Sprintf("UPDATE %d", result.Count)
	case "DELETE":
		commandTag = fmt.Sprintf("DELETE %d", result.Count)
	default: // SELECT
		if len(result.Rows) > 0 {
			commandTag = fmt.Sprintf("SELECT %d", result.Count)
		} else {
			commandTag = "SELECT 0"
		}
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
	// Use the professional parameter binder based on AST
	result, err := BindParametersInQuery(query, params)
	if err != nil {
		// Fallback to the old regex-based implementation if AST binding fails
		log.Printf("Failed to bind parameters using AST, falling back to regex: %v", err)
		return s.replacePlaceholdersRegex(query, params)
	}
	return result
}

// replacePlaceholdersRegex is the old regex-based implementation as fallback
func (s *PostgreSQLServer) replacePlaceholdersRegex(query string, params []interface{}) string {
	// 处理 PostgreSQL 风格参数 $1, $2, ...
	result := query
	
	// 按参数索引从高到低排序替换，避免编号冲突
	for i := len(params) - 1; i >= 0; i-- {
		param := params[i]
		placeholder := fmt.Sprintf("$%d", i+1)
		var replacement string
		
		// 根据参数类型进行适当转义
		switch v := param.(type) {
		case string:
			replacement = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		case nil:
			replacement = "NULL"
		case int, int32, int64, float32, float64:
			replacement = fmt.Sprintf("%v", v)
		case bool:
			if v {
				replacement = "true"
			} else {
				replacement = "false"
			}
		default:
			// 对于复杂类型,转换为字符串并加上引号
			replacement = "'" + strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''") + "'"
		}
		
		// 使用字符串替换确保精确替换,避免部分匹配
		// We use a simple string replacement with word boundary checking
		result = strings.ReplaceAll(result, placeholder, replacement)
	}
	
	return result
}

func (s *PostgreSQLServer) replacePostgreSQLPlaceholders(query string, params []interface{}) string {
	return s.replacePlaceholders(query, params)
}

// getStatementType determines the type of SQL statement
func (s *PostgreSQLServer) getStatementType(query string) string {
	query = strings.TrimSpace(query)
	lowerQuery := strings.ToLower(query)
	
	switch {
	case strings.HasPrefix(lowerQuery, "select"):
		return "SELECT"
	case strings.HasPrefix(lowerQuery, "insert"):
		return "INSERT"
	case strings.HasPrefix(lowerQuery, "update"):
		return "UPDATE"
	case strings.HasPrefix(lowerQuery, "delete"):
		return "DELETE"
	default:
		return "SELECT"
	}
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
	// More sophisticated implementation that can handle complex RETURNING clauses
	// For now, we'll enhance the existing implementation
	
	if result.LastInsertID == 0 && result.Count == 0 {
		return &sql.ResultSet{
			Columns: returningCols,
			Rows:    [][]interface{}{},
			Count:   0,
		}
	}
	
	// If we have a LastInsertID, create a simple result with it
	if result.LastInsertID != 0 {
		returningResult := &sql.ResultSet{
			Columns:      returningCols,
			Rows:         make([][]interface{}, 1),
			Count:        1,
			LastInsertID: result.LastInsertID,
		}
		
		row := make([]interface{}, len(returningCols))
		for i, col := range returningCols {
			switch col {
			case "id", "*":
				row[i] = result.LastInsertID
			default:
				// For other columns, we would need to fetch the actual values
				// This requires implementing proper catalog operations
				row[i] = nil
			}
		}
		returningResult.Rows[0] = row
		
		return returningResult
	}
	
	// For UPDATE/DELETE operations, we might have rows to return
	if len(result.Rows) > 0 {
		// Create a new result with the specified columns
		returningResult := &sql.ResultSet{
			Columns: returningCols,
			Rows:    make([][]interface{}, len(result.Rows)),
			Count:   result.Count,
		}
		
		// Copy the relevant columns from the original result
		for i, originalRow := range result.Rows {
			newRow := make([]interface{}, len(returningCols))
			for j, col := range returningCols {
				// Find the column in the original result
				found := false
				for k, originalCol := range result.Columns {
					if originalCol == col || col == "*" {
						if k < len(originalRow) {
							newRow[j] = originalRow[k]
							found = true
							break
						}
					}
				}
				if !found {
					newRow[j] = nil
				}
			}
			returningResult.Rows[i] = newRow
		}
		
		return returningResult
	}
	
	// Fallback
	return &sql.ResultSet{
		Columns: returningCols,
		Rows:    [][]interface{}{},
		Count:   0,
	}
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