package pgserver

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/jackc/pgx/v5/pgproto3"
)

// PreparedStatementManager manages prepared statements and portals
type PreparedStatementManager struct {
	parser sql.Parser
	
	// Extended query protocol state
	preparedStatements map[string]*PreparedStatement
	portals           map[string]*Portal
	mutex             *sync.RWMutex
}

// NewPreparedStatementManager creates a new prepared statement manager
func NewPreparedStatementManager(parser sql.Parser) *PreparedStatementManager {
	return &PreparedStatementManager{
		parser:            parser,
		preparedStatements: make(map[string]*PreparedStatement),
		portals:           make(map[string]*Portal),
		mutex:             &sync.RWMutex{},
	}
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

// Parse handles the Parse message
func (psm *PreparedStatementManager) Parse(backend *pgproto3.Backend, msg *pgproto3.Parse) bool {
	logger.Debug("Parsing prepared statement", "name", msg.Name, "query", msg.Query, "parameter_count", len(msg.ParameterOIDs))
	
	// Create a prepared statement
	stmt := &PreparedStatement{
		Name:  msg.Name,
		Query: msg.Query,
		ParameterOIDs: msg.ParameterOIDs,
	}
	
	// Parse the query to extract RETURNING columns if present
	startTime := time.Now()
	parsed, err := psm.parser.Parse(msg.Query)
	parseDuration := time.Since(startTime)
	if err == nil {
		stmt.ReturningColumns = parsed.ReturningColumns
		logger.Debug("Query parsed for prepared statement", "parse_duration", parseDuration.String(), "returning_columns", parsed.ReturningColumns)
	} else {
		logger.Warn("Failed to parse query for prepared statement", "error", err, "parse_duration", parseDuration.String())
	}
	
	// Store the prepared statement with mutex protection
	psm.mutex.Lock()
	if msg.Name == "" {
		// unnamed statement
		psm.preparedStatements[""] = stmt
		logger.Debug("Stored unnamed prepared statement")
	} else {
		psm.preparedStatements[msg.Name] = stmt
		logger.Debug("Stored named prepared statement", "name", msg.Name)
	}
	psm.mutex.Unlock()
	
	backend.Send(&pgproto3.ParseComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush ParseComplete", "error", err)
		return true
	}
	
	logger.Debug("Parse completed successfully")
	return false
}

// Bind handles the Bind message
func (psm *PreparedStatementManager) Bind(backend *pgproto3.Backend, msg *pgproto3.Bind) bool {
	logger.Debug("Binding portal", "destination_portal", msg.DestinationPortal, "prepared_statement", msg.PreparedStatement, "parameter_count", len(msg.Parameters))
	
	// Retrieve the prepared statement with mutex protection
	psm.mutex.RLock()
	stmt, exists := psm.preparedStatements[msg.PreparedStatement]
	psm.mutex.RUnlock()
	
	if !exists {
		logger.Warn("Prepared statement not found", "statement_name", msg.PreparedStatement)
		psm.sendErrorAndReady(backend, "26000", fmt.Sprintf("prepared statement \"%s\" does not exist", msg.PreparedStatement))
		return false
	}
	
	// Convert parameters to appropriate types
	params := make([]interface{}, len(msg.Parameters))
	for i, paramBytes := range msg.Parameters {
		if paramBytes == nil {
			params[i] = nil
			continue
		}
		
		paramStr := string(paramBytes)
		if paramStr == "" {
			params[i] = nil
			continue
		}
		
		// Try to convert to appropriate type based on OID if available
		if i < len(stmt.ParameterOIDs) {
			switch stmt.ParameterOIDs[i] {
			case 20: // INT8
				if val, err := strconv.ParseInt(paramStr, 10, 64); err == nil {
					params[i] = val
				} else {
					params[i] = paramStr
				}
			case 23: // INT4
				if val, err := strconv.ParseInt(paramStr, 10, 32); err == nil {
					params[i] = int32(val)
				} else {
					params[i] = paramStr
				}
			case 700: // FLOAT4
				if val, err := strconv.ParseFloat(paramStr, 32); err == nil {
					params[i] = float32(val)
				} else {
					params[i] = paramStr
				}
			case 701: // FLOAT8
				if val, err := strconv.ParseFloat(paramStr, 64); err == nil {
					params[i] = val
				} else {
					params[i] = paramStr
				}
			default:
				params[i] = paramStr
			}
		} else {
			params[i] = paramStr
		}
	}
	
	// Create portal
	portal := &Portal{
		Name:         msg.DestinationPortal,
		Statement:    stmt,
		Params:       params,
		ParamFormats: msg.ParameterFormatCodes,
	}
	
	// Store the portal with mutex protection
	psm.mutex.Lock()
	if msg.DestinationPortal == "" {
		// unnamed portal
		psm.portals[""] = portal
		logger.Debug("Stored unnamed portal")
	} else {
		psm.portals[msg.DestinationPortal] = portal
		logger.Debug("Stored named portal", "name", msg.DestinationPortal)
	}
	psm.mutex.Unlock()
	
	backend.Send(&pgproto3.BindComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush BindComplete", "error", err)
		return true
	}
	
	logger.Debug("Bind completed successfully")
	return false
}

// Describe handles the Describe message
func (psm *PreparedStatementManager) Describe(backend *pgproto3.Backend, msg *pgproto3.Describe) bool {
	logger.Debug("Describing object", "object_type", string(msg.ObjectType), "name", msg.Name)
	
	psm.mutex.RLock()
	defer psm.mutex.RUnlock()
	
	if msg.ObjectType == 'S' {
		// Describe a prepared statement
		stmt, exists := psm.preparedStatements[msg.Name]
		if !exists {
			logger.Warn("Prepared statement not found for describe", "statement_name", msg.Name)
			psm.sendErrorAndReady(backend, "26000", fmt.Sprintf("prepared statement \"%s\" does not exist", msg.Name))
			return false
		}
		
		// Send parameter description
		backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: stmt.ParameterOIDs})
		
		// If we have returning columns, send row description
		if len(stmt.ReturningColumns) > 0 {
			fields := make([]pgproto3.FieldDescription, len(stmt.ReturningColumns))
			for i, col := range stmt.ReturningColumns {
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
		} else {
			// For non-RETURNING statements, we might want to send an empty RowDescription
			// or determine column types through query analysis
			backend.Send(&pgproto3.NoData{})
		}
	} else if msg.ObjectType == 'P' {
		// Describe a portal
		portal, exists := psm.portals[msg.Name]
		if !exists {
			logger.Warn("Portal not found for describe", "portal_name", msg.Name)
			psm.sendErrorAndReady(backend, "26000", fmt.Sprintf("portal \"%s\" does not exist", msg.Name))
			return false
		}
		
		// If we have returning columns, send row description
		if len(portal.Statement.ReturningColumns) > 0 {
			fields := make([]pgproto3.FieldDescription, len(portal.Statement.ReturningColumns))
			for i, col := range portal.Statement.ReturningColumns {
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
		} else {
			// For non-RETURNING statements, we might want to send an empty RowDescription
			// or determine column types through query analysis
			backend.Send(&pgproto3.NoData{})
		}
	} else {
		logger.Warn("Unknown describe object type", "object_type", string(msg.ObjectType))
		psm.sendErrorAndReady(backend, "0A000", "unknown describe object type")
		return false
	}
	
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Describe response", "error", err)
		return true
	}
	
	logger.Debug("Describe completed successfully")
	return false
}

// Execute handles the Execute message
func (psm *PreparedStatementManager) Execute(backend *pgproto3.Backend, msg *pgproto3.Execute) bool {
	logger.Debug("Executing portal", "portal", msg.Portal, "max_rows", msg.MaxRows)
	
	// Retrieve the portal with mutex protection
	psm.mutex.RLock()
	portal, exists := psm.portals[msg.Portal]
	psm.mutex.RUnlock()
	
	if !exists {
		logger.Warn("Portal not found", "portal_name", msg.Portal)
		psm.sendErrorAndReady(backend, "26000", fmt.Sprintf("portal \"%s\" does not exist", msg.Portal))
		return false
	}
	
	// Bind parameters to the query
	boundQuery, err := BindParametersInQuery(portal.Statement.Query, portal.Params)
	if err != nil {
		logger.Warn("Failed to bind parameters", "error", err)
		psm.sendErrorAndReady(backend, "08006", fmt.Sprintf("failed to bind parameters: %v", err))
		return false
	}
	
	// For now, we'll just send back a completion message
	// In a real implementation, this would execute the query
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("EXECUTE")})
	
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Execute response", "error", err)
		return true
	}
	
	logger.Debug("Execute completed successfully")
	return false
}

// Helper method to send error and ready response
func (psm *PreparedStatementManager) sendErrorAndReady(backend *pgproto3.Backend, code, message string) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	backend.Flush()
}

// BindParametersInQuery binds parameters to a query string
func BindParametersInQuery(query string, params []interface{}) (string, error) {
	// This is a simplified implementation
	// A real implementation would need to properly escape parameters
	// and handle different data types correctly
	
	result := query
	for i, param := range params {
		placeholder := fmt.Sprintf("$%d", i+1)
		var replacement string
		
		if param == nil {
			replacement = "NULL"
		} else {
			switch v := param.(type) {
			case string:
				replacement = fmt.Sprintf("'%s'", v) // This is unsafe and should be properly escaped
			default:
				replacement = fmt.Sprintf("'%v'", v)
			}
		}
		
		result = strings.ReplaceAll(result, placeholder, replacement)
	}
	
	return result, nil
}