package management

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/guileen/pglitedb/logger"
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

// StatementManager manages prepared statements and portals
type StatementManager struct {
	parser sql.Parser
	
	// Extended query protocol state
	preparedStatements map[string]*PreparedStatement
	portals           map[string]*Portal
	mutex             *sync.RWMutex
}

// NewStatementManager creates a new prepared statement manager
func NewStatementManager(parser sql.Parser) *StatementManager {
	return &StatementManager{
		parser:            parser,
		preparedStatements: make(map[string]*PreparedStatement),
		portals:           make(map[string]*Portal),
		mutex:             &sync.RWMutex{},
	}
}

// Parse handles the Parse message
func (sm *StatementManager) Parse(ctx context.Context, backend *pgproto3.Backend, msg *pgproto3.Parse) (bool, error) {
	logger.Debug("Parsing prepared statement", "name", msg.Name, "query", msg.Query, "parameter_count", len(msg.ParameterOIDs))
	
	// Create a prepared statement
	stmt := &PreparedStatement{
		Name:  msg.Name,
		Query: msg.Query,
		ParameterOIDs: msg.ParameterOIDs,
	}
	
	// Parse the query to extract RETURNING columns if present
	parsed, err := sm.parser.Parse(msg.Query)
	if err == nil {
		stmt.ReturningColumns = parsed.ReturningColumns
		logger.Debug("Query parsed for prepared statement", "returning_columns", parsed.ReturningColumns)
	} else {
		logger.Warn("Failed to parse query for prepared statement", "error", err)
	}
	
	// Store the prepared statement with mutex protection
	sm.mutex.Lock()
	if msg.Name == "" {
		// unnamed statement
		sm.preparedStatements[""] = stmt
		logger.Debug("Stored unnamed prepared statement")
	} else {
		sm.preparedStatements[msg.Name] = stmt
		logger.Debug("Stored named prepared statement", "name", msg.Name)
	}
	sm.mutex.Unlock()
	
	backend.Send(&pgproto3.ParseComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush ParseComplete", "error", err)
		return true, err
	}
	
	logger.Debug("Parse completed successfully")
	return false, nil
}

// Bind handles the Bind message
func (sm *StatementManager) Bind(ctx context.Context, backend *pgproto3.Backend, msg *pgproto3.Bind) (bool, error) {
	logger.Debug("Binding portal", "destination_portal", msg.DestinationPortal, "prepared_statement", msg.PreparedStatement, "parameter_count", len(msg.Parameters))
	
	// Retrieve the prepared statement with mutex protection
	sm.mutex.RLock()
	stmt, exists := sm.preparedStatements[msg.PreparedStatement]
	sm.mutex.RUnlock()
	
	if !exists {
		logger.Warn("Prepared statement not found", "statement_name", msg.PreparedStatement)
		sm.sendErrorAndReady(backend, "26000", fmt.Sprintf("prepared statement \"%s\" does not exist", msg.PreparedStatement))
		return false, nil
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
	sm.mutex.Lock()
	if msg.DestinationPortal == "" {
		// unnamed portal
		sm.portals[""] = portal
		logger.Debug("Stored unnamed portal")
	} else {
		sm.portals[msg.DestinationPortal] = portal
		logger.Debug("Stored named portal", "name", msg.DestinationPortal)
	}
	sm.mutex.Unlock()
	
	backend.Send(&pgproto3.BindComplete{})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush BindComplete", "error", err)
		return true, err
	}
	
	logger.Debug("Bind completed successfully")
	return false, nil
}

// Describe handles the Describe message
func (sm *StatementManager) Describe(ctx context.Context, backend *pgproto3.Backend, msg *pgproto3.Describe) (bool, error) {
	logger.Debug("Describing object", "object_type", string(msg.ObjectType), "name", msg.Name)
	
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	
	if msg.ObjectType == 'S' {
		// Describe a prepared statement
		stmt, exists := sm.preparedStatements[msg.Name]
		if !exists {
			logger.Warn("Prepared statement not found for describe", "statement_name", msg.Name)
			sm.sendErrorAndReady(backend, "26000", fmt.Sprintf("prepared statement \"%s\" does not exist", msg.Name))
			return false, nil
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
		portal, exists := sm.portals[msg.Name]
		if !exists {
			logger.Warn("Portal not found for describe", "portal_name", msg.Name)
			sm.sendErrorAndReady(backend, "26000", fmt.Sprintf("portal \"%s\" does not exist", msg.Name))
			return false, nil
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
		sm.sendErrorAndReady(backend, "0A000", "unknown describe object type")
		return false, nil
	}
	
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Describe response", "error", err)
		return true, err
	}
	
	logger.Debug("Describe completed successfully")
	return false, nil
}

// Execute handles the Execute message
func (sm *StatementManager) Execute(ctx context.Context, backend *pgproto3.Backend, msg *pgproto3.Execute) (bool, error) {
	logger.Debug("Executing portal", "portal", msg.Portal, "max_rows", msg.MaxRows)
	
	// Retrieve the portal with mutex protection
	sm.mutex.RLock()
	portal, exists := sm.portals[msg.Portal]
	sm.mutex.RUnlock()
	
	if !exists {
		logger.Warn("Portal not found", "portal_name", msg.Portal)
		sm.sendErrorAndReady(backend, "26000", fmt.Sprintf("portal \"%s\" does not exist", msg.Portal))
		return false, nil
	}
	
	// Bind parameters to the query
	_, err := BindParametersInQuery(portal.Statement.Query, portal.Params)
	if err != nil {
		logger.Warn("Failed to bind parameters", "error", err)
		sm.sendErrorAndReady(backend, "08006", fmt.Sprintf("failed to bind parameters: %v", err))
		return false, nil
	}
	
	// For now, we'll just send back a completion message
	// In a real implementation, this would execute the query
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("EXECUTE")})
	
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush Execute response", "error", err)
		return true, err
	}
	
	logger.Debug("Execute completed successfully")
	return false, nil
}

// HealthCheck performs a health check on the statement manager
func (sm *StatementManager) HealthCheck() error {
	// Perform any necessary health checks
	return nil
}

// Helper method to send error and ready response
func (sm *StatementManager) sendErrorAndReady(backend *pgproto3.Backend, code, message string) {
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