package components

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/guileen/pglitedb/logger"
	customctx "github.com/guileen/pglitedb/context"
	"github.com/guileen/pglitedb/types"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// Ensure QueryProcessor implements QueryProcessorInterface
var _ interfaces.QueryProcessorInterface = &QueryProcessor{}

// QueryProcessor handles query parsing and execution
type QueryProcessor struct {
	executor *sql.Executor
	parser   sql.Parser
	planner  *sql.Planner
}

// NewQueryProcessor creates a new query processor
func NewQueryProcessor(executor *sql.Executor, parser sql.Parser, planner *sql.Planner) *QueryProcessor {
	return &QueryProcessor{
		executor: executor,
		parser:   parser,
		planner:  planner,
	}
}

// ProcessQuery handles a simple query
func (qp *QueryProcessor) ProcessQuery(ctx context.Context, backend *pgproto3.Backend, query string) (bool, error) {
	// Get a QueryContext from the pool
	queryCtx := customctx.GetQueryContext()
	defer customctx.PutQueryContext(queryCtx)
	
	// Set query context values
	queryCtx.QueryID = fmt.Sprintf("query-%d", time.Now().UnixNano())
	queryCtx.SQL = query
	queryCtx.StartTime = time.Now()
	
	logger.Debug("Processing query", "query", query, "query_id", queryCtx.QueryID)
	
	// Handle empty query or standalone semicolon
	trimmedQuery := strings.TrimSpace(query)
	if trimmedQuery == "" || trimmedQuery == ";" {
		logger.Debug("Empty query or standalone semicolon received")
		backend.Send(&pgproto3.EmptyQueryResponse{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			logger.Error("Failed to flush EmptyQueryResponse", "error", err)
			return true, err
		}
		return false, nil
	}
	
	startTime := time.Now()
	parsed, err := qp.parser.Parse(query)
	parseDuration := time.Since(startTime)
	if err != nil {
		logger.Warn("Failed to parse SQL query", "error", err, "query", query, "parse_duration", parseDuration.String(), "query_id", queryCtx.QueryID)
		qp.sendErrorAndReady(backend, "42601", fmt.Sprintf("Syntax error: failed to parse SQL query: %v", err))
		return false, nil
	}
	
	logger.Debug("Query parsed successfully", "parse_duration", parseDuration.String(), "query_id", queryCtx.QueryID)
	
	startTime = time.Now()
	result, err := qp.planner.Execute(context.Background(), query)
	executeDuration := time.Since(startTime)
	if err != nil {
		logger.Warn("Query execution failed", "error", err, "query", query, "execute_duration", executeDuration.String(), "query_id", queryCtx.QueryID)
		qp.sendErrorAndReady(backend, "42000", fmt.Sprintf("Query execution failed: %v", err))
		return false, nil
	}
	
	logger.Debug("Query executed successfully", "execute_duration", executeDuration.String(), "row_count", result.Count, "query_id", queryCtx.QueryID)
	
	// Handle RETURNING clause for INSERT/UPDATE/DELETE
	if len(parsed.ReturningColumns) > 0 {
		logger.Debug("Processing RETURNING clause", "returning_columns", parsed.ReturningColumns)
		returningResult := qp.buildReturningResult(result, parsed.ReturningColumns)
		qp.sendReturningResult(backend, returningResult)
		if err := backend.Flush(); err != nil {
			logger.Error("Failed to flush RETURNING result", "error", err)
			return true, err
		}
		return false, nil
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
		} else {
			commandTag = fmt.Sprintf("SELECT %d", result.Count)
		}
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	}
	
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		logger.Error("Failed to flush query response", "error", err)
		return true, err
	}
	
	return false, nil
}

// ProcessDDL handles DDL queries
func (qp *QueryProcessor) ProcessDDL(ctx context.Context, backend *pgproto3.Backend, query string) (bool, error) {
	// For now, we'll just delegate to ProcessQuery
	// In a real implementation, this might have special handling for DDL operations
	return qp.ProcessQuery(ctx, backend, query)
}

// ProcessDML handles DML queries
func (qp *QueryProcessor) ProcessDML(ctx context.Context, backend *pgproto3.Backend, query string) (bool, error) {
	// For now, we'll just delegate to ProcessQuery
	// In a real implementation, this might have special handling for DML operations
	return qp.ProcessQuery(ctx, backend, query)
}

// HealthCheck performs a health check on the query processor
func (qp *QueryProcessor) HealthCheck() error {
	// Perform any necessary health checks
	return nil
}

// sendErrorAndReady sends an error response followed by a ReadyForQuery message
func (qp *QueryProcessor) sendErrorAndReady(backend *pgproto3.Backend, code, message string) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	backend.Flush()
}

// buildReturningResult constructs a result set for RETURNING clauses
func (qp *QueryProcessor) buildReturningResult(result *types.ResultSet, returningColumns []string) *types.ResultSet {
	// For now, we'll just return the original result
	// In a real implementation, this would filter the result to only include the RETURNING columns
	return result
}

// sendReturningResult sends a RETURNING result set
func (qp *QueryProcessor) sendReturningResult(backend *pgproto3.Backend, result *types.ResultSet) {
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
			for j, val := range row {
				if val == nil {
					values[j] = nil
				} else {
					values[j] = []byte(fmt.Sprintf("%v", val))
				}
			}
			backend.Send(&pgproto3.DataRow{Values: values})
		}
		
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", result.Count))})
	} else {
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
	}
}