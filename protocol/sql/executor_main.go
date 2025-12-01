package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/guileen/pglitedb/types"
)

type Executor struct {
	planner *Planner
	catalog catalog.Manager
	inTransaction bool
	pipeline *QueryPipeline
}

// GetCatalog returns the catalog manager
func (e *Executor) GetCatalog() catalog.Manager {
	return e.catalog
}

// SetCatalog sets the catalog manager
func (e *Executor) SetCatalog(catalog catalog.Manager) {
	e.catalog = catalog
}

// SetPlanner sets the planner for this executor
func (e *Executor) SetPlanner(planner *Planner) {
	e.planner = planner
}

// getTenantIDFromContext extracts tenant ID from context, defaulting to 1 if not found
func (e *Executor) getTenantIDFromContext(ctx context.Context) int64 {
	if ctx == nil {
		return 1
	}
	
	if tenantID, ok := ctx.Value(logger.TenantIDKey).(int64); ok {
		return tenantID
	}
	
	// Default to 1 for backward compatibility
	return 1
}

func NewExecutor(planner *Planner) *Executor {
	return &Executor{
		planner: planner,
	}
}

func NewExecutorWithCatalog(planner *Planner, catalog catalog.Manager) *Executor {
	exec := &Executor{
		planner: planner,
		catalog: catalog,
	}
	
	// Initialize pipeline for batched execution
	if exec.planner != nil {
		exec.pipeline = NewQueryPipeline(exec, 10)
	}
	
	return exec
}

func (e *Executor) Execute(ctx context.Context, query string) (*types.ResultSet, error) {
	// Use pooled ResultSet instead of allocating new one each time
	// result := types.AcquireResultSet()
	// defer func() {
		// Only release if not returning successfully
		// Note: This is a simplified approach - in practice, the caller would need to release
	// }()
	
	if e.planner == nil {
		return nil, fmt.Errorf("planner not initialized")
	}
	
	parsed, err := e.planner.parser.Parse(query)
	if err != nil {
		// ReleaseResultSet(result)  // This line was incorrect
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	switch parsed.StatementType {
	case parser.SelectStatement:
		plan, err := e.planner.CreatePlan(query)
		if err != nil {
			return nil, fmt.Errorf("failed to create execution plan: %w", err)
		}
		return e.executeSelect(ctx, plan)
	case parser.InsertStatement, parser.UpdateStatement, parser.DeleteStatement:
		plan, err := e.planner.CreatePlan(query)
		if err != nil {
			return nil, fmt.Errorf("failed to create execution plan: %w", err)
		}
		switch parsed.StatementType {
		case parser.InsertStatement:
			return e.executeInsert(ctx, plan)
		case parser.UpdateStatement:
			return e.executeUpdate(ctx, plan)
		case parser.DeleteStatement:
			return e.executeDelete(ctx, plan)
		}
	case parser.BeginStatement:
		return e.executeBegin(ctx)
	case parser.CommitStatement:
		return e.executeCommit(ctx)
	case parser.RollbackStatement:
		return e.executeRollback(ctx)
	case parser.CreateTableStatement, parser.DropTableStatement, parser.AlterTableStatement, 
	     parser.CreateIndexStatement, parser.DropIndexStatement, parser.CreateViewStatement, parser.DropViewStatement,
	     parser.CreateDatabaseStatement, parser.DropDatabaseStatement, parser.AlterDatabaseStatement, parser.TruncateTableStatement:
		return e.executeDDL(ctx, query)
	case parser.AnalyzeStatementType:
		return e.executeAnalyze(ctx, query)
	case parser.UnknownStatement:
		// For unknown statements, try to handle them as system queries
		// This is particularly important for pgbench and other tools that query system tables
		return e.executeSystemQuery(ctx, query)
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", parsed.StatementType)
	}

	// This should never be reached
	return nil, fmt.Errorf("unhandled statement type: %v", parsed.StatementType)
}

func (e *Executor) ExecuteParsed(ctx context.Context, parsed *parser.ParsedQuery) (*types.ResultSet, error) {
	plan, err := e.planner.CreatePlan(parsed.QueryString)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	switch plan.Type {
	case parser.SelectStatement:
		return e.executeSelect(ctx, plan)
	case parser.UnknownStatement:
		// For unknown statements, try to handle them as system queries
		return e.executeSystemQuery(ctx, parsed.QueryString)
	case parser.InsertStatement, parser.UpdateStatement, parser.DeleteStatement,
	     parser.BeginStatement, parser.CommitStatement, parser.RollbackStatement,
	     parser.CreateTableStatement, parser.DropTableStatement, parser.AlterTableStatement,
	     parser.CreateIndexStatement, parser.DropIndexStatement, parser.CreateViewStatement, parser.DropViewStatement,
	     parser.CreateDatabaseStatement, parser.DropDatabaseStatement, parser.AlterDatabaseStatement,
	     parser.TruncateTableStatement, parser.AnalyzeStatementType:
		// Delegate to the main Execute method for all other supported statement types
		return e.Execute(ctx, parsed.QueryString)
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", plan.Type)
	}
}

// executeSystemQuery handles queries that the parser couldn't classify
// This is particularly important for system table queries from tools like pgbench
func (e *Executor) executeSystemQuery(ctx context.Context, query string) (*types.ResultSet, error) {
	// First try to handle as a system query (SELECT against system tables)
	trimmedQuery := strings.TrimSpace(query)
	lowerQuery := strings.ToLower(trimmedQuery)
	
	// Check if this looks like a SELECT query against system tables
	if strings.HasPrefix(lowerQuery, "select") {
		// Try to extract the table name from a simple SELECT query
		fromIndex := strings.Index(lowerQuery, " from ")
		if fromIndex != -1 {
			// Extract everything after FROM until WHERE, ORDER BY, etc.
			afterFrom := trimmedQuery[fromIndex+6:] // Skip " from "
			tableEnd := len(afterFrom)
			
			// Find end of table name
			spaceIndex := strings.Index(afterFrom, " ")
			whereIndex := strings.Index(strings.ToLower(afterFrom), " where ")
			orderIndex := strings.Index(strings.ToLower(afterFrom), " order by ")
			limitIndex := strings.Index(strings.ToLower(afterFrom), " limit ")
			groupIndex := strings.Index(strings.ToLower(afterFrom), " group by ")
			
			// Find the earliest termination
			indices := []int{spaceIndex, whereIndex, orderIndex, limitIndex, groupIndex}
			for _, idx := range indices {
				if idx != -1 && idx < tableEnd {
					tableEnd = idx
				}
			}
			
			tableName := strings.TrimSpace(afterFrom[:tableEnd])
			
			// If this looks like a system table, create a fake plan and route to existing system query handler
			if isSystemTable(tableName) {
				// Create a minimal plan to pass to the existing system query handler
				plan := &Plan{
					QueryString: query,
					Type:        parser.SelectStatement,
					Table:       tableName,
				}
				return e.executeSystemTableQuery(ctx, plan)
			}
		}
	}
	
	// If we can't handle it as a system query, try to parse and execute it normally
	// This handles cases where the parser marked it as Unknown but it might actually be a valid statement
	if e.planner != nil {
		parsed, err := e.planner.parser.Parse(query)
		if err == nil && parsed.StatementType != parser.UnknownStatement {
			// If we can parse it and it's not Unknown, delegate to Execute method
			return e.Execute(ctx, query)
		}
	}
	
	// If we can't handle it as a system query or parse it successfully, return the original error
	return nil, fmt.Errorf("unsupported statement type: UNKNOWN")
}

func (e *Executor) ValidateQuery(query string) error {
	_, err := e.planner.CreatePlan(query)
	return err
}

func (e *Executor) Explain(query string) (*Plan, error) {
	return e.planner.CreatePlan(query)
}