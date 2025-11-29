package sql

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/logger"
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

	switch parsed.Type {
	case SelectStatement:
		plan, err := e.planner.CreatePlan(query)
		if err != nil {
			return nil, fmt.Errorf("failed to create execution plan: %w", err)
		}
		return e.executeSelect(ctx, plan)
	case InsertStatement, UpdateStatement, DeleteStatement:
		plan, err := e.planner.CreatePlan(query)
		if err != nil {
			return nil, fmt.Errorf("failed to create execution plan: %w", err)
		}
		switch parsed.Type {
		case InsertStatement:
			return e.executeInsert(ctx, plan)
		case UpdateStatement:
			return e.executeUpdate(ctx, plan)
		case DeleteStatement:
			return e.executeDelete(ctx, plan)
		}
	case BeginStatement:
		return e.executeBegin(ctx)
	case CommitStatement:
		return e.executeCommit(ctx)
	case RollbackStatement:
		return e.executeRollback(ctx)
	case CreateTableStatement, DropTableStatement, AlterTableStatement, 
	     CreateIndexStatement, DropIndexStatement, CreateViewStatement, DropViewStatement:
		return e.executeDDL(ctx, query)
	case AnalyzeStatementType:
		return e.executeAnalyze(ctx, query)
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", parsed.Type)
	}

	// This should never be reached
	return nil, fmt.Errorf("unhandled statement type: %v", parsed.Type)
}

func (e *Executor) ExecuteParsed(ctx context.Context, parsed *ParsedQuery) (*types.ResultSet, error) {
	plan, err := e.planner.CreatePlan(parsed.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	switch plan.Type {
	case SelectStatement:
		return e.executeSelect(ctx, plan)
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", plan.Type)
	}
}

func (e *Executor) ValidateQuery(query string) error {
	_, err := e.planner.CreatePlan(query)
	return err
}

func (e *Executor) Explain(query string) (*Plan, error) {
	return e.planner.CreatePlan(query)
}