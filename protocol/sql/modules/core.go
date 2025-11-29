package modules

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/types"
)

// CoreExecutor handles the main execution logic
type CoreExecutor struct {
	planner   *sql.Planner
	catalog   catalog.Manager
	pipeline  *sql.QueryPipeline
}

// NewCoreExecutor creates a new core executor
func NewCoreExecutor(planner *sql.Planner, catalog catalog.Manager) *CoreExecutor {
	exec := &CoreExecutor{
		planner: planner,
		catalog: catalog,
	}
	
	// Initialize pipeline for batched execution
	// exec.pipeline = sql.NewQueryPipeline(exec, 10)
	
	return exec
}

// Execute executes a SQL query
func (ce *CoreExecutor) Execute(ctx context.Context, query string) (*types.ResultSet, error) {
	// Delegate to planner's executor
	return ce.planner.Executor().Execute(ctx, query)
}

// ExecuteParsed executes a parsed query
func (ce *CoreExecutor) ExecuteParsed(ctx context.Context, parsed *sql.ParsedQuery) (*types.ResultSet, error) {
	plan, err := ce.planner.CreatePlan(parsed.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	switch plan.Type {
	case sql.SelectStatement:
		// Delegate to DML executor
		return nil, fmt.Errorf("not implemented")
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", plan.Type)
	}
}

// ValidateQuery validates a SQL query
func (ce *CoreExecutor) ValidateQuery(query string) error {
	_, err := ce.planner.CreatePlan(query)
	return err
}

// Explain explains a SQL query
func (ce *CoreExecutor) Explain(query string) (*sql.Plan, error) {
	return ce.planner.CreatePlan(query)
}

// GetCatalog returns the catalog manager
func (ce *CoreExecutor) GetCatalog() catalog.Manager {
	return ce.catalog
}

// getTenantIDFromContext extracts tenant ID from context, defaulting to 1 if not found
func (ce *CoreExecutor) getTenantIDFromContext(ctx context.Context) int64 {
	if ctx == nil {
		return 1
	}
	
	if tenantID, ok := ctx.Value(logger.TenantIDKey).(int64); ok {
		return tenantID
	}
	
	// Default to 1 for backward compatibility
	return 1
}