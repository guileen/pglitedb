package sql

import (
	"context"
	"fmt"
	
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
)

// Planner is responsible for creating execution plans from parsed queries
type Planner struct {
	parser    Parser
	executor  *Executor
	optimizer *QueryOptimizer
}

// NewPlanner creates a new query planner
func NewPlanner(parser Parser) *Planner {
	// If no parser is provided, use the hybrid parser for better performance
	if parser == nil {
		parser = NewHybridPGParser()
	}
	
	return &Planner{
		parser:    parser,
		optimizer: NewQueryOptimizer(),
	}
}

// NewPlannerWithCatalog creates a new query planner with catalog
func NewPlannerWithCatalog(parser Parser, catalogMgr catalog.Manager) *Planner {
	planner := &Planner{
		parser:    parser,
		optimizer: NewQueryOptimizerWithDataManager(catalogMgr),
	}
	// Create executor with this planner and catalog
	planner.executor = NewExecutorWithCatalog(planner, catalogMgr)
	return planner
}

// Execute executes a SQL query and returns the result
func (p *Planner) Execute(ctx context.Context, query string) (*types.ResultSet, error) {
	if p.executor != nil {
		return p.executor.Execute(ctx, query)
	}
	return nil, fmt.Errorf("executor not initialized")
}

// Executor returns the executor associated with this planner
func (p *Planner) Executor() *Executor {
	return p.executor
}

// SetCatalog sets the catalog manager for the executor
func (p *Planner) SetCatalog(catalogMgr catalog.Manager) {
	if p.executor == nil {
		// Create a new executor with catalog
		p.executor = NewExecutorWithCatalog(p, catalogMgr)
	} else {
		p.executor.SetCatalog(catalogMgr)
	}
}

// CreatePlan creates an execution plan from a SQL query
func (p *Planner) CreatePlan(query string) (*Plan, error) {
	// Parse the query using pg_query
	result, err := pg_query.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	if len(result.Stmts) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	// For now, we only handle the first statement
	stmt := result.Stmts[0].GetStmt()
	if stmt == nil {
		return nil, fmt.Errorf("invalid statement")
	}

	plan := &Plan{
		QueryString: query,
	}

	// Determine statement type and extract relevant information
	switch {
	case stmt.GetSelectStmt() != nil:
		plan.Type = SelectStatement
		p.extractSelectInfoFromPGNode(stmt, plan)
	case stmt.GetInsertStmt() != nil:
		plan.Type = InsertStatement
		p.extractInsertInfoFromPGNode(stmt, plan)
	case stmt.GetUpdateStmt() != nil:
		plan.Type = UpdateStatement
		p.extractUpdateInfoFromPGNode(stmt, plan)
	case stmt.GetDeleteStmt() != nil:
		plan.Type = DeleteStatement
		p.extractDeleteInfoFromPGNode(stmt, plan)
	case stmt.GetCreateStmt() != nil:
		plan.Type = CreateTableStatement
	case stmt.GetDropStmt() != nil:
		plan.Type = DropTableStatement
	case stmt.GetAlterTableStmt() != nil:
		plan.Type = AlterTableStatement
	case stmt.GetIndexStmt() != nil:
		plan.Type = CreateIndexStatement
	case stmt.GetDropStmt() != nil:
		plan.Type = DropIndexStatement
	case stmt.GetViewStmt() != nil:
		plan.Type = CreateViewStatement
	case stmt.GetDropStmt() != nil:
		plan.Type = DropViewStatement
	case stmt.GetTransactionStmt() != nil:
		transStmt := stmt.GetTransactionStmt()
		switch transStmt.GetKind() {
		case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN:
			plan.Type = BeginStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
			plan.Type = CommitStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			plan.Type = RollbackStatement
		}
	case stmt.GetVacuumStmt() != nil:
		vacuumStmt := stmt.GetVacuumStmt()
		if !vacuumStmt.GetIsVacuumcmd() {
			plan.Type = AnalyzeStatementType
		}
	default:
		plan.Type = UnknownStatement
	}

	return plan, nil
}