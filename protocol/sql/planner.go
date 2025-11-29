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
	// Parse the query using the configured parser
	parsedQuery, err := p.parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}
	
	// Extract the statement from the parsed query
	stmt := parsedQuery.RawStmt
	if stmt == nil {
		// Handle case when RawStmt is nil (simple parser result)
		// Create a basic plan using the information available in ParsedQuery
		plan := &Plan{
			QueryString: query,
			Type:        parsedQuery.Type,
		}
		
		// Set operation based on statement type
		switch parsedQuery.Type {
		case SelectStatement:
			plan.Operation = "select"
		case InsertStatement:
			plan.Operation = "insert"
		case UpdateStatement:
			plan.Operation = "update"
		case DeleteStatement:
			plan.Operation = "delete"
		case BeginStatement:
			plan.Operation = "begin"
		case CommitStatement:
			plan.Operation = "commit"
		case RollbackStatement:
			plan.Operation = "rollback"
		case CreateTableStatement:
			plan.Operation = "create_table"
		case DropTableStatement:
			plan.Operation = "drop_table"
		case AlterTableStatement:
			plan.Operation = "alter_table"
		case CreateIndexStatement:
			plan.Operation = "create_index"
		case DropIndexStatement:
			plan.Operation = "drop_index"
		case CreateViewStatement:
			plan.Operation = "create_view"
		case DropViewStatement:
			plan.Operation = "drop_view"
		case AnalyzeStatementType:
			plan.Operation = "analyze"
		default:
			plan.Operation = "unknown"
		}
		
		return plan, nil
	}
	
	// Type assert to the correct type
	pgStmt, ok := stmt.(*pg_query.ParseResult)
	if !ok {
		return nil, fmt.Errorf("statement is not of expected type")
	}

	if len(pgStmt.Stmts) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	// For now, we only handle the first statement
	stmtNode := pgStmt.Stmts[0].GetStmt()
	if stmtNode == nil {
		return nil, fmt.Errorf("invalid statement")
	}

	plan := &Plan{
		QueryString: query,
	}

	// Determine statement type and extract relevant information
	switch {
	case stmtNode.GetSelectStmt() != nil:
		plan.Type = SelectStatement
		plan.Operation = "select"
		p.extractSelectInfoFromPGNode(pgStmt, plan)
	case stmtNode.GetInsertStmt() != nil:
		plan.Type = InsertStatement
		plan.Operation = "insert"
		p.extractInsertInfoFromPGNode(pgStmt, plan)
	case stmtNode.GetUpdateStmt() != nil:
		plan.Type = UpdateStatement
		plan.Operation = "update"
		p.extractUpdateInfoFromPGNode(pgStmt, plan)
	case stmtNode.GetDeleteStmt() != nil:
		plan.Type = DeleteStatement
		plan.Operation = "delete"
		p.extractDeleteInfoFromPGNode(pgStmt, plan)
	case stmtNode.GetCreateStmt() != nil:
		plan.Type = CreateTableStatement
	case stmtNode.GetDropStmt() != nil:
		plan.Type = DropTableStatement
	case stmtNode.GetAlterTableStmt() != nil:
		plan.Type = AlterTableStatement
	case stmtNode.GetIndexStmt() != nil:
		plan.Type = CreateIndexStatement
	case stmtNode.GetDropStmt() != nil:
		plan.Type = DropIndexStatement
	case stmtNode.GetViewStmt() != nil:
		plan.Type = CreateViewStatement
	case stmtNode.GetDropStmt() != nil:
		plan.Type = DropViewStatement
	case stmtNode.GetTransactionStmt() != nil:
		transStmt := stmtNode.GetTransactionStmt()
		switch transStmt.GetKind() {
		case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN:
			plan.Type = BeginStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
			plan.Type = CommitStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			plan.Type = RollbackStatement
		}
	case stmtNode.GetVacuumStmt() != nil:
		vacuumStmt := stmtNode.GetVacuumStmt()
		if !vacuumStmt.GetIsVacuumcmd() {
			plan.Type = AnalyzeStatementType
		}
	default:
		plan.Type = UnknownStatement
	}

	return plan, nil
}