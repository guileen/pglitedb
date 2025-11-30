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
	planCache *LRUCache
	planPool  *PlanPool
}

// NewPlanner creates a new query planner
// Increased plan cache size to reduce repeated parsing based on profiling analysis
func NewPlanner(parser Parser) *Planner {
	// If no parser is provided, use the hybrid parser for better performance
	if parser == nil {
		parser = NewHybridPGParser()
	}
	
	// Initialize plan cache with larger capacity to reduce repeated parsing
	planCache := NewLRUCache(5000)
	
	return &Planner{
		parser:    parser,
		optimizer: NewQueryOptimizer(),
		planCache: planCache,
		planPool:  NewPlanPool(),
	}
}

// NewPlannerWithCatalog creates a new query planner with catalog
// Increased plan cache size to reduce repeated parsing based on profiling analysis
func NewPlannerWithCatalog(parser Parser, catalogMgr catalog.Manager) *Planner {
	// Initialize plan cache with larger capacity to reduce repeated parsing
	planCache := NewLRUCache(5000)
	
	planner := &Planner{
		parser:    parser,
		optimizer: NewQueryOptimizerWithDataManager(catalogMgr),
		planCache: planCache,
		planPool:  NewPlanPool(),
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

// EnablePlanCaching enables or disables query plan caching
func (p *Planner) EnablePlanCaching(enabled bool) {
	if enabled && p.planCache == nil {
		p.planCache = NewLRUCache(1000)
	} else if !enabled {
		p.planCache = nil
	}
}

// ClearPlanCache clears the query plan cache
func (p *Planner) ClearPlanCache() {
	if p.planCache != nil {
		p.planCache.Clear()
	}
}

// PlanCacheSize returns the number of cached plans
func (p *Planner) PlanCacheSize() int {
	if p.planCache != nil {
		return p.planCache.Len()
	}
	return 0
}

// CacheStats returns cache hit and miss statistics
func (p *Planner) CacheStats() (hits, misses int64) {
	if p.planCache != nil {
		return p.planCache.Stats()
	}
	return 0, 0
}

// CacheHitRate returns the cache hit rate as a percentage
func (p *Planner) CacheHitRate() float64 {
	if p.planCache != nil {
		return p.planCache.HitRate()
	}
	return 0.0
}

// ResetCacheStats resets the cache hit/miss counters
func (p *Planner) ResetCacheStats() {
	if p.planCache != nil {
		p.planCache.ResetStats()
	}
}

// CreatePlan creates an execution plan from a SQL query
func (p *Planner) CreatePlan(query string) (*Plan, error) {
	// Normalize the query for cache key
	normalizedQuery := p.normalizeSQL(query)
	
	// Check cache first
	if p.planCache != nil {
		if cachedPlan, ok := p.planCache.Get(normalizedQuery); ok {
			if plan, ok := cachedPlan.(*Plan); ok {
				// Return a copy of the cached plan to avoid concurrency issues
				return p.copyPlan(plan), nil
			}
		}
	}
	
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
			Table:       parsedQuery.Table,
			Fields:      parsedQuery.Fields,
			Conditions:  parsedQuery.Conditions,
			OrderBy:     parsedQuery.OrderBy,
			Limit:       parsedQuery.Limit,
			Updates:     parsedQuery.Updates,
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
		
		// Cache the plan with normalized query as key
		if p.planCache != nil {
			p.planCache.Put(normalizedQuery, p.copyPlan(plan))
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
	
	// Apply optimization if optimizer is available
	if p.optimizer != nil {
		optimizedPlan, err := p.optimizer.OptimizePlan(plan)
		if err == nil {
			plan = optimizedPlan
		}
	}
	
	// Cache the plan with normalized query as key
	if p.planCache != nil {
		p.planCache.Put(normalizedQuery, p.copyPlan(plan))
	}

	return plan, nil
}

// normalizeSQL normalizes a SQL query string for use as a cache key
// This removes extra whitespace, normalizes case, and standardizes formatting
func (p *Planner) normalizeSQL(query string) string {
	// Use the enhanced NormalizeQuery function for better cache hit rates
	return NormalizeQuery(query)
}

// copyPlan creates a deep copy of a Plan for thread safety using object pooling
func (p *Planner) copyPlan(original *Plan) *Plan {
	// Get a plan from the pool
	plan := p.planPool.GetPlan()
	
	// Copy scalar fields
	plan.Type = original.Type
	plan.Operation = original.Operation
	plan.Table = original.Table
	plan.QueryString = original.QueryString
	
	// Copy slices using the pool
	if len(original.Fields) > 0 {
		fields := p.planPool.GetStringSlice(len(original.Fields))
		*fields = append(*fields, original.Fields...)
		plan.Fields = *fields
	}
	
	if len(original.Conditions) > 0 {
		conditions := p.planPool.GetConditionSlice(len(original.Conditions))
		*conditions = append(*conditions, original.Conditions...)
		plan.Conditions = *conditions
	}
	
	if len(original.OrderBy) > 0 {
		orderBy := p.planPool.GetOrderBySlice(len(original.OrderBy))
		*orderBy = append(*orderBy, original.OrderBy...)
		plan.OrderBy = *orderBy
	}
	
	if len(original.GroupBy) > 0 {
		groupBy := p.planPool.GetStringSlice(len(original.GroupBy))
		*groupBy = append(*groupBy, original.GroupBy...)
		plan.GroupBy = *groupBy
	}
	
	if len(original.Aggregates) > 0 {
		// For Aggregates, we'll create a new slice as it's not pooled
		aggregates := make([]Aggregate, len(original.Aggregates))
		copy(aggregates, original.Aggregates)
		plan.Aggregates = aggregates
	}
	
	// Copy maps
	if len(original.Values) > 0 {
		plan.Values = make(map[string]interface{}, len(original.Values))
		for k, v := range original.Values {
			plan.Values[k] = v
		}
	}
	
	if len(original.Updates) > 0 {
		plan.Updates = make(map[string]interface{}, len(original.Updates))
		for k, v := range original.Updates {
			plan.Updates[k] = v
		}
	}
	
	// Handle pointers
	if original.Limit != nil {
		limitCopy := *original.Limit
		plan.Limit = &limitCopy
	}
	
	if original.Offset != nil {
		offsetCopy := *original.Offset
		plan.Offset = &offsetCopy
	}
	
	return plan
}