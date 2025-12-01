package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/protocol/sql/parser"
)

// Cache defines the interface for cache implementations
type Cache interface {
	Get(key string) (interface{}, bool)
	Put(key string, value interface{})
	Remove(key string)
	Len() int
	Clear()
	Stats() (hits, misses int64)
	HitRate() float64
	ResetStats()
}

// Planner is responsible for creating execution plans from parsed queries
type Planner struct {
	parser    Parser
	executor  *Executor
	optimizer *QueryOptimizer
	planCache Cache
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
	// Increased from 5000 to 10000 to further reduce CGO call overhead
	// Using sharded LRU cache to reduce lock contention in concurrent scenarios
	planCache := NewShardedLRUCache(10000)
	
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
	// Increased from 5000 to 10000 to further reduce CGO call overhead
	// Using sharded LRU cache to reduce lock contention in concurrent scenarios
	planCache := NewShardedLRUCache(10000)
	
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
		p.planCache = NewShardedLRUCache(1000)
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
			Type:        parsedQuery.StatementType,
			Table:       parsedQuery.Table,
			Fields:      parsedQuery.Fields,
			OrderBy:     parsedQuery.OrderBy,
			Limit:       parsedQuery.Limit,
			Updates:     parsedQuery.Updates,
		}
		
		// Convert parser.Conditions to planner.Conditions
		plan.Conditions = make([]Condition, len(parsedQuery.Conditions))
		for i, cond := range parsedQuery.Conditions {
			// Parse the string value back to the appropriate type
			var parsedValue interface{} = cond.Value
			// Try to parse as integer
			if intVal, err := strconv.ParseInt(cond.Value, 10, 32); err == nil {
				parsedValue = int32(intVal)
			} else if floatVal, err := strconv.ParseFloat(cond.Value, 64); err == nil {
				parsedValue = floatVal
			} else if cond.Value == "true" {
				parsedValue = true
			} else if cond.Value == "false" {
				parsedValue = false
			}
			// For other cases, keep as string
			
			plan.Conditions[i] = Condition{
				Field:    cond.Field,
				Operator: cond.Operator,
				Value:    parsedValue,
			}
		}
		
		// Handle INSERT statement values for simple parser
		if parsedQuery.StatementType == parser.InsertStatement {
			// Convert parsedQuery.Values ([][]string) to plan.Values (map[string]interface{})
			plan.Values = make(map[string]interface{})
			
			// Get column names from Fields or create generic names
			columns := parsedQuery.Fields
			if len(columns) == 0 && len(parsedQuery.Values) > 0 && len(parsedQuery.Values[0]) > 0 {
				// Create generic column names if none provided
				columns = make([]string, len(parsedQuery.Values[0]))
				for i := range columns {
					columns[i] = fmt.Sprintf("col%d", i)
				}
			}
			
			// Use first row of values (assuming single row insert)
			if len(parsedQuery.Values) > 0 && len(columns) > 0 {
				firstRow := parsedQuery.Values[0]
				for i, value := range firstRow {
					if i < len(columns) {
						// Try to parse the value to the appropriate type
						if parsedValue, err := parseLiteralValue(value); err == nil {
							plan.Values[columns[i]] = parsedValue
						} else {
							// Keep as string if parsing fails
							plan.Values[columns[i]] = value
						}
					}
				}
			}
		}
		
		// Set operation based on statement type
		switch parsedQuery.StatementType {
		case parser.SelectStatement:
			plan.Operation = "select"
		case parser.InsertStatement:
			plan.Operation = "insert"
		case parser.UpdateStatement:
			plan.Operation = "update"
		case parser.DeleteStatement:
			plan.Operation = "delete"
		case parser.BeginStatement:
			plan.Operation = "begin"
		case parser.CommitStatement:
			plan.Operation = "commit"
		case parser.RollbackStatement:
			plan.Operation = "rollback"
		case parser.CreateTableStatement:
			plan.Operation = "create_table"
		case parser.DropTableStatement:
			plan.Operation = "drop_table"
		case parser.AlterTableStatement:
			plan.Operation = "alter_table"
		case parser.CreateIndexStatement:
			plan.Operation = "create_index"
		case parser.DropIndexStatement:
			plan.Operation = "drop_index"
		case parser.CreateViewStatement:
			plan.Operation = "create_view"
		case parser.DropViewStatement:
			plan.Operation = "drop_view"
		case parser.AnalyzeStatementType:
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
		plan.Type = parser.SelectStatement
		plan.Operation = "select"
		p.extractSelectInfoFromPGNode(pgStmt, plan)
	case stmtNode.GetInsertStmt() != nil:
		plan.Type = parser.InsertStatement
		plan.Operation = "insert"
		p.extractInsertInfoFromPGNode(pgStmt, plan)
	case stmtNode.GetUpdateStmt() != nil:
		plan.Type = parser.UpdateStatement
		plan.Operation = "update"
		p.extractUpdateInfoFromPGNode(pgStmt, plan)
	case stmtNode.GetDeleteStmt() != nil:
		plan.Type = parser.DeleteStatement
		plan.Operation = "delete"
		p.extractDeleteInfoFromPGNode(pgStmt, plan)
	case stmtNode.GetCreateStmt() != nil:
		plan.Type = parser.CreateTableStatement
	case stmtNode.GetDropStmt() != nil:
		plan.Type = parser.DropTableStatement
	case stmtNode.GetAlterTableStmt() != nil:
		plan.Type = parser.AlterTableStatement
	case stmtNode.GetIndexStmt() != nil:
		plan.Type = parser.CreateIndexStatement
	case stmtNode.GetDropStmt() != nil:
		plan.Type = parser.DropIndexStatement
	case stmtNode.GetViewStmt() != nil:
		plan.Type = parser.CreateViewStatement
	case stmtNode.GetDropStmt() != nil:
		plan.Type = parser.DropViewStatement
	case stmtNode.GetTransactionStmt() != nil:
		transStmt := stmtNode.GetTransactionStmt()
		switch transStmt.GetKind() {
		case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN:
			plan.Type = parser.BeginStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
			plan.Type = parser.CommitStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			plan.Type = parser.RollbackStatement
		}
	case stmtNode.GetVacuumStmt() != nil:
		vacuumStmt := stmtNode.GetVacuumStmt()
		if !vacuumStmt.GetIsVacuumcmd() {
			plan.Type = parser.AnalyzeStatementType
		}
	default:
		plan.Type = parser.UnknownStatement
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

// parseLiteralValue parses a literal value string into the appropriate Go type
func parseLiteralValue(value string) (interface{}, error) {
	trimmed := strings.TrimSpace(value)
	
	// Handle string literals (single or double quotes)
	if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
	   (strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
		// Remove quotes
		unquoted := trimmed[1 : len(trimmed)-1]
		// Handle escaped quotes
		unquoted = strings.ReplaceAll(unquoted, "''", "'")
		unquoted = strings.ReplaceAll(unquoted, "\\\"", "\"")
		return unquoted, nil
	}
	
	// Handle boolean values
	if strings.ToLower(trimmed) == "true" {
		return true, nil
	}
	if strings.ToLower(trimmed) == "false" {
		return false, nil
	}
	
	// Handle numeric values
	if i, err := strconv.ParseInt(trimmed, 10, 32); err == nil {
		return int32(i), nil
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return f, nil
	}
	
	// Return as string if no other type matches
	return trimmed, fmt.Errorf("could not parse as literal")
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
		for _, cond := range original.Conditions {
			*conditions = append(*conditions, Condition{
				Field:    cond.Field,
				Operator: cond.Operator,
				Value:    cond.Value,
			})
		}
		// Convert local Condition slice to parser.Condition slice
		parserConditions := make([]parser.Condition, len(*conditions))
		for i, cond := range *conditions {
			// Convert value to string for parser.Condition
			var valueStr string
			switch v := cond.Value.(type) {
			case string:
				valueStr = v
			case int, int32, int64, float32, float64:
				valueStr = fmt.Sprintf("%v", v)
			case bool:
				valueStr = strconv.FormatBool(v)
			default:
				valueStr = fmt.Sprintf("%v", v)
			}
			
			parserConditions[i] = parser.Condition{
				Field:    cond.Field,
				Operator: cond.Operator,
				Value:    valueStr,
			}
		}
		plan.Conditions = *conditions
	}
	
	if len(original.OrderBy) > 0 {
		orderBy := p.planPool.GetOrderBySlice(len(original.OrderBy))
		for _, ob := range original.OrderBy {
			*orderBy = append(*orderBy, OrderBy{
				Field: ob.Field,
				Order: ob.Direction,
			})
		}
		// Convert local OrderBy slice to parser.OrderBy slice
		parserOrderBy := make([]parser.OrderBy, len(*orderBy))
		for i, ob := range *orderBy {
			parserOrderBy[i] = parser.OrderBy{
				Field:      ob.Field,
				Direction:  ob.Order,
				NullsOrder: "", // Default value
			}
		}
		plan.OrderBy = parserOrderBy
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