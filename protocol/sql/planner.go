package sql

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Plan represents a query execution plan
type Plan struct {
	Type        StatementType
	Operation   string
	Table       string
	Fields      []string
	Conditions  []Condition
	Limit       *int64
	Offset      *int64
	OrderBy     []OrderBy
	GroupBy     []string
	Aggregates  []Aggregate
	QueryString string
	Values      map[string]interface{} // For INSERT operations
	Updates     map[string]interface{} // For UPDATE operations
}

// Condition represents a WHERE clause condition
type Condition struct {
	Field    string
	Operator string
	Value    interface{}
}

// OrderBy represents an ORDER BY clause
type OrderBy struct {
	Field string
	Order string // ASC or DESC
}

// Aggregate represents an aggregation function
type Aggregate struct {
	Function string // COUNT, SUM, AVG, etc.
	Field    string
	Alias    string
}

// Planner is responsible for creating execution plans from parsed queries
type Planner struct {
	parser   Parser
	executor *Executor
}

// NewPlanner creates a new query planner
func NewPlanner(parser Parser) *Planner {
	planner := &Planner{
		parser: parser,
	}
	// Create executor with this planner (circular dependency resolved at runtime)
	planner.executor = NewExecutor(planner)
	return planner
}

// Execute executes a SQL query and returns the result
func (p *Planner) Execute(ctx context.Context, query string) (*ResultSet, error) {
	if p.executor != nil {
		return p.executor.Execute(ctx, query)
	}
	return nil, fmt.Errorf("executor not initialized")
}

// SetCatalog sets the catalog manager for the executor
func (p *Planner) SetCatalog(catalog interface{}) {
	if p.executor != nil {
		// Create a new executor with catalog if needed
		executor := &Executor{
			planner: p,
		}
		// The catalog will be set directly in executor if needed
		p.executor = executor
	}
}

// CreatePlan generates an execution plan from a SQL query string
func (p *Planner) CreatePlan(query string) (*Plan, error) {
	parsed, err := p.parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	plan := &Plan{
		Type:        parsed.Type,
		QueryString: query,
	}

	// Handle different types of statements based on the parser used
	switch stmt := parsed.Statement.(type) {
	case string:
		// Handle string-based statements from our new parser
		lowerStmt := strings.ToLower(strings.TrimSpace(stmt))
		switch {
		case strings.HasPrefix(lowerStmt, "select"):
			plan.Operation = "select"
			// Extract table name and fields for SELECT
			p.extractSelectInfo(stmt, plan)
		case strings.HasPrefix(lowerStmt, "insert"):
			plan.Operation = "insert"
			// Extract table name for INSERT
			p.extractInsertInfo(stmt, plan)
		case strings.HasPrefix(lowerStmt, "update"):
			plan.Operation = "update"
			// Extract table name for UPDATE
			p.extractUpdateInfo(stmt, plan)
		case strings.HasPrefix(lowerStmt, "delete"):
			plan.Operation = "delete"
			// Extract table name for DELETE
			p.extractDeleteInfo(stmt, plan)
		case strings.HasPrefix(lowerStmt, "begin"), strings.HasPrefix(lowerStmt, "commit"), strings.HasPrefix(lowerStmt, "rollback"):
			plan.Operation = "transaction"
		case strings.HasPrefix(lowerStmt, "create"), strings.HasPrefix(lowerStmt, "drop"), strings.HasPrefix(lowerStmt, "alter"):
			plan.Operation = "ddl"
		default:
			plan.Operation = "unsupported"
		}
	case interface{}:
		// Handle pg_query.Node based statements from the professional parser
		// We need to check if this is a pg_query Node type
		switch parsed.Type {
		case SelectStatement:
			plan.Operation = "select"
			// Table name extraction would go here
			p.extractSelectInfoFromPG(stmt, plan)
		case InsertStatement:
			plan.Operation = "insert"
			// Table name extraction would go here
		case UpdateStatement:
			plan.Operation = "update"
			// Table name extraction would go here
		case DeleteStatement:
			plan.Operation = "delete"
			// Table name extraction would go here
		default:
			plan.Operation = "unsupported"
		}
	default:
		plan.Operation = "unsupported"
	}

	return plan, nil
}

// extractSelectInfo extracts table name, fields, and conditions for SELECT statements
func (p *Planner) extractSelectInfo(stmt string, plan *Plan) {
	// Extract fields and table
	selectRe := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM\s+(\w+)`)
	matches := selectRe.FindStringSubmatch(stmt)
	if len(matches) >= 3 {
		plan.Table = matches[2]
		// Parse the fields properly
		fields := strings.Split(matches[1], ",")
		for i, field := range fields {
			fields[i] = strings.TrimSpace(field)
		}
		plan.Fields = fields
	}

	// Extract WHERE conditions
	whereRe := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s*$)`)
	whereMatches := whereRe.FindStringSubmatch(stmt)
	if len(whereMatches) >= 2 {
		conditionsStr := whereMatches[1]
		// Parse simple conditions (field operator value)
		conditionRe := regexp.MustCompile(`(\w+)\s*(=|>|<|>=|<=|!=)\s*['"]?([^'"\s]+)['"]?`)
		conditionMatches := conditionRe.FindAllStringSubmatch(conditionsStr, -1)
		
		var conditions []Condition
		for _, match := range conditionMatches {
			if len(match) >= 4 {
				// Try to convert value to appropriate type
				var value interface{} = match[3]
				if i, err := strconv.ParseInt(match[3], 10, 64); err == nil {
					value = i
				} else if b, err := strconv.ParseBool(match[3]); err == nil {
					value = b
				}
				conditions = append(conditions, Condition{
					Field:    match[1],
					Operator: match[2],
					Value:    value,
				})
			}
		}
		plan.Conditions = conditions
	}

	// Extract ORDER BY
	orderRe := regexp.MustCompile(`(?i)ORDER\s+BY\s+([^,]+?)(?:\s+(ASC|DESC))?(?:\s+LIMIT|\s*$)`)
	orderMatches := orderRe.FindStringSubmatch(stmt)
	if len(orderMatches) >= 2 {
		orderField := strings.TrimSpace(orderMatches[1])
		order := "ASC"
		if len(orderMatches) >= 3 && strings.ToUpper(strings.TrimSpace(orderMatches[2])) == "DESC" {
			order = "DESC"
		}
		plan.OrderBy = []OrderBy{
			{
				Field: orderField,
				Order: order,
			},
		}
	}

	// Extract LIMIT
	limitRe := regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)
	limitMatches := limitRe.FindStringSubmatch(stmt)
	if len(limitMatches) >= 2 {
		if limit, err := strconv.ParseInt(limitMatches[1], 10, 64); err == nil {
			plan.Limit = &limit
		}
	}
}

// extractSelectInfoFromPG extracts information from PG parser nodes
func (p *Planner) extractSelectInfoFromPG(stmt interface{}, plan *Plan) {
	// For now, fall back to string-based extraction
	// In the future, this should properly parse pg_query nodes
	if stmtStr, ok := stmt.(string); ok {
		p.extractSelectInfo(stmtStr, plan)
	}
}

// extractInsertInfo extracts table name for INSERT statements
func (p *Planner) extractInsertInfo(stmt string, plan *Plan) {
	// Simple extraction logic for INSERT statements
	// This should be replaced with proper parsing in the future
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) >= 2 {
		plan.Table = matches[1]
	}
}

// extractUpdateInfo extracts table name for UPDATE statements
func (p *Planner) extractUpdateInfo(stmt string, plan *Plan) {
	// Simple extraction logic for UPDATE statements
	// This should be replaced with proper parsing in the future
	re := regexp.MustCompile(`(?i)UPDATE\s+(\w+)`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) >= 2 {
		plan.Table = matches[1]
	}
}

// extractDeleteInfo extracts table name for DELETE statements
func (p *Planner) extractDeleteInfo(stmt string, plan *Plan) {
	// Simple extraction logic for DELETE statements
	// This should be replaced with proper parsing in the future
	re := regexp.MustCompile(`(?i)DELETE\s+FROM\s+(\w+)`)
	matches := re.FindStringSubmatch(stmt)
	if len(matches) >= 2 {
		plan.Table = matches[1]
	}
}