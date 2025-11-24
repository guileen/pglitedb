package sql

import (
	"context"
	"fmt"

	"github.com/xwb1989/sqlparser"
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

	switch stmt := parsed.Statement.(type) {
	case *sqlparser.Select:
		if err := p.planSelect(stmt, plan); err != nil {
			return nil, fmt.Errorf("failed to plan SELECT statement: %w", err)
		}
	case *sqlparser.DDL:
		plan.Operation = "ddl"
	default:
		plan.Operation = "unsupported"
	}

	return plan, nil
}

// planSelect creates a plan for a SELECT statement
func (p *Planner) planSelect(stmt *sqlparser.Select, plan *Plan) error {
	plan.Operation = "select"

	// Extract table name
	if len(stmt.From) > 0 {
		if aliasedTable, ok := stmt.From[0].(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := aliasedTable.Expr.(sqlparser.TableName); ok {
				plan.Table = tableName.Name.String()
			}
		}
	}

	// Extract fields
	for _, expr := range stmt.SelectExprs {
		if aliasedExpr, ok := expr.(*sqlparser.AliasedExpr); ok {
			if colName, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok {
				plan.Fields = append(plan.Fields, colName.Name.String())
			}
		}
	}

	// Extract conditions
	if stmt.Where != nil {
		p.extractConditions(stmt.Where.Expr, &plan.Conditions)
	}

	// Extract LIMIT
	if stmt.Limit != nil {
		if val, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); ok {
			if val.Type == sqlparser.IntVal {
				limit := int64(0)
				fmt.Sscanf(string(val.Val), "%d", &limit)
				plan.Limit = &limit
			}
		}
	}

	// Extract ORDER BY
	for _, orderBy := range stmt.OrderBy {
		order := "ASC"
		if orderBy.Direction == sqlparser.DescScr {
			order = "DESC"
		}
		plan.OrderBy = append(plan.OrderBy, OrderBy{
			Field: orderBy.Expr.(*sqlparser.ColName).Name.String(),
			Order: order,
		})
	}

	return nil
}

// extractConditions extracts WHERE clause conditions
func (p *Planner) extractConditions(expr sqlparser.Expr, conditions *[]Condition) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if colName, ok := e.Left.(*sqlparser.ColName); ok {
			field := colName.Name.String()
			operator := e.Operator
			var value interface{}

			if val, ok := e.Right.(*sqlparser.SQLVal); ok {
				value = string(val.Val)
			}

			*conditions = append(*conditions, Condition{
				Field:    field,
				Operator: operator,
				Value:    value,
			})
		}
	case *sqlparser.AndExpr:
		p.extractConditions(e.Left, conditions)
		p.extractConditions(e.Right, conditions)
	case *sqlparser.OrExpr:
		// For simplicity, we're treating OR expressions similarly to AND
		// In a production system, this would need more sophisticated handling
		p.extractConditions(e.Left, conditions)
		p.extractConditions(e.Right, conditions)
	}
}