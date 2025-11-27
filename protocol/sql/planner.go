package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/guileen/pglitedb/catalog"
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
	return &Planner{
		parser: parser,
	}
}

// NewPlannerWithCatalog creates a new query planner with catalog
func NewPlannerWithCatalog(parser Parser, catalogMgr catalog.Manager) *Planner {
	planner := &Planner{
		parser: parser,
	}
	// Create executor with this planner and catalog
	planner.executor = NewExecutorWithCatalog(planner, catalogMgr)
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
func (p *Planner) SetCatalog(catalogMgr catalog.Manager) {
	if p.executor == nil {
		// Create a new executor with catalog
		p.executor = NewExecutorWithCatalog(p, catalogMgr)
	} else {
		// Update existing executor with catalog
		p.executor.catalog = catalogMgr
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

		case strings.HasPrefix(lowerStmt, "insert"):
			plan.Operation = "insert"
			// Extract table name for INSERT

		case strings.HasPrefix(lowerStmt, "update"):
			plan.Operation = "update"
			// Extract table name for UPDATE

		case strings.HasPrefix(lowerStmt, "delete"):
			plan.Operation = "delete"
			// Extract table name for DELETE

		case strings.HasPrefix(lowerStmt, "begin"), strings.HasPrefix(lowerStmt, "commit"), strings.HasPrefix(lowerStmt, "rollback"):
			plan.Operation = "transaction"
		case strings.HasPrefix(lowerStmt, "create"), strings.HasPrefix(lowerStmt, "drop"), strings.HasPrefix(lowerStmt, "alter"):
			plan.Operation = "ddl"
		default:
			plan.Operation = "unsupported"
		}
	case *pg_query.Node:
		// Handle pg_query.Node based statements from the professional parser
		switch parsed.Type {
		case SelectStatement:
			plan.Operation = "select"
			// Extract table name and fields for SELECT from pg_query AST
			p.extractSelectInfoFromPGNode(stmt, plan)
		case InsertStatement:
			plan.Operation = "insert"
			// Extract table name for INSERT from pg_query AST
			p.extractInsertInfoFromPGNode(stmt, plan)
		case UpdateStatement:
			plan.Operation = "update"
			// Extract table name for UPDATE from pg_query AST
			p.extractUpdateInfoFromPGNode(stmt, plan)
		case DeleteStatement:
			plan.Operation = "delete"
			// Extract table name for DELETE from pg_query AST
			p.extractDeleteInfoFromPGNode(stmt, plan)
		default:
			plan.Operation = "unsupported"
		}
	case *ParsedQuery:
		// Handle ParsedQuery from our new AST parser
		plan.Table = stmt.Table
		plan.Fields = stmt.Fields
		plan.Conditions = stmt.Conditions
		plan.OrderBy = stmt.OrderBy
		plan.Limit = stmt.Limit
		
		switch stmt.Type {
		case SelectStatement:
			plan.Operation = "select"
		case InsertStatement:
			plan.Operation = "insert"
		case UpdateStatement:
			plan.Operation = "update"
		case DeleteStatement:
			plan.Operation = "delete"
		default:
			plan.Operation = "unsupported"
		}
	default:
		plan.Operation = "unsupported"
	}

	return plan, nil
}

// extractSelectInfoFromPGNode extracts table name, fields, and conditions for SELECT statements from pg_query AST
func (p *Planner) extractSelectInfoFromPGNode(stmt *pg_query.Node, plan *Plan) {
	// Implementation to extract information from pg_query.SelectStmt
	selectStmt := stmt.GetSelectStmt()
	if selectStmt == nil {
		return
	}
	
	// Extract table name from FROM clause
	if len(selectStmt.GetFromClause()) > 0 {
		fromClause := selectStmt.GetFromClause()[0]
		if rangeVar := fromClause.GetRangeVar(); rangeVar != nil {
			plan.Table = rangeVar.GetRelname()
		}
	}
	
	// Extract fields (target list)
	if targetList := selectStmt.GetTargetList(); targetList != nil {
		fields := make([]string, 0, len(targetList))
		for _, target := range targetList {
			if resTarget := target.GetResTarget(); resTarget != nil {
				if val := resTarget.GetVal(); val != nil {
					if columnRef := val.GetColumnRef(); columnRef != nil {
						// Extract column name from ColumnRef
						if len(columnRef.GetFields()) > 0 {
							if str := columnRef.GetFields()[len(columnRef.GetFields())-1].GetString_(); str != nil {
								fields = append(fields, str.GetSval())
							}
						}
					} else if aConst := val.GetAConst(); aConst != nil {
						// Handle constants like '*'
						fields = append(fields, "*")
					}
				}
			}
		}
		plan.Fields = fields
	}
	
	// Extract WHERE conditions
	if whereClause := selectStmt.GetWhereClause(); whereClause != nil {
		// Extract simple equality conditions
		conditions := p.extractConditionsFromExpr(whereClause)
		plan.Conditions = conditions
	}
	
	// Extract ORDER BY
	if sortClause := selectStmt.GetSortClause(); sortClause != nil {
		orderBy := make([]OrderBy, 0, len(sortClause))
		for _, sortBy := range sortClause {
			if sortNode := sortBy.GetSortBy(); sortNode != nil {
				var field string
				// Extract field name from sort expression
				if node := sortNode.GetNode(); node != nil {
					if columnRef := node.GetColumnRef(); columnRef != nil {
						if len(columnRef.GetFields()) > 0 {
							if str := columnRef.GetFields()[len(columnRef.GetFields())-1].GetString_(); str != nil {
								field = str.GetSval()
							}
						}
					}
				}
				
				// Determine sort order
				order := "ASC"
				if sortNode.GetSortbyDir() == pg_query.SortByDir_SORTBY_DESC {
					order = "DESC"
				}
				
				if field != "" {
					orderBy = append(orderBy, OrderBy{
						Field: field,
						Order: order,
					})
				}
			}
		}
		plan.OrderBy = orderBy
	}
	
	// Extract LIMIT
	if limitCount := selectStmt.GetLimitCount(); limitCount != nil {
		if aConst := limitCount.GetAConst(); aConst != nil {
			if iConst := aConst.GetIval(); iConst != nil {
				limit := int64(iConst.GetIval())
				plan.Limit = &limit
			}
		}
	}
}

// extractConditionsFromExpr extracts simple conditions from a pg_query expression
func (p *Planner) extractConditionsFromExpr(expr *pg_query.Node) []Condition {
	// Pre-allocate with reasonable capacity to reduce reallocations
	conditions := make([]Condition, 0, 4)
	
	if expr == nil {
		return conditions
	}
	
	// Handle A_Expr (arithmetic expressions like =, >, <, etc.)
	if aExpr := expr.GetAExpr(); aExpr != nil {
		// Check if this is a simple equality condition
		if aExpr.GetKind() == pg_query.A_Expr_Kind_AEXPR_OP {
			// Get the operator name
			var opName string
			if nameParts := aExpr.GetName(); len(nameParts) > 0 {
				if str := nameParts[0].GetString_(); str != nil {
					opName = str.GetSval()
				}
			}
			
			// Get left side (should be a column reference)
			if left := aExpr.GetLexpr(); left != nil {
				// Get right side (should be a constant)
				if right := aExpr.GetRexpr(); right != nil {
					// Extract column name from left side
					var columnName string
					if columnRef := left.GetColumnRef(); columnRef != nil {
						if fields := columnRef.GetFields(); len(fields) > 0 {
							if str := fields[len(fields)-1].GetString_(); str != nil {
								columnName = str.GetSval()
							}
						}
					}
					
					// Extract value from right side
					var value interface{}
					if aConst := right.GetAConst(); aConst != nil {
						switch {
						case aConst.GetSval() != nil:
							value = aConst.GetSval().GetSval()
						case aConst.GetIval() != nil:
							value = aConst.GetIval().GetIval()
						case aConst.GetFval() != nil:
							if f, err := strconv.ParseFloat(aConst.GetFval().GetFval(), 64); err == nil {
								value = f
							}
						case aConst.GetBoolval() != nil:
							value = aConst.GetBoolval().GetBoolval()
						}
					}
					
					// Only add condition if we have both field and operator
					if columnName != "" && opName != "" {
						conditions = append(conditions, Condition{
							Field:    columnName,
							Operator: opName,
							Value:    value,
						})
					}
				}
			}
		}
	} else if boolExpr := expr.GetBoolExpr(); boolExpr != nil {
		// Handle Boolean expressions (AND, OR)
		if boolExpr.GetBoolop() == pg_query.BoolExprType_AND_EXPR {
			// Extract conditions from each operand with pre-allocation
			args := boolExpr.GetArgs()
			// Pre-size the conditions slice to reduce reallocations
			totalCap := cap(conditions) + len(args)*2
			if totalCap > cap(conditions) {
				newConditions := make([]Condition, len(conditions), totalCap)
				copy(newConditions, conditions)
				conditions = newConditions
			}
			
			// Extract conditions from each operand
			for _, operand := range args {
				subConditions := p.extractConditionsFromExpr(operand)
				conditions = append(conditions, subConditions...)
			}
		}
	}
	
	return conditions
}

// extractInsertInfoFromPGNode extracts table name for INSERT statements from pg_query AST
func (p *Planner) extractInsertInfoFromPGNode(stmt *pg_query.Node, plan *Plan) {
	insertStmt := stmt.GetInsertStmt()
	if insertStmt == nil {
		return
	}
	
	// Extract table name
	if relation := insertStmt.GetRelation(); relation != nil {
		plan.Table = relation.GetRelname()
	}
}

// extractUpdateInfoFromPGNode extracts table name for UPDATE statements from pg_query AST
func (p *Planner) extractUpdateInfoFromPGNode(stmt *pg_query.Node, plan *Plan) {
	updateStmt := stmt.GetUpdateStmt()
	if updateStmt == nil {
		return
	}
	
	// Extract table name
	if relation := updateStmt.GetRelation(); relation != nil {
		plan.Table = relation.GetRelname()
	}
}

// extractDeleteInfoFromPGNode extracts table name for DELETE statements from pg_query AST
func (p *Planner) extractDeleteInfoFromPGNode(stmt *pg_query.Node, plan *Plan) {
	deleteStmt := stmt.GetDeleteStmt()
	if deleteStmt == nil {
		return
	}
	
	// Extract table name
	if relation := deleteStmt.GetRelation(); relation != nil {
		plan.Table = relation.GetRelname()
	}
}