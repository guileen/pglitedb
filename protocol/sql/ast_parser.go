package sql

import (
	"fmt"
	"strconv"
	
	"github.com/pganalyze/pg_query_go/v6"
)

// ASTParser is a professional SQL parser based on PostgreSQL's parser
type ASTParser struct{}

// NewASTParser creates a new AST-based SQL parser
func NewASTParser() *ASTParser {
	return &ASTParser{}
}

// Parse parses a SQL query string and returns a parsed query structure
func (p *ASTParser) Parse(query string) (*ParsedQuery, error) {
	result, err := pg_query.Parse(query)
	if err != nil {
		return nil, err
	}
	
	parsed := &ParsedQuery{
		Statement: result,
		Query:     query,
		Type:      p.determineStatementType(result),
	}
	
	// Extract details based on AST
	p.extractDetails(result, parsed)
	
	return parsed, nil
}

// determineStatementType determines the type of SQL statement from the parse result
func (p *ASTParser) determineStatementType(result *pg_query.ParseResult) StatementType {
	if len(result.Stmts) == 0 {
		return UnknownStatement
	}
	
	stmt := result.Stmts[0].Stmt
	switch stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		return SelectStatement
	case *pg_query.Node_InsertStmt:
		return InsertStatement
	case *pg_query.Node_UpdateStmt:
		return UpdateStatement
	case *pg_query.Node_DeleteStmt:
		return DeleteStatement
	default:
		return UnknownStatement
	}
}

// extractDetails extracts detailed information from the parse result based on AST
func (p *ASTParser) extractDetails(result *pg_query.ParseResult, parsed *ParsedQuery) {
	if len(result.Stmts) == 0 {
		return
	}
	
	stmt := result.Stmts[0].Stmt
	switch node := stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		p.extractSelectDetails(node.SelectStmt, parsed)
	case *pg_query.Node_InsertStmt:
		p.extractInsertDetails(node.InsertStmt, parsed)
	case *pg_query.Node_UpdateStmt:
		p.extractUpdateDetails(node.UpdateStmt, parsed)
	case *pg_query.Node_DeleteStmt:
		p.extractDeleteDetails(node.DeleteStmt, parsed)
	}
}

// extractSelectDetails extracts details from a SELECT statement
func (p *ASTParser) extractSelectDetails(stmt *pg_query.SelectStmt, parsed *ParsedQuery) {
	// Extract table name from FROM clause
	if len(stmt.GetFromClause()) > 0 {
		fromClause := stmt.GetFromClause()[0]
		if rangeVar := fromClause.GetRangeVar(); rangeVar != nil {
			parsed.Table = rangeVar.GetRelname()
		}
	}
	
	// Extract fields (target list)
	if targetList := stmt.GetTargetList(); targetList != nil {
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
		parsed.Fields = fields
	}
	
	// Extract WHERE conditions
	if whereClause := stmt.GetWhereClause(); whereClause != nil {
		// Extract simple equality conditions
		conditions := p.extractConditionsFromExpr(whereClause)
		parsed.Conditions = conditions
	}
	
	// Extract ORDER BY
	if sortClause := stmt.GetSortClause(); sortClause != nil {
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
		parsed.OrderBy = orderBy
	}
	
	// Extract LIMIT
	if limitCount := stmt.GetLimitCount(); limitCount != nil {
		if aConst := limitCount.GetAConst(); aConst != nil {
			if iConst := aConst.GetIval(); iConst != nil {
				limit := int64(iConst.GetIval())
				parsed.Limit = &limit
			}
		}
	}
}

// extractConditionsFromExpr extracts simple conditions from a pg_query expression
func (p *ASTParser) extractConditionsFromExpr(expr *pg_query.Node) []Condition {
	var conditions []Condition
	
	if expr == nil {
		return conditions
	}
	
	// Handle A_Expr (arithmetic expressions like =, >, <, etc.)
	if aExpr := expr.GetAExpr(); aExpr != nil {
		// Check if this is a simple equality condition
		if aExpr.GetKind() == pg_query.A_Expr_Kind_AEXPR_OP {
			// Get the operator name
			var opName string
			if len(aExpr.GetName()) > 0 {
				if str := aExpr.GetName()[0].GetString_(); str != nil {
					opName = str.GetSval()
				}
			}
			
			// Get left side (should be a column reference)
			left := aExpr.GetLexpr()
			// Get right side (should be a constant)
			right := aExpr.GetRexpr()
			
			if left != nil && right != nil {
				// Extract column name from left side
				var columnName string
				if columnRef := left.GetColumnRef(); columnRef != nil {
					if len(columnRef.GetFields()) > 0 {
						if str := columnRef.GetFields()[len(columnRef.GetFields())-1].GetString_(); str != nil {
							columnName = str.GetSval()
						}
					}
				}
				
				// Extract value from right side
				var value interface{}
				if aConst := right.GetAConst(); aConst != nil {
					if aStr := aConst.GetSval(); aStr != nil {
						value = aStr.GetSval()
					} else if aInt := aConst.GetIval(); aInt != nil {
						value = aInt.GetIval()
					} else if aBool := aConst.GetBoolval(); aBool != nil {
						value = aBool.GetBoolval()
					}
				} else if paramRef := right.GetParamRef(); paramRef != nil {
					// Handle parameter references in conditions
					value = fmt.Sprintf("$%d", paramRef.GetNumber())
				}
				
				if columnName != "" && opName != "" {
					conditions = append(conditions, Condition{
						Field:    columnName,
						Operator: opName,
						Value:    value,
					})
				}
			}
		}
	} else if boolExpr := expr.GetBoolExpr(); boolExpr != nil {
		// Handle Boolean expressions (AND, OR)
		if boolExpr.GetBoolop() == pg_query.BoolExprType_AND_EXPR {
			// Extract conditions from each operand
			for _, operand := range boolExpr.GetArgs() {
				subConditions := p.extractConditionsFromExpr(operand)
				conditions = append(conditions, subConditions...)
			}
		}
	}
	
	return conditions
}

// extractInsertDetails extracts details from an INSERT statement
func (p *ASTParser) extractInsertDetails(stmt *pg_query.InsertStmt, parsed *ParsedQuery) {
	// Extract table name
	if relation := stmt.GetRelation(); relation != nil {
		parsed.Table = relation.GetRelname()
	}
}

// extractUpdateDetails extracts details from an UPDATE statement
func (p *ASTParser) extractUpdateDetails(stmt *pg_query.UpdateStmt, parsed *ParsedQuery) {
	// Extract table name
	if relation := stmt.GetRelation(); relation != nil {
		parsed.Table = relation.GetRelname()
	}
	
	// Extract SET values
	if targetList := stmt.GetTargetList(); targetList != nil {
		updates := make(map[string]interface{})
		for _, target := range targetList {
			if resTarget := target.GetResTarget(); resTarget != nil {
				fieldName := resTarget.GetName()
				if val := resTarget.GetVal(); val != nil {
					// For parameter references, we'll store them as placeholders
					// Actual parameter binding happens later in execution
					if paramRef := val.GetParamRef(); paramRef != nil {
						updates[fieldName] = fmt.Sprintf("$%d", paramRef.GetNumber())
					} else if aConst := val.GetAConst(); aConst != nil {
						// Extract constant values
						switch {
						case aConst.GetSval() != nil:
							updates[fieldName] = aConst.GetSval().GetSval()
						case aConst.GetIval() != nil:
							updates[fieldName] = aConst.GetIval().GetIval()
						case aConst.GetFval() != nil:
							if f, err := strconv.ParseFloat(aConst.GetFval().GetFval(), 64); err == nil {
								updates[fieldName] = f
							}
						case aConst.GetBoolval() != nil:
							updates[fieldName] = aConst.GetBoolval().GetBoolval()
						}
					}
				}
			}
		}
		parsed.Updates = updates
	}
	
	// Extract WHERE conditions
	if whereClause := stmt.GetWhereClause(); whereClause != nil {
		conditions := p.extractConditionsFromExpr(whereClause)
		parsed.Conditions = conditions
	}
}

// extractDeleteDetails extracts details from a DELETE statement
func (p *ASTParser) extractDeleteDetails(stmt *pg_query.DeleteStmt, parsed *ParsedQuery) {
	// Extract table name
	if relation := stmt.GetRelation(); relation != nil {
		parsed.Table = relation.GetRelname()
	}
	
	// Extract WHERE conditions
	if whereClause := stmt.GetWhereClause(); whereClause != nil {
		conditions := p.extractConditionsFromExpr(whereClause)
		parsed.Conditions = conditions
	}
}