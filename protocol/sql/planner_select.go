package sql

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/guileen/pglitedb/protocol/sql/parser"
)

// extractSelectInfoFromPGNode extracts SELECT statement information from a PG query node
func (p *Planner) extractSelectInfoFromPGNode(stmt *pg_query.ParseResult, plan *Plan) {
	stmtNode := stmt.Stmts[0].GetStmt()
	selectStmt := stmtNode.GetSelectStmt()
	if selectStmt == nil {
		return
	}

	// Extract table names from FROM clause (including subqueries)
	if fromClause := selectStmt.GetFromClause(); len(fromClause) > 0 {
		subqueries := make([]Subquery, 0)
		for _, fromItem := range fromClause {
			if rangeVar := fromItem.GetRangeVar(); rangeVar != nil {
				plan.Table = rangeVar.GetRelname()
			} else if subLink := fromItem.GetSubLink(); subLink != nil {
				// Handle subqueries in FROM clause
				if subqueryPlan := p.extractSubquery(subLink); subqueryPlan != nil {
					subqueries = append(subqueries, *subqueryPlan)
				}
			}
		}
		if len(subqueries) > 0 {
			plan.Subqueries = []parser.Subquery{}
			for _, sq := range subqueries {
				plan.Subqueries = append(plan.Subqueries, parser.Subquery{
					Query: sq.Query,
					Alias: sq.Alias,
				})
			}
		}
	}

	// Extract target fields with enhanced support for complex expressions
	if targetList := selectStmt.GetTargetList(); len(targetList) > 0 {
		fields := make([]string, 0, len(targetList))
		aggregates := make([]Aggregate, 0)
		caseExpressions := make([]CaseExpression, 0)
		windowFunctions := make([]WindowFunction, 0)

		for _, target := range targetList {
			if targetEntry := target.GetResTarget(); targetEntry != nil {
				alias := targetEntry.GetName()
				
				if val := targetEntry.GetVal(); val != nil {
					// Handle aggregate functions
					if funcCall := val.GetFuncCall(); funcCall != nil {
						agg := p.extractAggregateFunction(funcCall)
						if agg.Function != "" {
							if alias != "" {
								agg.Alias = alias
							}
							aggregates = append(aggregates, agg)
							fields = append(fields, "func:"+strings.ToLower(agg.Function))
							continue
						}
					}
					
					// Handle CASE expressions
					if caseExpr := val.GetCaseExpr(); caseExpr != nil {
						caseExp := p.extractCaseExpression(caseExpr)
						if caseExp != nil {
							if alias != "" {
								caseExp.Alias = alias
							}
							caseExpressions = append(caseExpressions, *caseExp)
							fields = append(fields, "case:"+alias)
							continue
						}
					}
					
					// Handle column references
					if columnRef := val.GetColumnRef(); columnRef != nil {
						if fieldsList := columnRef.GetFields(); len(fieldsList) > 0 {
							if str := fieldsList[len(fieldsList)-1].GetString_(); str != nil {
								fields = append(fields, str.GetSval())
							}
						}
					} else if val.GetAConst() != nil {
						fields = append(fields, "*constant*")
					}
				}
			}
		}

		plan.Fields = fields
		plan.Aggregates = aggregates
		if len(caseExpressions) > 0 {
			plan.CaseExpressions = caseExpressions
		}
		if len(windowFunctions) > 0 {
			// Convert local WindowFunction slice to parser.WindowFunction slice
			parserWindowFunctions := make([]parser.WindowFunction, len(windowFunctions))
			for i, wf := range windowFunctions {
				// Convert OrderBy slice to parser.OrderBy slice
				parserOrderBy := make([]parser.OrderBy, len(wf.OrderBy))
				for j, ob := range wf.OrderBy {
					parserOrderBy[j] = parser.OrderBy{
						Field:      ob.Field,
						Direction:  ob.Order,
						NullsOrder: "", // Default value
					}
				}
				parserWindowFunctions[i] = parser.WindowFunction{
					Function:    wf.Function,
					Arguments:   wf.Arguments,
					PartitionBy: wf.PartitionBy,
					OrderBy:     parserOrderBy,
					FrameClause: wf.FrameClause,
					Alias:       wf.Alias,
				}
			}
			plan.WindowFunctions = parserWindowFunctions
		}
	}

	// Extract WHERE conditions with subquery support
	if whereClause := selectStmt.GetWhereClause(); whereClause != nil {
		conditions := p.extractConditionsFromExpr(whereClause)
		// Convert local Condition slice to parser.Condition slice
		parserConditions := make([]parser.Condition, len(conditions))
		for i, cond := range conditions {
			// Type assert cond.Value to string
			valueStr, ok := cond.Value.(string)
			if !ok {
				// Convert to string if it's not already a string
				valueStr = fmt.Sprintf("%v", cond.Value)
			}
			parserConditions[i] = parser.Condition{
				Field:    cond.Field,
				Operator: cond.Operator,
				Value:    valueStr,
			}
		}
		plan.Conditions = parserConditions
	}

	// Extract LIMIT
	if limitCount := selectStmt.GetLimitCount(); limitCount != nil {
		if aConst := limitCount.GetAConst(); aConst != nil {
			if i := aConst.GetIval(); i != nil {
				limit := int64(i.GetIval())
				plan.Limit = &limit
			}
		}
	}

	// Extract OFFSET
	if limitOffset := selectStmt.GetLimitOffset(); limitOffset != nil {
		if aConst := limitOffset.GetAConst(); aConst != nil {
			if i := aConst.GetIval(); i != nil {
				offset := int64(i.GetIval())
				plan.Offset = &offset
			}
		}
	}

	// Extract ORDER BY
	if sortClause := selectStmt.GetSortClause(); len(sortClause) > 0 {
		orderBy := make([]OrderBy, 0, len(sortClause))
		for _, sortBy := range sortClause {
			if sort := sortBy.GetSortBy(); sort != nil {
				ob := OrderBy{}
				if node := sort.GetNode(); node != nil {
					if columnRef := node.GetColumnRef(); columnRef != nil {
						if fieldsList := columnRef.GetFields(); len(fieldsList) > 0 {
							if str := fieldsList[len(fieldsList)-1].GetString_(); str != nil {
								ob.Field = str.GetSval()
							}
						}
					}
				}
				// Default to ascending order
				ob.Order = "ASC"
				if sort.GetSortbyDir() == pg_query.SortByDir_SORTBY_DESC {
					ob.Order = "DESC"
				}
				orderBy = append(orderBy, ob)
			}
		}
		// Convert local OrderBy slice to parser.OrderBy slice
		parserOrderBy := make([]parser.OrderBy, len(orderBy))
		for i, ob := range orderBy {
			parserOrderBy[i] = parser.OrderBy{
				Field:      ob.Field,
				Direction:  ob.Order,
				NullsOrder: "", // Default value
			}
		}
		plan.OrderBy = parserOrderBy
	}

	// Extract GROUP BY
	if groupClause := selectStmt.GetGroupClause(); len(groupClause) > 0 {
		groupBy := make([]string, 0, len(groupClause))
		for _, group := range groupClause {
			if columnRef := group.GetColumnRef(); columnRef != nil {
				if fieldsList := columnRef.GetFields(); len(fieldsList) > 0 {
					if str := fieldsList[len(fieldsList)-1].GetString_(); str != nil {
						groupBy = append(groupBy, str.GetSval())
					}
				}
			}
		}
		plan.GroupBy = groupBy
	}
}

// extractSubquery extracts information about a subquery
func (p *Planner) extractSubquery(subLink *pg_query.SubLink) *Subquery {
	if subLink == nil {
		return nil
	}
	
	subquery := &Subquery{
		Type: TableSubquery, // Default assumption
	}
	
	// Determine subquery type
	switch subLink.GetSubLinkType() {
	case pg_query.SubLinkType_EXISTS_SUBLINK:
		subquery.Type = ScalarSubquery
	case pg_query.SubLinkType_ALL_SUBLINK:
		subquery.Type = ScalarSubquery
	case pg_query.SubLinkType_ANY_SUBLINK:
		subquery.Type = ScalarSubquery
	case pg_query.SubLinkType_ROWCOMPARE_SUBLINK:
		subquery.Type = RowSubquery
	case pg_query.SubLinkType_EXPR_SUBLINK:
		subquery.Type = ScalarSubquery
	case pg_query.SubLinkType_MULTIEXPR_SUBLINK:
		subquery.Type = RowSubquery
	case pg_query.SubLinkType_ARRAY_SUBLINK:
		subquery.Type = TableSubquery
	case pg_query.SubLinkType_CTE_SUBLINK:
		// CTE handling would go here
	}
	
	// Extract the subquery statement
	if subSelect := subLink.GetSubselect(); subSelect != nil {
		// Convert the subquery node back to SQL text or extract key information
		// This is a simplified approach - in practice you might serialize the AST
		subquery.Query = "subquery_placeholder" // Would be replaced with actual serialization
		
		// Check for correlation
		subquery.Correlated = p.isCorrelatedSubquery(subLink)
	}
	
	// Extract alias if available
	if subLink.GetTestexpr() != nil {
		// Extract alias information from test expression
	}
	
	return subquery
}

// isCorrelatedSubquery determines if a subquery is correlated
func (p *Planner) isCorrelatedSubquery(subLink *pg_query.SubLink) bool {
	// This would analyze the subquery to see if it references columns
	// from the outer query - simplified implementation
	return false
}

// extractAggregateFunction extracts information about an aggregate function with enhanced support
func (p *Planner) extractAggregateFunction(funcCall *pg_query.FuncCall) Aggregate {
	agg := Aggregate{
		Distinct: funcCall.GetAggDistinct(),
	}

	if funcName := funcCall.GetFuncname(); len(funcName) > 0 {
		if str := funcName[0].GetString_(); str != nil {
			agg.Function = strings.ToUpper(str.GetSval())
		}
	}

	if agg.Function != "" {
		// Handle arguments
		args := funcCall.GetArgs()
		if len(args) > 0 {
			agg.Arguments = make([]string, len(args))
			for i, arg := range args {
				if arg != nil {
					agg.Arguments[i] = p.exprToString(arg)
				}
			}
			
			// For backward compatibility, set Field to first argument
			if len(agg.Arguments) > 0 {
				agg.Field = agg.Arguments[0]
			}
		} else {
			// Handle COUNT(*) case
			agg.Field = "*"
		}
		
		// Extract FILTER clause if present
		if filter := funcCall.GetAggFilter(); filter != nil {
			agg.Filters = p.extractConditionsFromExpr(filter)
		}
	}

	return agg
}

// exprToString converts a pg_query expression node to a string representation
func (p *Planner) exprToString(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	
	// This would be a more complete implementation that serializes
	// various expression types back to SQL-like strings
	// For now, a simplified version:
	
	if columnRef := node.GetColumnRef(); columnRef != nil {
		if fields := columnRef.GetFields(); len(fields) > 0 {
			if str := fields[len(fields)-1].GetString_(); str != nil {
				return str.GetSval()
			}
		}
	} else if aConst := node.GetAConst(); aConst != nil {
		// Handle constants
		if i := aConst.GetIval(); i != nil {
			return fmt.Sprintf("%d", i.GetIval())
		} else if f := aConst.GetFval(); f != nil {
			return f.GetFval()
		} else if s := aConst.GetSval(); s != nil {
			return fmt.Sprintf("'%s'", s.GetSval())
		}
	}
	
	return "expression"
}

// extractCaseExpression extracts information about a CASE expression
func (p *Planner) extractCaseExpression(caseExpr *pg_query.CaseExpr) *CaseExpression {
	if caseExpr == nil {
		return nil
	}
	
	caseExp := &CaseExpression{}
	
	// Extract CASE conditions
	if args := caseExpr.GetArgs(); len(args) > 0 {
		caseExp.Conditions = make([]CaseCondition, len(args))
		for i, arg := range args {
			if caseWhen := arg.GetCaseWhen(); caseWhen != nil {
				condition := ""
				result := ""
				
				if expr := caseWhen.GetExpr(); expr != nil {
					condition = p.exprToString(expr)
				}
				
				if resultExpr := caseWhen.GetResult(); resultExpr != nil {
					result = p.exprToString(resultExpr)
				}
				
				caseExp.Conditions[i] = CaseCondition{
					Condition: condition,
					Result:    result,
				}
			}
		}
	}
	
	// Extract ELSE clause
	if defResult := caseExpr.GetDefresult(); defResult != nil {
		caseExp.ElseValue = p.exprToString(defResult)
	}
	
	return caseExp
}