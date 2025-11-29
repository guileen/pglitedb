package sql

import (
	"strings"
	
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// extractSelectInfoFromPGNode extracts SELECT statement information from a PG query node
func (p *Planner) extractSelectInfoFromPGNode(stmt *pg_query.ParseResult, plan *Plan) {
	stmtNode := stmt.Stmts[0].GetStmt()
	selectStmt := stmtNode.GetSelectStmt()
	if selectStmt == nil {
		return
	}

	// Extract table names from FROM clause
	if fromClause := selectStmt.GetFromClause(); len(fromClause) > 0 {
		for _, fromItem := range fromClause {
			if rangeVar := fromItem.GetRangeVar(); rangeVar != nil {
				plan.Table = rangeVar.GetRelname()
				break // For simplicity, take the first table
			}
		}
	}

	// Extract target fields
	if targetList := selectStmt.GetTargetList(); len(targetList) > 0 {
		fields := make([]string, 0, len(targetList))
		aggregates := make([]Aggregate, 0)

		for _, target := range targetList {
			if targetEntry := target.GetResTarget(); targetEntry != nil {
				// Extract alias if available
				alias := targetEntry.GetName()
				
				if val := targetEntry.GetVal(); val != nil {
					// Handle aggregate functions
					if funcCall := val.GetFuncCall(); funcCall != nil {
						agg := p.extractAggregateFunction(funcCall)
						if agg.Function != "" {
							// Set alias if available
							if alias != "" {
								agg.Alias = alias
							}
							aggregates = append(aggregates, agg)
							// Add the aggregate function to fields with "func:" prefix
							fields = append(fields, "func:"+strings.ToLower(agg.Function))
							continue
						}
					}

					// Handle regular column references
					if columnRef := val.GetColumnRef(); columnRef != nil {
						if fieldsList := columnRef.GetFields(); len(fieldsList) > 0 {
							if str := fieldsList[len(fieldsList)-1].GetString_(); str != nil {
								fields = append(fields, str.GetSval())
							}
						}
					} else if val.GetAConst() != nil {
						// Handle constant values (SELECT 1, 'hello', etc.)
						fields = append(fields, "*constant*")
					}
				}
			}
		}

		plan.Fields = fields
		plan.Aggregates = aggregates
	}

	// Extract WHERE conditions
	if whereClause := selectStmt.GetWhereClause(); whereClause != nil {
		conditions := p.extractConditionsFromExpr(whereClause)
		plan.Conditions = conditions
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
		plan.OrderBy = orderBy
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

// extractAggregateFunction extracts information about an aggregate function
func (p *Planner) extractAggregateFunction(funcCall *pg_query.FuncCall) Aggregate {
	agg := Aggregate{}

	if funcName := funcCall.GetFuncname(); len(funcName) > 0 {
		if str := funcName[0].GetString_(); str != nil {
			agg.Function = strings.ToUpper(str.GetSval())
		}
	}

	if agg.Function != "" {
		// Handle COUNT(*) case - no arguments
		if funcCall.GetArgs() == nil {
			// This is COUNT(*) or similar
			agg.Field = "*"
		} else {
			// Handle regular aggregate functions with arguments
			args := funcCall.GetArgs()
			if len(args) > 0 {
				if arg := args[0]; arg != nil {
					if columnRef := arg.GetColumnRef(); columnRef != nil {
						if fieldsList := columnRef.GetFields(); len(fieldsList) > 0 {
							if str := fieldsList[len(fieldsList)-1].GetString_(); str != nil {
								agg.Field = str.GetSval()
							}
						}
					} else if aStar := arg.GetAStar(); aStar != nil {
						// Handle explicit * argument
						agg.Field = "*"
					}
				}
			}
		}
	}

	// Extract alias if available
	// Note: The alias is typically set on the ResTarget that contains this FuncCall
	
	return agg
}