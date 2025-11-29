package sql

import (
	"fmt"
	"strconv"
	
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// extractConditionsFromExpr extracts WHERE clause conditions from an expression
func (p *Planner) extractConditionsFromExpr(expr *pg_query.Node) []Condition {
	conditions := make([]Condition, 0)

	if expr == nil {
		return conditions
	}

	// Handle A_Expr (arithmetic expressions like =, >, <, etc.)
	if aExpr := expr.GetAExpr(); aExpr != nil {
		condition := Condition{}
		
		// Extract operator
		if nameList := aExpr.GetName(); len(nameList) > 0 {
			if str := nameList[0].GetString_(); str != nil {
				condition.Operator = str.GetSval()
			}
		}

		// Extract left operand (usually the field name)
		if lexpr := aExpr.GetLexpr(); lexpr != nil {
			if columnRef := lexpr.GetColumnRef(); columnRef != nil {
				if fieldsList := columnRef.GetFields(); len(fieldsList) > 0 {
					if str := fieldsList[len(fieldsList)-1].GetString_(); str != nil {
						condition.Field = str.GetSval()
					}
				}
			}
		}

		// Extract right operand (usually the value)
		if rexpr := aExpr.GetRexpr(); rexpr != nil {
			if aConst := rexpr.GetAConst(); aConst != nil {
				if i := aConst.GetIval(); i != nil {
					condition.Value = i.GetIval()
				} else if f := aConst.GetFval(); f != nil {
					if val, err := strconv.ParseFloat(f.GetFval(), 64); err == nil {
						condition.Value = val
					}
				} else if s := aConst.GetSval(); s != nil {
					condition.Value = s.GetSval()
				}
			} else if paramRef := rexpr.GetParamRef(); paramRef != nil {
				// Handle parameter placeholders like $1, $2, etc.
				condition.Value = fmt.Sprintf("$%d", paramRef.GetNumber())
			}
		}

		if condition.Field != "" && condition.Operator != "" {
			conditions = append(conditions, condition)
		}
	}

	// Handle BoolExpr (AND, OR conditions)
	if boolExpr := expr.GetBoolExpr(); boolExpr != nil {
		if args := boolExpr.GetArgs(); len(args) > 0 {
			for _, arg := range args {
				subConditions := p.extractConditionsFromExpr(arg)
				conditions = append(conditions, subConditions...)
			}
		}
	}

	return conditions
}

// extractUpdateInfoFromPGNode extracts UPDATE statement information from a PG query node
func (p *Planner) extractUpdateInfoFromPGNode(stmt *pg_query.Node, plan *Plan) {
	updateStmt := stmt.GetUpdateStmt()
	if updateStmt == nil {
		return
	}

	// Extract table name
	if relation := updateStmt.GetRelation(); relation != nil {
		plan.Table = relation.GetRelname()
	}

	// Extract SET clauses
	if targetList := updateStmt.GetTargetList(); len(targetList) > 0 {
		updates := make(map[string]interface{})
		for _, target := range targetList {
			if targetEntry := target.GetResTarget(); targetEntry != nil {
				fieldName := targetEntry.GetName()
				if val := targetEntry.GetVal(); val != nil {
					if aConst := val.GetAConst(); aConst != nil {
						if i := aConst.GetIval(); i != nil {
							updates[fieldName] = i.GetIval()
						} else if f := aConst.GetFval(); f != nil {
							if val, err := strconv.ParseFloat(f.GetFval(), 64); err == nil {
								updates[fieldName] = val
							}
						} else if s := aConst.GetSval(); s != nil {
							updates[fieldName] = s.GetSval()
						} else if b := aConst.GetBoolval(); b != nil {
							updates[fieldName] = b.GetBoolval()
						}
					} else if paramRef := val.GetParamRef(); paramRef != nil {
						// Handle parameter placeholders like $1, $2, etc.
						updates[fieldName] = fmt.Sprintf("$%d", paramRef.GetNumber())
					}
				}
			}
		}
		plan.Updates = updates
	}

	// Extract WHERE conditions
	if whereClause := updateStmt.GetWhereClause(); whereClause != nil {
		conditions := p.extractConditionsFromExpr(whereClause)
		plan.Conditions = conditions
	}
}

// extractInsertInfoFromPGNode extracts INSERT statement information from a PG query node
func (p *Planner) extractInsertInfoFromPGNode(stmt *pg_query.Node, plan *Plan) {
	insertStmt := stmt.GetInsertStmt()
	if insertStmt == nil {
		return
	}

	// Extract table name
	if relation := insertStmt.GetRelation(); relation != nil {
		plan.Table = relation.GetRelname()
	}

	// Extract columns and values
	plan.Values = make(map[string]interface{})

	// Extract column names
	columns := make([]string, 0)
	if cols := insertStmt.GetCols(); len(cols) > 0 {
		for _, col := range cols {
			if resTarget := col.GetResTarget(); resTarget != nil {
				columns = append(columns, resTarget.GetName())
			}
		}
	}

	// Extract values from the select statement (which contains the VALUES clause)
	if selectStmt := insertStmt.GetSelectStmt(); selectStmt != nil {
		if selStmt := selectStmt.GetSelectStmt(); selStmt != nil {
			if valuesLists := selStmt.GetValuesLists(); len(valuesLists) > 0 {
				// Get the first row of values (assuming single row insert)
				if len(valuesLists) > 0 {
					firstRow := valuesLists[0]
					if list := firstRow.GetList(); list != nil {
						items := list.GetItems()
						for i, item := range items {
							if i < len(columns) {
								if aConst := item.GetAConst(); aConst != nil {
									if ival := aConst.GetIval(); ival != nil {
										plan.Values[columns[i]] = ival.GetIval()
									} else if f := aConst.GetFval(); f != nil {
										if val, err := strconv.ParseFloat(f.GetFval(), 64); err == nil {
											plan.Values[columns[i]] = val
										}
									} else if s := aConst.GetSval(); s != nil {
										plan.Values[columns[i]] = s.GetSval()
									} else if b := aConst.GetBoolval(); b != nil {
										plan.Values[columns[i]] = b.GetBoolval()
									}
								}
							}
						}
					}
				}
			}
		}
	}
}

// extractDeleteInfoFromPGNode extracts DELETE statement information from a PG query node
func (p *Planner) extractDeleteInfoFromPGNode(stmt *pg_query.Node, plan *Plan) {
	deleteStmt := stmt.GetDeleteStmt()
	if deleteStmt == nil {
		return
	}

	// Extract table name
	if relation := deleteStmt.GetRelation(); relation != nil {
		plan.Table = relation.GetRelname()
	}

	// Extract WHERE conditions
	if whereClause := deleteStmt.GetWhereClause(); whereClause != nil {
		conditions := p.extractConditionsFromExpr(whereClause)
		plan.Conditions = conditions
	}
}