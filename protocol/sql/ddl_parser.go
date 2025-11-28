package sql

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)



// DDLParser handles parsing of Data Definition Language statements
type DDLParser struct{}

// NewDDLParser creates a new DDL parser
func NewDDLParser() *DDLParser {
	return &DDLParser{}
}

// Parse parses a DDL statement and returns structured information
func (p *DDLParser) Parse(query string) (*DDLStatement, error) {
	result, err := pg_query.Parse(query)
	if err != nil {
		return nil, err
	}

	if len(result.Stmts) == 0 {
		return nil, fmt.Errorf("no statements found")
	}

	stmt := result.Stmts[0].Stmt
	ddlStmt := &DDLStatement{
		Query: query,
	}

	switch {
	case stmt.GetCreateStmt() != nil:
		ddlStmt.Type = CreateTableStatement
		p.parseCreateTable(stmt.GetCreateStmt(), ddlStmt)
	case stmt.GetIndexStmt() != nil:
		ddlStmt.Type = CreateIndexStatement
		p.parseCreateIndex(stmt.GetIndexStmt(), ddlStmt)
	case stmt.GetViewStmt() != nil:
		ddlStmt.Type = CreateViewStatement
		p.parseCreateView(stmt.GetViewStmt(), ddlStmt)
	case stmt.GetDropStmt() != nil:
		dropStmt := stmt.GetDropStmt()
		if p.isDropIndexStatement(dropStmt) {
			ddlStmt.Type = DropIndexStatement
			p.parseDropIndex(dropStmt, ddlStmt)
		} else if p.isDropViewStatement(dropStmt) {
			ddlStmt.Type = DropViewStatement
			p.parseDropView(dropStmt, ddlStmt)
		} else {
			ddlStmt.Type = DropTableStatement
			p.parseDropTable(dropStmt, ddlStmt)
		}
	case stmt.GetAlterTableStmt() != nil:
		ddlStmt.Type = AlterTableStatement
		p.parseAlterTable(stmt.GetAlterTableStmt(), ddlStmt)
	case stmt.GetVacuumStmt() != nil:
		// ANALYZE statements are parsed as VacuumStmt with IsVacuumcmd = false
		vacuumStmt := stmt.GetVacuumStmt()
		if !vacuumStmt.GetIsVacuumcmd() {
			p.parseAnalyzeStatement(vacuumStmt, ddlStmt)
		} else {
			// Handle VACUUM statements if needed
			return nil, fmt.Errorf("VACUUM statements not supported")
		}
	default:
		return nil, fmt.Errorf("unsupported DDL statement type")
	}

	return ddlStmt, nil
}

// parseCreateTable parses a CREATE TABLE statement
func (p *DDLParser) parseCreateTable(stmt *pg_query.CreateStmt, ddlStmt *DDLStatement) {
	if relation := stmt.GetRelation(); relation != nil {
		ddlStmt.TableName = relation.GetRelname()
	}

	if columnDefs := stmt.GetTableElts(); columnDefs != nil {
		columns := make([]ColumnDefinition, 0)
		for _, elt := range columnDefs {
			if columnDef := elt.GetColumnDef(); columnDef != nil {
				col := ColumnDefinition{
					Name: columnDef.GetColname(),
				}

				// Extract column type
				if typeName := columnDef.GetTypeName(); typeName != nil {
					if names := typeName.GetNames(); len(names) > 0 {
						// Get the last part of the type name (e.g., "integer" from "pg_catalog.integer")
						if str := names[len(names)-1].GetString_(); str != nil {
							col.Type = strings.ToLower(str.GetSval())
							// Map PostgreSQL type names to our internal type names
							col.Type = mapPostgreSQLTypeToInternal(col.Type)
						}
					}
				}

				// Check for constraints
				if columnDef.GetConstraints() != nil {
					for _, constraintNode := range columnDef.GetConstraints() {
						if constraint := constraintNode.GetConstraint(); constraint != nil {
							switch constraint.GetContype() {
							case pg_query.ConstrType_CONSTR_NOTNULL:
								col.NotNull = true
							case pg_query.ConstrType_CONSTR_PRIMARY:
								col.PrimaryKey = true
							case pg_query.ConstrType_CONSTR_UNIQUE:
								col.Unique = true
							case pg_query.ConstrType_CONSTR_DEFAULT:
								if rawExpr := constraint.GetRawExpr(); rawExpr != nil {
									if aConst := rawExpr.GetAConst(); aConst != nil {
										if aStr := aConst.GetSval(); aStr != nil {
											col.Default = aStr.GetSval()
										} else if aInt := aConst.GetIval(); aInt != nil {
											col.Default = fmt.Sprintf("%d", aInt.GetIval())
										} else if aBool := aConst.GetBoolval(); aBool != nil {
											if aBool.GetBoolval() {
												col.Default = "true"
											} else {
												col.Default = "false"
											}
										}
									}
								}
							}
						}
					}
				}

				columns = append(columns, col)
			}
		}
		ddlStmt.Columns = columns
	}
}

// parseDropTable parses a DROP TABLE statement
func (p *DDLParser) parseDropTable(stmt *pg_query.DropStmt, ddlStmt *DDLStatement) {
	// Parse table names from the objects
	if objects := stmt.GetObjects(); objects != nil {
		tableNames := make([]string, 0)
		for _, obj := range objects {
			if list := obj.GetList(); list != nil {
				if items := list.GetItems(); items != nil {
					// Get the last item which should be the table name
					if len(items) > 0 {
						if lastItem := items[len(items)-1]; lastItem != nil {
							if str := lastItem.GetString_(); str != nil {
								tableNames = append(tableNames, str.GetSval())
							}
						}
					}
				}
			}
		}
		if len(tableNames) > 0 {
			ddlStmt.TableName = tableNames[0] // For simplicity, we take the first table name
		}
	}
	
	// Parse CASCADE/RESTRICT options
	ddlStmt.Cascade = stmt.GetBehavior() == pg_query.DropBehavior_DROP_CASCADE
	ddlStmt.Restrict = stmt.GetBehavior() == pg_query.DropBehavior_DROP_RESTRICT
	
	// Parse IF EXISTS option
	ddlStmt.IfExists = stmt.GetMissingOk()
}

// isDropIndexStatement checks if a DropStmt is for dropping an index
func (p *DDLParser) isDropIndexStatement(stmt *pg_query.DropStmt) bool {
	return stmt.GetRemoveType() == pg_query.ObjectType_OBJECT_INDEX
}

// isDropViewStatement checks if a DropStmt is for dropping a view
func (p *DDLParser) isDropViewStatement(stmt *pg_query.DropStmt) bool {
	return stmt.GetRemoveType() == pg_query.ObjectType_OBJECT_VIEW
}

// parseAlterTable parses an ALTER TABLE statement
func (p *DDLParser) parseAlterTable(stmt *pg_query.AlterTableStmt, ddlStmt *DDLStatement) {
	if relation := stmt.GetRelation(); relation != nil {
		ddlStmt.TableName = relation.GetRelname()
	}
	
	// Parse the commands
	if cmds := stmt.GetCmds(); cmds != nil {
		alterCommands := make([]AlterCommand, 0)
		for _, cmdNode := range cmds {
			if cmd := cmdNode.GetAlterTableCmd(); cmd != nil {
				alterCmd := AlterCommand{
					Action: cmd.GetSubtype(),
				}
				
				// Extract column name if available
				if name := cmd.GetName(); name != "" {
					alterCmd.ColumnName = name
				}
				
				// Extract column definition if available
				if def := cmd.GetDef(); def != nil {
					if columnDef := def.GetColumnDef(); columnDef != nil {
						alterCmd.ColumnName = columnDef.GetColname()
						
						// Extract column type
						if typeName := columnDef.GetTypeName(); typeName != nil {
							if names := typeName.GetNames(); len(names) > 0 {
								if str := names[len(names)-1].GetString_(); str != nil {
									alterCmd.ColumnType = strings.ToLower(str.GetSval())
								}
							}
						}
						
						// Extract constraints
						if constraints := columnDef.GetConstraints(); constraints != nil {
							constraintTypes := make([]string, 0)
							for _, constraintNode := range constraints {
								if constraint := constraintNode.GetConstraint(); constraint != nil {
									switch constraint.GetContype() {
									case pg_query.ConstrType_CONSTR_NOTNULL:
										constraintTypes = append(constraintTypes, "NOT NULL")
									case pg_query.ConstrType_CONSTR_PRIMARY:
										constraintTypes = append(constraintTypes, "PRIMARY KEY")
									case pg_query.ConstrType_CONSTR_UNIQUE:
										constraintTypes = append(constraintTypes, "UNIQUE")
									case pg_query.ConstrType_CONSTR_CHECK:
										constraintTypes = append(constraintTypes, "CHECK")
									case pg_query.ConstrType_CONSTR_FOREIGN:
										constraintTypes = append(constraintTypes, "FOREIGN KEY")
									}
								}
							}
							alterCmd.ConstraintTypes = constraintTypes
						}
					}
				}
				
				// Handle constraint definitions
				if cmd.GetSubtype() == pg_query.AlterTableType_AT_AddConstraint {
					if def := cmd.GetDef(); def != nil {
						if constraint := def.GetConstraint(); constraint != nil {
							alterCmd.ConstraintName = constraint.GetConname()
							switch constraint.GetContype() {
							case pg_query.ConstrType_CONSTR_PRIMARY:
								alterCmd.ConstraintType = "PRIMARY KEY"
							case pg_query.ConstrType_CONSTR_UNIQUE:
								alterCmd.ConstraintType = "UNIQUE"
							case pg_query.ConstrType_CONSTR_CHECK:
								alterCmd.ConstraintType = "CHECK"
							case pg_query.ConstrType_CONSTR_FOREIGN:
								alterCmd.ConstraintType = "FOREIGN KEY"
							}
							
							// Extract constraint columns
							if keys := constraint.GetKeys(); keys != nil {
								columns := make([]string, 0)
								for _, key := range keys {
									if str := key.GetString_(); str != nil {
										columns = append(columns, str.GetSval())
									}
								}
								alterCmd.ConstraintColumns = columns
							}
						}
					}
				}
				
				// Handle DROP COLUMN actions
				if cmd.GetSubtype() == pg_query.AlterTableType_AT_DropColumn {
					alterCmd.ColumnName = cmd.GetName()
				}
				
				// Handle DROP CONSTRAINT actions
				if cmd.GetSubtype() == pg_query.AlterTableType_AT_DropConstraint {
					alterCmd.ConstraintName = cmd.GetName()
				}
				
				// Handle ALTER COLUMN TYPE actions
				if cmd.GetSubtype() == pg_query.AlterTableType_AT_AlterColumnType {
					alterCmd.ColumnName = cmd.GetName()
					if def := cmd.GetDef(); def != nil {
						if columnDef := def.GetColumnDef(); columnDef != nil {
							// Extract new column type
							if typeName := columnDef.GetTypeName(); typeName != nil {
								if names := typeName.GetNames(); len(names) > 0 {
									if str := names[len(names)-1].GetString_(); str != nil {
										alterCmd.ColumnType = strings.ToLower(str.GetSval())
									}
								}
							}
						}
					}
				}
				
				alterCommands = append(alterCommands, alterCmd)
			}
		}
		ddlStmt.AlterCommands = alterCommands
	}
}

// parseCreateIndex parses a CREATE INDEX statement
func (p *DDLParser) parseCreateIndex(stmt *pg_query.IndexStmt, ddlStmt *DDLStatement) {
	if relation := stmt.GetRelation(); relation != nil {
		ddlStmt.TableName = relation.GetRelname()
	}
	
	ddlStmt.IndexName = stmt.GetIdxname()
	ddlStmt.Unique = stmt.GetUnique()
	
	// Parse index columns
	if indexParams := stmt.GetIndexParams(); indexParams != nil {
		columns := make([]string, 0)
		for _, param := range indexParams {
			if indexElem := param.GetIndexElem(); indexElem != nil {
				if name := indexElem.GetName(); name != "" {
					columns = append(columns, name)
				}
			}
		}
		ddlStmt.IndexColumns = columns
	}
	
	// Parse index type
	if accessMethod := stmt.GetAccessMethod(); accessMethod != "" {
		ddlStmt.IndexType = accessMethod
	} else {
		ddlStmt.IndexType = "btree" // default
	}
	
	// Parse concurrent creation
	ddlStmt.Concurrent = stmt.GetConcurrent()
	
	// Parse WHERE clause for partial indexes
	if whereClause := stmt.GetWhereClause(); whereClause != nil {
		ddlStmt.WhereClause = whereClause.String()
	}
	
	// Parse index options
	if options := stmt.GetOptions(); options != nil {
		indexOptions := make(map[string]string)
		for _, option := range options {
			if defElem := option.GetDefElem(); defElem != nil {
				if defName := defElem.GetDefname(); defName != "" {
					if arg := defElem.GetArg(); arg != nil {
						// Extract the value as string
						indexOptions[defName] = arg.String()
					}
				}
			}
		}
		ddlStmt.IndexOptions = indexOptions
	}
}

// parseDropIndex parses a DROP INDEX statement
func (p *DDLParser) parseDropIndex(stmt *pg_query.DropStmt, ddlStmt *DDLStatement) {
	// Parse index names from the objects
	if objects := stmt.GetObjects(); objects != nil {
		indexNames := make([]string, 0)
		for _, obj := range objects {
			if list := obj.GetList(); list != nil {
				if items := list.GetItems(); items != nil {
					// Get the last item which should be the index name
					if len(items) > 0 {
						if lastItem := items[len(items)-1]; lastItem != nil {
							if str := lastItem.GetString_(); str != nil {
								indexNames = append(indexNames, str.GetSval())
							}
						}
					}
				}
			}
		}
		ddlStmt.IndexNames = indexNames
	}
	
	// Parse concurrent deletion
	ddlStmt.Concurrent = stmt.GetConcurrent()
	
	// Parse CASCADE/RESTRICT options
	ddlStmt.Cascade = stmt.GetBehavior() == pg_query.DropBehavior_DROP_CASCADE
	ddlStmt.Restrict = stmt.GetBehavior() == pg_query.DropBehavior_DROP_RESTRICT
}

// parseCreateView parses a CREATE VIEW statement
func (p *DDLParser) parseCreateView(stmt *pg_query.ViewStmt, ddlStmt *DDLStatement) {
	if view := stmt.GetView(); view != nil {
		ddlStmt.ViewName = view.GetRelname()
	}
	
	// Store the query for the view
	if query := stmt.GetQuery(); query != nil {
		ddlStmt.ViewQuery = query.String() // Simplified representation
	}
	
	// Check if it's a REPLACE operation
	ddlStmt.Replace = stmt.GetReplace()
	
	// Parse column names if specified
	if aliases := stmt.GetAliases(); aliases != nil {
		columnNames := make([]string, 0)
		for _, alias := range aliases {
			if str := alias.GetString_(); str != nil {
				columnNames = append(columnNames, str.GetSval())
			}
		}
		ddlStmt.ViewColumnNames = columnNames
	}
	
	// Parse view options
	if options := stmt.GetOptions(); options != nil {
		viewOptions := make(map[string]string)
		for _, option := range options {
			if defElem := option.GetDefElem(); defElem != nil {
				if defName := defElem.GetDefname(); defName != "" {
					if arg := defElem.GetArg(); arg != nil {
						// Extract the value as string
						viewOptions[defName] = arg.String()
					}
				}
			}
		}
		ddlStmt.ViewOptions = viewOptions
	}
}

// parseDropView parses a DROP VIEW statement
func (p *DDLParser) parseDropView(stmt *pg_query.DropStmt, ddlStmt *DDLStatement) {
	// Parse view names from the objects
	if objects := stmt.GetObjects(); objects != nil {
		viewNames := make([]string, 0)
		for _, obj := range objects {
			if list := obj.GetList(); list != nil {
				if items := list.GetItems(); items != nil {
					// Get the last item which should be the view name
					if len(items) > 0 {
						if lastItem := items[len(items)-1]; lastItem != nil {
							if str := lastItem.GetString_(); str != nil {
								viewNames = append(viewNames, str.GetSval())
							}
						}
					}
				}
			}
		}
		ddlStmt.ViewNames = viewNames
	}
	
	// Parse CASCADE/RESTRICT options
	ddlStmt.Cascade = stmt.GetBehavior() == pg_query.DropBehavior_DROP_CASCADE
	ddlStmt.Restrict = stmt.GetBehavior() == pg_query.DropBehavior_DROP_RESTRICT
}