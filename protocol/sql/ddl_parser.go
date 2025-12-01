package sql

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/guileen/pglitedb/protocol/sql/parser"
)



// DDLParser handles parsing of Data Definition Language statements
type DDLParser struct{}

// NewDDLParser creates a new DDL parser
func NewDDLParser() *DDLParser {
	return &DDLParser{}
}

// Parse parses a DDL statement and returns structured information
func (p *DDLParser) Parse(query string) (*parser.DDLStatement, error) {
	result, err := pg_query.Parse(query)
	if err != nil {
		return nil, err
	}

	if len(result.Stmts) == 0 {
		return nil, fmt.Errorf("no statements found")
	}

	stmt := result.Stmts[0].Stmt
	ddlStmt := &parser.DDLStatement{
		Query: query,
	}

	switch {
	case stmt.GetCreateStmt() != nil:
		ddlStmt.Type = parser.CreateTableStatement
		p.parseCreateTable(stmt.GetCreateStmt(), ddlStmt)
	case stmt.GetIndexStmt() != nil:
		ddlStmt.Type = parser.CreateIndexStatement
		p.parseCreateIndex(stmt.GetIndexStmt(), ddlStmt)
	case stmt.GetViewStmt() != nil:
		ddlStmt.Type = parser.CreateViewStatement
		p.parseCreateView(stmt.GetViewStmt(), ddlStmt)
	case stmt.GetDropStmt() != nil:
		dropStmt := stmt.GetDropStmt()
		if p.isDropIndexStatement(dropStmt) {
			ddlStmt.Type = parser.DropIndexStatement
			p.parseDropIndex(dropStmt, ddlStmt)
		} else if p.isDropViewStatement(dropStmt) {
			ddlStmt.Type = parser.DropViewStatement
			p.parseDropView(dropStmt, ddlStmt)
		} else {
			ddlStmt.Type = parser.DropTableStatement
			p.parseDropTable(dropStmt, ddlStmt)
		}
	case stmt.GetAlterTableStmt() != nil:
		ddlStmt.Type = parser.AlterTableStatement
		p.parseAlterTable(stmt.GetAlterTableStmt(), ddlStmt)
	case stmt.GetAlterDatabaseStmt() != nil:
		ddlStmt.Type = parser.AlterDatabaseStatement
		p.parseAlterDatabase(stmt.GetAlterDatabaseStmt(), ddlStmt)
	case stmt.GetAlterDatabaseSetStmt() != nil:
		ddlStmt.Type = parser.AlterDatabaseStatement
		p.parseAlterDatabaseSet(stmt.GetAlterDatabaseSetStmt(), ddlStmt)
	case stmt.GetAlterDatabaseRefreshCollStmt() != nil:
		ddlStmt.Type = parser.AlterDatabaseStatement
		p.parseAlterDatabaseRefreshColl(stmt.GetAlterDatabaseRefreshCollStmt(), ddlStmt)
	case stmt.GetAlterOwnerStmt() != nil:
		ddlStmt.Type = parser.AlterDatabaseStatement
		p.parseAlterOwner(stmt.GetAlterOwnerStmt(), ddlStmt)
	case stmt.GetVacuumStmt() != nil:
		// ANALYZE statements are parsed as VacuumStmt with IsVacuumcmd = false
		vacuumStmt := stmt.GetVacuumStmt()
		if !vacuumStmt.GetIsVacuumcmd() {
			p.parseAnalyzeStatement(vacuumStmt, ddlStmt)
		} else {
			// Handle VACUUM statements if needed
			return nil, fmt.Errorf("VACUUM statements not supported")
		}
	case stmt.GetCreatedbStmt() != nil:
		ddlStmt.Type = parser.CreateDatabaseStatement
		p.parseCreateDatabase(stmt.GetCreatedbStmt(), ddlStmt)
	case stmt.GetDropdbStmt() != nil:
		ddlStmt.Type = parser.DropDatabaseStatement
		p.parseDropDatabase(stmt.GetDropdbStmt(), ddlStmt)
	case stmt.GetTruncateStmt() != nil:
		ddlStmt.Type = parser.TruncateTableStatement
		p.parseTruncate(stmt.GetTruncateStmt(), ddlStmt)
	default:
		return nil, fmt.Errorf("unsupported DDL statement type")
	}

	return ddlStmt, nil
}

// parseCreateTable parses a CREATE TABLE statement
func (p *DDLParser) parseCreateTable(stmt *pg_query.CreateStmt, ddlStmt *parser.DDLStatement) {
	if relation := stmt.GetRelation(); relation != nil {
		ddlStmt.TableName = relation.GetRelname()
	}
	
	// Set IF NOT EXISTS flag
	ddlStmt.IfNotExists = stmt.GetIfNotExists()

	if columnDefs := stmt.GetTableElts(); columnDefs != nil {
		columns := make([]parser.ColumnDefinition, 0)
		for _, elt := range columnDefs {
			if columnDef := elt.GetColumnDef(); columnDef != nil {
				col := parser.ColumnDefinition{
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
func (p *DDLParser) parseDropTable(stmt *pg_query.DropStmt, ddlStmt *parser.DDLStatement) {
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
func (p *DDLParser) parseAlterTable(stmt *pg_query.AlterTableStmt, ddlStmt *parser.DDLStatement) {
	if relation := stmt.GetRelation(); relation != nil {
		ddlStmt.TableName = relation.GetRelname()
	}
	
	// Parse the commands
	if cmds := stmt.GetCmds(); cmds != nil {
		alterCommands := make([]parser.AlterCommand, 0)
		for _, cmdNode := range cmds {
			if cmd := cmdNode.GetAlterTableCmd(); cmd != nil {
				alterCmd := parser.AlterCommand{
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
func (p *DDLParser) parseCreateIndex(stmt *pg_query.IndexStmt, ddlStmt *parser.DDLStatement) {
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
func (p *DDLParser) parseDropIndex(stmt *pg_query.DropStmt, ddlStmt *parser.DDLStatement) {
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
func (p *DDLParser) parseCreateView(stmt *pg_query.ViewStmt, ddlStmt *parser.DDLStatement) {
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
func (p *DDLParser) parseDropView(stmt *pg_query.DropStmt, ddlStmt *parser.DDLStatement) {
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

// parseCreateDatabase parses a CREATE DATABASE statement
func (p *DDLParser) parseCreateDatabase(stmt *pg_query.CreatedbStmt, ddlStmt *parser.DDLStatement) {
	ddlStmt.TableName = stmt.GetDbname()
	
	// Parse options if present
	if options := stmt.GetOptions(); options != nil {
		// For now, we'll just acknowledge that options exist
		// In a full implementation, we would parse the specific options
	}
}

// parseDropDatabase parses a DROP DATABASE statement
func (p *DDLParser) parseDropDatabase(stmt *pg_query.DropdbStmt, ddlStmt *parser.DDLStatement) {
	ddlStmt.TableName = stmt.GetDbname()
	ddlStmt.IfExists = stmt.GetMissingOk()
	
	// Parse options if present
	if options := stmt.GetOptions(); options != nil {
		// For now, we'll just acknowledge that options exist
		// In a full implementation, we would parse the specific options
	}
}

// parseAlterDatabase parses an ALTER DATABASE statement
func (p *DDLParser) parseAlterDatabase(stmt *pg_query.AlterDatabaseStmt, ddlStmt *parser.DDLStatement) {
	ddlStmt.TableName = stmt.GetDbname()
	
	// Parse options if present
	if options := stmt.GetOptions(); options != nil {
		// For now, we'll just acknowledge that options exist
		// In a full implementation, we would parse the specific options
	}
}

// parseAlterDatabaseSet parses an ALTER DATABASE SET statement
func (p *DDLParser) parseAlterDatabaseSet(stmt *pg_query.AlterDatabaseSetStmt, ddlStmt *parser.DDLStatement) {
	ddlStmt.TableName = stmt.GetDbname()
	
	// Parse the set statement if present
	if setStmt := stmt.GetSetstmt(); setStmt != nil {
		// For now, we'll just acknowledge that set statement exists
		// In a full implementation, we would parse the specific set options
	}
}

// parseAlterDatabaseRefreshColl parses an ALTER DATABASE REFRESH COLL statement
func (p *DDLParser) parseAlterDatabaseRefreshColl(stmt *pg_query.AlterDatabaseRefreshCollStmt, ddlStmt *parser.DDLStatement) {
	ddlStmt.TableName = stmt.GetDbname()
	
	// For now, we'll just acknowledge that this statement exists
	// In a full implementation, we would parse the specific options
}

// parseAlterOwner parses an ALTER OWNER statement
func (p *DDLParser) parseAlterOwner(stmt *pg_query.AlterOwnerStmt, ddlStmt *parser.DDLStatement) {
	// Extract database name from object
	if object := stmt.GetObject(); object != nil {
		if list := object.GetList(); list != nil {
			if items := list.GetItems(); items != nil {
				// Get the last item which should be the database name
				if len(items) > 0 {
					if lastItem := items[len(items)-1]; lastItem != nil {
						if str := lastItem.GetString_(); str != nil {
							ddlStmt.TableName = str.GetSval()
						}
					}
				}
			}
		}
	}
	
	// If we couldn't extract the database name from the AST, fall back to string parsing
	if ddlStmt.TableName == "" {
		// Use our helper method to extract database name from the query string
		parsedQuery := &parser.ParsedQuery{}
		p.ExtractAlterDatabaseInfoFromRawQuery(parsedQuery, ddlStmt.Query)
		ddlStmt.TableName = parsedQuery.TableName
	}
	
	// For now, we'll just acknowledge that this statement exists
	// In a full implementation, we would parse the specific owner information
}

// ExtractAlterDatabaseInfoFromRawQuery extracts database name from ALTER DATABASE query string
func (p *DDLParser) ExtractAlterDatabaseInfoFromRawQuery(parsed *parser.ParsedQuery, query string) {
	lowerQuery := strings.ToLower(query)
	// Extract database name
	databaseIndex := strings.Index(lowerQuery, " database ")
	if databaseIndex != -1 {
		// Calculate the correct position in the original query
		originalDatabaseIndex := strings.Index(strings.ToLower(query), " database ")
		if originalDatabaseIndex != -1 {
			// Get the part after "DATABASE"
			afterDatabase := strings.TrimSpace(query[originalDatabaseIndex+10:])
			
			// Find the end of the database name by looking for the next keyword
			dbNameEnd := len(afterDatabase)
			
			// Look for common ALTER DATABASE action keywords
			keywords := []string{" set ", " owner ", " refresh ", " reset ", " rename "}
			for _, keyword := range keywords {
				if idx := strings.Index(strings.ToLower(afterDatabase), keyword); idx != -1 && idx < dbNameEnd {
					dbNameEnd = idx
				}
			}
			
			// Extract the database name
			dbName := strings.TrimSpace(afterDatabase[:dbNameEnd])
			
			// Handle quoted database names
			if strings.HasPrefix(dbName, `"`) && strings.HasSuffix(dbName, `"`) {
				dbName = dbName[1 : len(dbName)-1]
			}
			
			parsed.TableName = dbName
		}
	}
}

// parseTruncate parses a TRUNCATE TABLE statement
func (p *DDLParser) parseTruncate(stmt *pg_query.TruncateStmt, ddlStmt *parser.DDLStatement) {
	// Parse table names from the relations
	if relations := stmt.GetRelations(); relations != nil {
		tableNames := make([]string, 0)
		for _, relationNode := range relations {
			if relation := relationNode.GetRangeVar(); relation != nil {
				tableNames = append(tableNames, relation.GetRelname())
			}
		}
		if len(tableNames) > 0 {
			ddlStmt.TableName = tableNames[0] // For simplicity, we take the first table name
			ddlStmt.TableNames = tableNames   // Store all table names
		}
	}
	
	// Parse RESTART IDENTITY option
	ddlStmt.RestartSequences = stmt.GetRestartSeqs()
	
	// Parse CASCADE/RESTRICT options
	ddlStmt.Cascade = stmt.GetBehavior() == pg_query.DropBehavior_DROP_CASCADE
	ddlStmt.Restrict = stmt.GetBehavior() == pg_query.DropBehavior_DROP_RESTRICT
}