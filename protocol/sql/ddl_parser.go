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
	case stmt.GetDropStmt() != nil:
		ddlStmt.Type = DropTableStatement
		p.parseDropTable(stmt.GetDropStmt(), ddlStmt)
	case stmt.GetAlterTableStmt() != nil:
		ddlStmt.Type = AlterTableStatement
		p.parseAlterTable(stmt.GetAlterTableStmt(), ddlStmt)
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
	// For now, we'll just set the statement type
	// Full implementation would extract table names from the objects
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
					}
				}
				
				alterCommands = append(alterCommands, alterCmd)
			}
		}
		ddlStmt.AlterCommands = alterCommands
	}
}