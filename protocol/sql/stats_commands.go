package sql

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// AnalyzeStatement represents a parsed ANALYZE statement
type AnalyzeStatement struct {
	Query      string
	TableName  string
	Columns    []string // Empty means all columns
	AllTables  bool     // True when ANALYZE; is used
}

// parseAnalyzeStatement parses an ANALYZE statement from a VacuumStmt
func (p *DDLParser) parseAnalyzeStatement(stmt *pg_query.VacuumStmt, ddlStmt *DDLStatement) {
	ddlStmt.Type = AnalyzeStatementType
	
	analyzeStmt := &AnalyzeStatement{
		Query:     ddlStmt.Query,
		AllTables: len(stmt.GetRels()) == 0,
	}
	
	// Extract table name and columns if specified
	if len(stmt.GetRels()) > 0 {
		rel := stmt.GetRels()[0]
		if vacuumRel := rel.GetVacuumRelation(); vacuumRel != nil {
			// Extract table name
			if relation := vacuumRel.GetRelation(); relation != nil {
				analyzeStmt.TableName = relation.GetRelname()
			}
			
			// Extract column names
			if vaCols := vacuumRel.GetVaCols(); len(vaCols) > 0 {
				columns := make([]string, len(vaCols))
				for i, colNode := range vaCols {
					if str := colNode.GetString_(); str != nil {
						columns[i] = str.GetSval()
					}
				}
				analyzeStmt.Columns = columns
			}
		}
	}
	
	ddlStmt.Statement = analyzeStmt
}