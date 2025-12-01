//go:build !pgquery_simple
// +build !pgquery_simple

package sql

import (
	"fmt"
	
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/guileen/pglitedb/protocol/sql/parser"
)

// FullPGParser implements the Parser interface using pganalyze/pg_query_go/v6
type FullPGParser struct{}

// NewFullPGParser creates a new PostgreSQL parser instance
// This is the default build variant that uses the full CGO parser
func NewFullPGParser() *FullPGParser {
	return &FullPGParser{}
}

// NewPGParserFull creates a new PostgreSQL parser instance
// This is the default build variant that uses the full CGO parser
func NewPGParserFull() *FullPGParser {
	return NewFullPGParser()
}

// Parse takes a raw SQL query string and returns a parsed representation
func (p *FullPGParser) Parse(query string) (*parser.ParsedQuery, error) {
	result, err := pg_query.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL query: %w", err)
	}

	if len(result.Stmts) == 0 {
		return nil, fmt.Errorf("no statements found in query")
	}

	// Convert pg_query AST to our internal representation
	returningColumns := p.extractReturningColumns(query)

	parsed := &parser.ParsedQuery{
		StatementType:    p.getStatementType(query),
		ReturningColumns: returningColumns,
		QueryString:      query, // Add this line
		RawStmt:          result, // Add this line
	}

	return parsed, nil
}

// getStatementType determines the type of SQL statement from the pg_query AST
func (p *FullPGParser) getStatementType(query string) parser.StatementType {
	// Parse the query to get the AST
	result, err := pg_query.Parse(query)
	if err != nil {
		return parser.UnknownStatement
	}
	
	if len(result.Stmts) == 0 {
		return parser.UnknownStatement
	}
	
	// Get the first statement
	stmt := result.Stmts[0].Stmt
	
	switch {
	case stmt.GetSelectStmt() != nil:
		return parser.SelectStatement
	case stmt.GetInsertStmt() != nil:
		return parser.InsertStatement
	case stmt.GetUpdateStmt() != nil:
		return parser.UpdateStatement
	case stmt.GetDeleteStmt() != nil:
		return parser.DeleteStatement
	case stmt.GetTransactionStmt() != nil:
		// Handle transaction statements
		transStmt := stmt.GetTransactionStmt()
		switch transStmt.GetKind() {
		case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN:
			return parser.BeginStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_START:
			return parser.BeginStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT:
			return parser.CommitStatement
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			return parser.RollbackStatement
		default:
			return parser.UnknownStatement
		}
	case stmt.GetCreateStmt() != nil:
		return parser.CreateTableStatement
	case stmt.GetCreatedbStmt() != nil:
		return parser.CreateDatabaseStatement
	case stmt.GetDropdbStmt() != nil:
		return parser.DropDatabaseStatement
	case stmt.GetAlterDatabaseStmt() != nil:
		return parser.AlterDatabaseStatement
	case stmt.GetAlterDatabaseSetStmt() != nil:
		return parser.AlterDatabaseStatement
	case stmt.GetAlterDatabaseRefreshCollStmt() != nil:
		return parser.AlterDatabaseStatement
	case stmt.GetAlterOwnerStmt() != nil:
		return parser.AlterDatabaseStatement
	case stmt.GetDropStmt() != nil:
		// Check if it's DROP INDEX or DROP VIEW
		dropStmt := stmt.GetDropStmt()
		if dropStmt != nil {
			// For now, we'll assume it's DROP TABLE
			// A more complete implementation would check the object type
			return parser.DropTableStatement
		}
		return parser.DropTableStatement
	case stmt.GetAlterTableStmt() != nil:
		return parser.AlterTableStatement
	case stmt.GetIndexStmt() != nil:
		return parser.CreateIndexStatement
	case stmt.GetViewStmt() != nil:
		return parser.CreateViewStatement
	case stmt.GetVacuumStmt() != nil:
		// ANALYZE statements are parsed as VacuumStmt with IsVacuumcmd = false
		vacuumStmt := stmt.GetVacuumStmt()
		if !vacuumStmt.GetIsVacuumcmd() {
			return parser.AnalyzeStatementType
		}
		// Handle VACUUM statements if needed
		return parser.UnknownStatement
	default:
		// Debug: Print the type of statement that we don't recognize
		// This will help us identify what type of statement is causing the issue
		return parser.UnknownStatement
	}
}

// ExtractReturningColumns extracts RETURNING columns from a pg_query AST node
func (p *FullPGParser) ExtractReturningColumns(query string) []string {
	return p.extractReturningColumns(query)
}

// extractReturningColumns extracts RETURNING columns from the pg_query AST
func (p *FullPGParser) extractReturningColumns(query string) []string {
	// Parse the query to get the AST
	result, err := pg_query.Parse(query)
	if err != nil {
		return nil
	}
	
	if len(result.Stmts) == 0 {
		return nil
	}
	
	// Get the first statement
	stmt := result.Stmts[0].Stmt
	
	switch {
	case stmt.GetInsertStmt() != nil:
		insertStmt := stmt.GetInsertStmt()
		if insertStmt.GetReturningList() != nil {
			return p.parseReturningClause(insertStmt.GetReturningList())
		}
	case stmt.GetUpdateStmt() != nil:
		updateStmt := stmt.GetUpdateStmt()
		if updateStmt.GetReturningList() != nil {
			return p.parseReturningClause(updateStmt.GetReturningList())
		}
	case stmt.GetDeleteStmt() != nil:
		deleteStmt := stmt.GetDeleteStmt()
		if deleteStmt.GetReturningList() != nil {
			return p.parseReturningClause(deleteStmt.GetReturningList())
		}
	}
	return nil
}

// parseReturningClause parses the RETURNING clause from the pg_query AST
func (p *FullPGParser) parseReturningClause(returningList []*pg_query.Node) []string {
	// Implementation to parse RETURNING clause from pg_query AST
	columns := make([]string, len(returningList))
	for i, node := range returningList {
		if resTarget := node.GetResTarget(); resTarget != nil {
			if val := resTarget.GetVal(); val != nil {
				if columnRef := val.GetColumnRef(); columnRef != nil {
					// Extract column name from ColumnRef
					if len(columnRef.GetFields()) > 0 {
						// Check if the last field is a string (regular column name)
						if str := columnRef.GetFields()[len(columnRef.GetFields())-1].GetString_(); str != nil {
							columns[i] = str.GetSval()
						} else if aStar := columnRef.GetFields()[len(columnRef.GetFields())-1].GetAStar(); aStar != nil {
							// Handle * in RETURNING
							columns[i] = "*"
						}
					}
				} else if aStar := val.GetAStar(); aStar != nil {
					// Handle * in RETURNING (direct A_Star, not in ColumnRef)
					columns[i] = "*"
				}
			}
		}
	}
	return columns
}

// Validate checks if a query is syntactically valid
func (p *FullPGParser) Validate(query string) error {
	_, err := pg_query.Parse(query)
	return err
}

// ParseWithParams parses a query with parameter information
func (p *FullPGParser) ParseWithParams(query string, paramCount int) (*parser.ParsedQuery, error) {
	return p.Parse(query)
}

// GetStatementType returns the type of the SQL statement
func (p *FullPGParser) GetStatementType(stmt interface{}) parser.StatementType {
	if query, ok := stmt.(string); ok {
		return p.getStatementType(query)
	}
	return parser.SelectStatement
}

// SupportsParameterPlaceholders returns whether the parser supports parameter placeholders
func (p *FullPGParser) SupportsParameterPlaceholders() bool {
	return true
}