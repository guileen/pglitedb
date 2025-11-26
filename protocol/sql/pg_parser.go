//go:build !pgquery_simple
// +build !pgquery_simple

package sql

import (
	"fmt"
	
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// PGParser implements the Parser interface using pganalyze/pg_query_go/v6
type PGParser struct{}

// NewPGParser creates a new PostgreSQL parser instance
func NewPGParser() *PGParser {
	return &PGParser{}
}

// Parse takes a raw SQL query string and returns a parsed representation
func (p *PGParser) Parse(query string) (*ParsedQuery, error) {
	result, err := pg_query.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL query: %w", err)
	}

	if len(result.Stmts) == 0 {
		return nil, fmt.Errorf("no statements found in query")
	}

	// Convert pg_query AST to our internal representation
	stmt := result.Stmts[0].Stmt
	returningColumns := p.extractReturningColumns(stmt)

	parsed := &ParsedQuery{
		Statement:        stmt,
		Query:            query,
		Type:             p.getStatementType(stmt),
		ReturningColumns: returningColumns,
	}

	return parsed, nil
}

// getStatementType determines the type of SQL statement from the pg_query AST
func (p *PGParser) getStatementType(stmt *pg_query.Node) StatementType {
	switch {
	case stmt.GetSelectStmt() != nil:
		return SelectStatement
	case stmt.GetInsertStmt() != nil:
		return InsertStatement
	case stmt.GetUpdateStmt() != nil:
		return UpdateStatement
	case stmt.GetDeleteStmt() != nil:
		return DeleteStatement
	default:
		return SelectStatement
	}
}

// extractReturningColumns extracts RETURNING columns from the pg_query AST
func (p *PGParser) extractReturningColumns(stmt *pg_query.Node) []string {
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
func (p *PGParser) parseReturningClause(returningList []*pg_query.Node) []string {
	// Implementation to parse RETURNING clause from pg_query AST
	columns := make([]string, len(returningList))
	for i, node := range returningList {
		if resTarget := node.GetResTarget(); resTarget != nil {
			if val := resTarget.GetVal(); val != nil {
				if columnRef := val.GetColumnRef(); columnRef != nil {
					// Extract column name from ColumnRef
					if len(columnRef.GetFields()) > 0 {
						if str := columnRef.GetFields()[len(columnRef.GetFields())-1].GetString_(); str != nil {
							columns[i] = str.GetSval()
						}
					}
				} else if aStar := val.GetAStar(); aStar != nil {
					// Handle * in RETURNING
					columns[i] = "*"
				}
			}
		}
	}
	return columns
}

// Validate checks if a query is syntactically valid
func (p *PGParser) Validate(query string) error {
	_, err := pg_query.Parse(query)
	return err
}

// ParseWithParams parses a query with parameter information
func (p *PGParser) ParseWithParams(query string, paramCount int) (*ParsedQuery, error) {
	return p.Parse(query)
}

// GetStatementType returns the type of the SQL statement
func (p *PGParser) GetStatementType(stmt interface{}) StatementType {
	if pgStmt, ok := stmt.(*pg_query.Node); ok {
		return p.getStatementType(pgStmt)
	}
	return SelectStatement
}

// SupportsParameterPlaceholders returns whether the parser supports parameter placeholders
func (p *PGParser) SupportsParameterPlaceholders() bool {
	return true
}