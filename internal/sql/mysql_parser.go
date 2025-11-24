package sql

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// MySQLParser implements the Parser interface using xwb1989/sqlparser
type MySQLParser struct{}

// NewMySQLParser creates a new MySQL parser instance
func NewMySQLParser() *MySQLParser {
	return &MySQLParser{}
}

// Parse takes a raw SQL query string and returns a parsed representation
func (p *MySQLParser) Parse(query string) (*ParsedQuery, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL query: %w", err)
	}

	parsed := &ParsedQuery{
		Statement: stmt,
		Query:     query,
		Type:      p.GetStatementType(stmt),
	}

	return parsed, nil
}

// Validate checks if a query is syntactically valid
func (p *MySQLParser) Validate(query string) error {
	_, err := sqlparser.Parse(query)
	return err
}

// GetStatementType returns the type of the SQL statement
func (p *MySQLParser) GetStatementType(stmt sqlparser.Statement) StatementType {
	switch stmt.(type) {
	case *sqlparser.Select:
		return SelectStatement
	case *sqlparser.Insert:
		return InsertStatement
	case *sqlparser.Update:
		return UpdateStatement
	case *sqlparser.Delete:
		return DeleteStatement
	default:
		return SelectStatement // Default fallback
	}
}