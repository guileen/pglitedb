package sql

import (
	"fmt"
	"regexp"
	"strings"

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
	preprocessedQuery := p.preprocessPostgreSQLDDL(query)
	
	stmt, err := sqlparser.Parse(preprocessedQuery)
	if err != nil {
		if ddlStmt, ddlErr := p.tryParseDDL(preprocessedQuery); ddlErr == nil {
			return ddlStmt, nil
		}
		return nil, fmt.Errorf("failed to parse SQL query: %w", err)
	}

	parsed := &ParsedQuery{
		Statement: stmt,
		Query:     query,
		Type:      p.GetStatementType(stmt),
	}

	return parsed, nil
}

func (p *MySQLParser) preprocessPostgreSQLDDL(query string) string {
	reSerial := regexp.MustCompile(`(?i)\bBIGSERIAL\b`)
	query = reSerial.ReplaceAllString(query, "BIGINT")
	
	reSerial2 := regexp.MustCompile(`(?i)\bSMALLSERIAL\b`)
	query = reSerial2.ReplaceAllString(query, "SMALLINT")
	
	reSerial3 := regexp.MustCompile(`(?i)\bSERIAL\b`)
	query = reSerial3.ReplaceAllString(query, "INTEGER")
	
	reDecimal := regexp.MustCompile(`(?i)\bDECIMAL\s*\(\s*\d+\s*,\s*\d+\s*\)`)
	query = reDecimal.ReplaceAllString(query, "DECIMAL")
	
	reDefaultTrue := regexp.MustCompile(`(?i)DEFAULT\s+(true|TRUE)`)
	query = reDefaultTrue.ReplaceAllString(query, "DEFAULT 1")
	
	reDefaultFalse := regexp.MustCompile(`(?i)DEFAULT\s+(false|FALSE)`)
	query = reDefaultFalse.ReplaceAllString(query, "DEFAULT 0")
	
	reCurrentTimestamp := regexp.MustCompile(`(?i)DEFAULT\s+CURRENT_TIMESTAMP\b`)
	query = reCurrentTimestamp.ReplaceAllString(query, "DEFAULT CURRENT_TIMESTAMP()")
	
	return query
}

func (p *MySQLParser) tryParseDDL(query string) (*ParsedQuery, error) {
	stmt, err := sqlparser.ParseStrictDDL(query)
	if err != nil {
		return nil, err
	}
	
	return &ParsedQuery{
		Statement: stmt,
		Query:     query,
		Type:      SelectStatement,
	}, nil
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