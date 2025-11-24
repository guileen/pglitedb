package sql

import (
	"github.com/xwb1989/sqlparser"
)

// StatementType represents the type of SQL statement
type StatementType int

const (
	SelectStatement StatementType = iota
	InsertStatement
	UpdateStatement
	DeleteStatement
	// Note: CreateTable, DropTable, AlterTable may not be supported by this parser
)

// ParsedQuery represents a parsed SQL query
type ParsedQuery struct {
	Type             StatementType
	Statement        sqlparser.Statement
	Query            string
	ReturningColumns []string
}

// Parser is the interface for parsing SQL queries
type Parser interface {
	// Parse takes a raw SQL query string and returns a parsed representation
	Parse(query string) (*ParsedQuery, error)
	
	// Validate checks if a query is syntactically valid
	Validate(query string) error
	
	// GetStatementType returns the type of the SQL statement
	GetStatementType(stmt sqlparser.Statement) StatementType
}