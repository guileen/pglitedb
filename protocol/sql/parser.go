package sql

import (
	"fmt"
	"strings"

	"github.com/guileen/pglitedb/protocol/sql/parser"
)

// Parser defines the interface for SQL parsers
type Parser interface {
	Parse(query string) (*parser.ParsedQuery, error)
	getStatementType(query string) parser.StatementType
	extractReturningColumns(query string) []string
	Validate(query string) error
	ParseWithParams(query string, paramCount int) (*parser.ParsedQuery, error)
	GetStatementType(stmt interface{}) parser.StatementType
	SupportsParameterPlaceholders() bool
}

// SimplePGParser implements a simple PostgreSQL parser
type SimplePGParser struct {
	selectParser *parser.SelectParser
	dmlParser    *parser.DMLParser
	ddlParser    *parser.DDLParser
}

// NewSimplePGParser creates a new SimplePGParser
func NewSimplePGParser() *SimplePGParser {
	return &SimplePGParser{
		selectParser: parser.NewSelectParser(),
		dmlParser:    parser.NewDMLParser(),
		ddlParser:    parser.NewDDLParser(),
	}
}

// Parse parses a SQL query and returns a ParsedQuery struct
func (p *SimplePGParser) Parse(query string) (*parser.ParsedQuery, error) {
	trimmedQuery := strings.TrimSpace(query)
	if trimmedQuery == "" {
		return nil, fmt.Errorf("empty query")
	}

	parsed := &parser.ParsedQuery{
		StatementType:    p.getStatementType(trimmedQuery),
		Fields:           []string{},
		Conditions:       []parser.Condition{},
		OrderBy:          []parser.OrderBy{},
		WindowFunctions:  []parser.WindowFunction{},
		ReturningColumns: p.extractReturningColumns(trimmedQuery),
		Columns:          []parser.ColumnDefinition{},
		AlterActions:     []parser.AlterAction{},
		IndexColumns:     []string{},
		Subqueries:       []parser.Subquery{},
		SetClauses:       make(map[string]string),
	}

	// Normalize query for easier parsing
	lowerQuery := strings.ToLower(trimmedQuery)

	switch parsed.StatementType {
	case parser.SelectStatement:
		p.selectParser.ExtractSelectInfo(parsed, trimmedQuery, lowerQuery)
	case parser.InsertStatement:
		p.dmlParser.ExtractInsertInfo(parsed, trimmedQuery, lowerQuery)
	case parser.UpdateStatement:
		p.dmlParser.ExtractUpdateInfo(parsed, trimmedQuery, lowerQuery)
	case parser.DeleteStatement:
		p.dmlParser.ExtractDeleteInfo(parsed, trimmedQuery, lowerQuery)
	case parser.CreateTableStatement:
		p.ddlParser.ExtractCreateTableInfo(parsed, trimmedQuery, lowerQuery)
	case parser.AlterTableStatement:
		p.ddlParser.ExtractAlterTableInfo(parsed, trimmedQuery, lowerQuery)
	case parser.DropTableStatement:
		p.ddlParser.ExtractDropTableInfo(parsed, trimmedQuery, lowerQuery)
	case parser.CreateIndexStatement:
		p.ddlParser.ExtractCreateIndexInfo(parsed, trimmedQuery, lowerQuery)
	case parser.DropIndexStatement:
		p.ddlParser.ExtractDropIndexInfo(parsed, trimmedQuery, lowerQuery)
	}

	return parsed, nil
}

// getStatementType determines the type of SQL statement
func (p *SimplePGParser) getStatementType(query string) parser.StatementType {
	return parser.GetStatementType(query)
}

// extractReturningColumns extracts RETURNING columns from the query
func (p *SimplePGParser) extractReturningColumns(query string) []string {
	return parser.ParseReturningColumns(query)
}

// Validate checks if a query is syntactically valid
func (p *SimplePGParser) Validate(query string) error {
	// This is a basic validation
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return fmt.Errorf("empty query")
	}

	// Basic validation - check if query starts with a valid statement type
	lowerQuery := strings.ToLower(trimmed)
	validStart := strings.HasPrefix(lowerQuery, "select") ||
		strings.HasPrefix(lowerQuery, "insert") ||
		strings.HasPrefix(lowerQuery, "update") ||
		strings.HasPrefix(lowerQuery, "delete") ||
		strings.HasPrefix(lowerQuery, "create") ||
		strings.HasPrefix(lowerQuery, "drop") ||
		strings.HasPrefix(lowerQuery, "alter") ||
		strings.HasPrefix(lowerQuery, "begin") ||
		strings.HasPrefix(lowerQuery, "commit") ||
		strings.HasPrefix(lowerQuery, "rollback") ||
		strings.HasPrefix(lowerQuery, "analyze")

	if !validStart {
		return fmt.Errorf("invalid query syntax: query must start with a valid SQL statement keyword")
	}

	// Additional validation for incomplete statements
	// This is a simple check - in a real implementation, this would be more sophisticated
	if strings.HasPrefix(lowerQuery, "select") && strings.HasSuffix(lowerQuery, "from") {
		return fmt.Errorf("invalid query syntax: incomplete SELECT statement")
	}

	return nil
}

// ParseWithParams parses a query with parameter information
func (p *SimplePGParser) ParseWithParams(query string, paramCount int) (*parser.ParsedQuery, error) {
	return p.Parse(query)
}

// GetStatementType returns the type of the SQL statement
func (p *SimplePGParser) GetStatementType(stmt interface{}) parser.StatementType {
	if query, ok := stmt.(string); ok {
		return p.getStatementType(query)
	}
	return parser.SelectStatement
}

// SupportsParameterPlaceholders returns whether the parser supports parameter placeholders
func (p *SimplePGParser) SupportsParameterPlaceholders() bool {
	return true
}

// NewPGParser creates a new PostgreSQL parser
func NewPGParser() Parser {
	return NewSimplePGParser()
}