//go:build pgquery_simple
// +build pgquery_simple

package sql

import (
	"fmt"
	"regexp"
	"strings"
)

// PGParser implements a simplified PostgreSQL parser
type PGParser struct{}

// NewPGParser creates a new PostgreSQL parser instance
func NewPGParser() *PGParser {
	return &PGParser{}
}

// Parse takes a raw SQL query string and returns a parsed representation
func (p *PGParser) Parse(query string) (*ParsedQuery, error) {
	// This is a simplified implementation that doesn't actually parse the query
	// but just extracts basic information

	// Convert to lowercase for easier matching
	lowerQuery := strings.ToLower(strings.TrimSpace(query))

	// Determine statement type
	var stmtType StatementType
	switch {
	case strings.HasPrefix(lowerQuery, "select"):
		stmtType = SelectStatement
	case strings.HasPrefix(lowerQuery, "insert"):
		stmtType = InsertStatement
	case strings.HasPrefix(lowerQuery, "update"):
		stmtType = UpdateStatement
	case strings.HasPrefix(lowerQuery, "delete"):
		stmtType = DeleteStatement
	default:
		stmtType = SelectStatement
	}

	// Extract RETURNING columns if present
	returningColumns := p.extractReturningColumns(query)

	parsed := &ParsedQuery{
		Statement:        query,
		Query:            query,
		Type:             stmtType,
		ReturningColumns: returningColumns,
	}

	return parsed, nil
}

// getStatementType determines the type of SQL statement
func (p *PGParser) getStatementType(query string) StatementType {
	lowerQuery := strings.ToLower(strings.TrimSpace(query))

	switch {
	case strings.HasPrefix(lowerQuery, "select"):
		return SelectStatement
	case strings.HasPrefix(lowerQuery, "insert"):
		return InsertStatement
	case strings.HasPrefix(lowerQuery, "update"):
		return UpdateStatement
	case strings.HasPrefix(lowerQuery, "delete"):
		return DeleteStatement
	default:
		return SelectStatement
	}
}

// extractReturningColumns extracts RETURNING columns from the query
func (p *PGParser) extractReturningColumns(query string) []string {
	// Look for RETURNING clause (case insensitive)
	queryLower := strings.ToLower(query)
	returningIndex := strings.Index(queryLower, "returning")

	if returningIndex == -1 {
		return nil
	}

	// Extract the part after RETURNING
	returningPart := strings.TrimSpace(query[returningIndex+9:])

	// Handle different cases
	if returningPart == "*" {
		return []string{"*"}
	}

	// Use regex to properly extract RETURNING columns
	// This handles cases like "INSERT ... RETURNING id, name WHERE ..." correctly
	re := regexp.MustCompile(`(?i)^([^\s]+(?:\s*,\s*[^\s]+)*)`)
	matches := re.FindStringSubmatch(returningPart)
	if len(matches) > 1 {
		// Split by comma to get individual columns
		columns := strings.Split(matches[1], ",")
		result := make([]string, len(columns))
		for i, col := range columns {
			result[i] = strings.TrimSpace(col)
		}
		return result
	}

	return nil
}

// Validate checks if a query is syntactically valid
func (p *PGParser) Validate(query string) error {
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
		strings.HasPrefix(lowerQuery, "rollback")

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
func (p *PGParser) ParseWithParams(query string, paramCount int) (*ParsedQuery, error) {
	return p.Parse(query)
}

// GetStatementType returns the type of the SQL statement
func (p *PGParser) GetStatementType(stmt interface{}) StatementType {
	if query, ok := stmt.(string); ok {
		return p.getStatementType(query)
	}
	return SelectStatement
}

// SupportsParameterPlaceholders returns whether the parser supports parameter placeholders
func (p *PGParser) SupportsParameterPlaceholders() bool {
	return true
}
