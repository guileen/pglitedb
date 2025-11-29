package sql

// PostgreSQLParser implements the Parser interface for PostgreSQL queries
type PostgreSQLParser struct {
	pgParser Parser
}

// NewPostgreSQLParser creates a new PostgreSQL parser instance
func NewPostgreSQLParser() *PostgreSQLParser {
	return &PostgreSQLParser{
		pgParser: NewPGParser(),
	}
}

// Parse takes a raw SQL query string and returns a parsed representation
func (p *PostgreSQLParser) Parse(query string) (*ParsedQuery, error) {
	return p.pgParser.Parse(query)
}

// Validate checks if a query is syntactically valid
func (p *PostgreSQLParser) Validate(query string) error {
	return p.pgParser.Validate(query)
}

// ParseWithParams parses a query with parameter information
func (p *PostgreSQLParser) ParseWithParams(query string, paramCount int) (*ParsedQuery, error) {
	return p.pgParser.ParseWithParams(query, paramCount)
}

// GetStatementType returns the type of the SQL statement
func (p *PostgreSQLParser) GetStatementType(stmt interface{}) StatementType {
	return p.pgParser.GetStatementType(stmt)
}

// SupportsParameterPlaceholders returns whether the parser supports parameter placeholders
func (p *PostgreSQLParser) SupportsParameterPlaceholders() bool {
	return p.pgParser.SupportsParameterPlaceholders()
}