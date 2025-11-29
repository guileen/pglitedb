package sql

import (
	"fmt"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type StatementType int

const (
	SelectStatement StatementType = iota
	InsertStatement
	UpdateStatement
	DeleteStatement
	BeginStatement
	CommitStatement
	RollbackStatement
	CreateTableStatement
	DropTableStatement
	AlterTableStatement
	CreateIndexStatement
	DropIndexStatement
	CreateViewStatement
	DropViewStatement
	AnalyzeStatementType
	UnknownStatement
)

// ColumnDefinition represents a column definition in a table
type ColumnDefinition struct {
	Name       string
	Type       string
	NotNull    bool
	PrimaryKey bool
	Unique     bool
	Default    string
}

// AlterAction represents an ALTER TABLE action
type AlterAction interface {
	ActionType() string
}

// AddColumnAction represents adding a column
type AddColumnAction struct {
	ColumnDef *ColumnDefinition
}

func (a *AddColumnAction) ActionType() string {
	return "ADD_COLUMN"
}

// DropColumnAction represents dropping a column
type DropColumnAction struct {
	ColumnName string
	Cascade    bool
	Restrict   bool
}

func (a *DropColumnAction) ActionType() string {
	return "DROP_COLUMN"
}

// AlterColumnTypeAction represents altering a column type
type AlterColumnTypeAction struct {
	ColumnName string
	NewType    string
	UsingExpr  string
}

func (a *AlterColumnTypeAction) ActionType() string {
	return "ALTER_COLUMN_TYPE"
}

// AddConstraintAction represents adding a constraint
type AddConstraintAction struct {
	Constraint *ConstraintDefinition
}

func (a *AddConstraintAction) ActionType() string {
	return "ADD_CONSTRAINT"
}

// DropConstraintAction represents dropping a constraint
type DropConstraintAction struct {
	ConstraintName string
	Cascade        bool
}

func (a *DropConstraintAction) ActionType() string {
	return "DROP_CONSTRAINT"
}

// ConstraintDefinition represents a constraint definition
type ConstraintDefinition struct {
	Name       string
	Type       string // "PRIMARY KEY", "UNIQUE", "CHECK", "FOREIGN KEY"
	Columns    []string
	Reference  *ReferenceDefinition
	CheckExpr  string
}

// ReferenceDefinition represents a foreign key reference
type ReferenceDefinition struct {
	Table   string
	Columns []string
}

// AlterCommand represents an ALTER TABLE command
type AlterCommand struct {
	Action           pg_query.AlterTableType
	ColumnName       string
	ColumnType       string
	ConstraintTypes  []string // For column constraints
	ConstraintType   string   // For table constraints
	ConstraintName   string
	ConstraintColumns []string
}

// DDLStatement represents a parsed DDL statement
type DDLStatement struct {
	Type          StatementType
	Query         string
	TableName     string
	Columns       []ColumnDefinition
	AlterCommands []AlterCommand
	Statement     interface{} // Added to hold specific statement types like AnalyzeStatement
	
	// Index related fields
	IndexName     string
	IndexNames    []string
	IndexColumns  []string
	IndexType     string
	Unique        bool
	Concurrent    bool
	WhereClause   string
	IndexOptions  map[string]string
	Cascade       bool
	Restrict      bool
	IfExists      bool
	
	// View related fields
	ViewName         string
	ViewNames        []string
	ViewQuery        string
	Replace          bool
	ViewColumnNames  []string
	ViewOptions      map[string]string
}

type ParsedQuery struct {
	Type             StatementType
	Statement        interface{}
	Query            string
	ReturningColumns []string
	Table            string
	Fields           []string
	Conditions       []Condition
	OrderBy          []OrderBy
	Limit            *int64
	Updates          map[string]interface{}
	// Raw statement for planners that need direct access to the AST
	RawStmt          interface{}
}

type Statement interface {
	StatementNode()
}

type ParameterPlaceholder struct {
	Index int
}

type Parser interface {
	Parse(query string) (*ParsedQuery, error)
	
	ParseWithParams(query string, paramCount int) (*ParsedQuery, error)
	
	Validate(query string) error
	
	GetStatementType(stmt interface{}) StatementType
	
	SupportsParameterPlaceholders() bool
}

// SimplePGParser implements a simplified PostgreSQL parser
type SimplePGParser struct{}

// NewSimplePGParser creates a new simplified PostgreSQL parser instance
func NewSimplePGParser() *SimplePGParser {
	return &SimplePGParser{}
}

// Parse takes a raw SQL query string and returns a parsed representation
func (p *SimplePGParser) Parse(query string) (*ParsedQuery, error) {
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
	case strings.HasPrefix(lowerQuery, "begin") || strings.HasPrefix(lowerQuery, "start transaction"):
		stmtType = BeginStatement
	case strings.HasPrefix(lowerQuery, "commit"):
		stmtType = CommitStatement
	case strings.HasPrefix(lowerQuery, "rollback"):
		stmtType = RollbackStatement
	case strings.HasPrefix(lowerQuery, "create table"):
		stmtType = CreateTableStatement
	case strings.HasPrefix(lowerQuery, "drop table"):
		stmtType = DropTableStatement
	case strings.HasPrefix(lowerQuery, "alter table"):
		stmtType = AlterTableStatement
	case strings.HasPrefix(lowerQuery, "create index"):
		stmtType = CreateIndexStatement
	case strings.HasPrefix(lowerQuery, "drop index"):
		stmtType = DropIndexStatement
	case strings.HasPrefix(lowerQuery, "create view"):
		stmtType = CreateViewStatement
	case strings.HasPrefix(lowerQuery, "drop view"):
		stmtType = DropViewStatement
	case strings.HasPrefix(lowerQuery, "analyze"):
		stmtType = AnalyzeStatementType
	default:
		stmtType = UnknownStatement
	}

	// Extract RETURNING columns if present
	returningColumns := p.extractReturningColumns(query)

	parsed := &ParsedQuery{
		Statement:        query,
		Query:            query,
		Type:             stmtType,
		ReturningColumns: returningColumns,
		RawStmt:          nil, // Simple parser doesn't produce a raw statement
	}

	return parsed, nil
}

// getStatementType determines the type of SQL statement
func (p *SimplePGParser) getStatementType(query string) StatementType {
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
	case strings.HasPrefix(lowerQuery, "begin") || strings.HasPrefix(lowerQuery, "start transaction"):
		return BeginStatement
	case strings.HasPrefix(lowerQuery, "commit"):
		return CommitStatement
	case strings.HasPrefix(lowerQuery, "rollback"):
		return RollbackStatement
	case strings.HasPrefix(lowerQuery, "create table"):
		return CreateTableStatement
	case strings.HasPrefix(lowerQuery, "drop table"):
		return DropTableStatement
	case strings.HasPrefix(lowerQuery, "alter table"):
		return AlterTableStatement
	case strings.HasPrefix(lowerQuery, "create index"):
		return CreateIndexStatement
	case strings.HasPrefix(lowerQuery, "drop index"):
		return DropIndexStatement
	case strings.HasPrefix(lowerQuery, "create view"):
		return CreateViewStatement
	case strings.HasPrefix(lowerQuery, "drop view"):
		return DropViewStatement
	case strings.HasPrefix(lowerQuery, "analyze"):
		return AnalyzeStatementType
	default:
		return UnknownStatement
	}
}

// extractReturningColumns extracts RETURNING columns from the query
func (p *SimplePGParser) extractReturningColumns(query string) []string {
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
	// The regex captures everything up to the first keyword or end of string
	re := regexp.MustCompile(`(?i)^([^\s,]+(?:\s*,\s*[^\s,]+)*)`)
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
func (p *SimplePGParser) ParseWithParams(query string, paramCount int) (*ParsedQuery, error) {
	return p.Parse(query)
}

// GetStatementType returns the type of the SQL statement
func (p *SimplePGParser) GetStatementType(stmt interface{}) StatementType {
	if query, ok := stmt.(string); ok {
		return p.getStatementType(query)
	}
	return SelectStatement
}

// SupportsParameterPlaceholders returns whether the parser supports parameter placeholders
func (p *SimplePGParser) SupportsParameterPlaceholders() bool {
	return true
}