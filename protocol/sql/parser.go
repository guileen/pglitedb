package sql

import (
	"fmt"
	"regexp"
	"strconv"
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
	trimmedQuery := strings.TrimSpace(query)
	lowerQuery := strings.ToLower(trimmedQuery)

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

	// Create basic parsed query
	parsed := &ParsedQuery{
		Statement:        query,
		Query:            query,
		Type:             stmtType,
		ReturningColumns: returningColumns,
		RawStmt:          nil, // Simple parser doesn't produce a raw statement
	}

	// Extract additional information based on statement type
	switch stmtType {
	case SelectStatement:
		p.extractSelectInfo(parsed, trimmedQuery, lowerQuery)
	case InsertStatement:
		p.extractInsertInfo(parsed, trimmedQuery, lowerQuery)
	case UpdateStatement:
		p.extractUpdateInfo(parsed, trimmedQuery, lowerQuery)
	case DeleteStatement:
		p.extractDeleteInfo(parsed, trimmedQuery, lowerQuery)
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

// extractSelectInfo extracts detailed information from SELECT queries
func (p *SimplePGParser) extractSelectInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name from FROM clause
	fromIndex := strings.Index(lowerQuery, " from ")
	if fromIndex == -1 {
		return
	}

	// Find the end of the FROM clause (before WHERE, ORDER BY, LIMIT, etc.)
	endIndex := len(lowerQuery)
	for _, keyword := range []string{" where ", " order by ", " group by ", " limit ", " offset "} {
		if idx := strings.Index(lowerQuery[fromIndex+6:], keyword); idx != -1 {
			if fromIndex+6+idx < endIndex {
				endIndex = fromIndex + 6 + idx
			}
		}
	}

	tablePart := strings.TrimSpace(query[fromIndex+6 : endIndex])
	
	// Handle simple table names (no joins, no subqueries)
	if !strings.Contains(tablePart, " join ") && !strings.Contains(tablePart, "(") {
		// Remove any aliases
		parts := strings.Fields(tablePart)
		if len(parts) > 0 {
			parsed.Table = parts[0]
		}
	}

	// Extract fields from SELECT clause
	selectEnd := fromIndex
	fieldsPart := strings.TrimSpace(query[6:selectEnd])
	
	// Handle simple field extraction
	if fieldsPart != "*" {
		fields := strings.Split(fieldsPart, ",")
		parsed.Fields = make([]string, len(fields))
		for i, field := range fields {
			parsed.Fields[i] = strings.TrimSpace(field)
		}
	} else {
		parsed.Fields = []string{"*"}
	}

	// Extract conditions from WHERE clause
	whereIndex := strings.Index(lowerQuery, " where ")
	if whereIndex != -1 {
		// Find the end of WHERE clause
		whereEnd := len(lowerQuery)
		for _, keyword := range []string{" order by ", " group by ", " limit ", " offset "} {
			if idx := strings.Index(lowerQuery[whereIndex+7:], keyword); idx != -1 {
				if whereIndex+7+idx < whereEnd {
					whereEnd = whereIndex + 7 + idx
				}
			}
		}
		
		wherePart := strings.TrimSpace(query[whereIndex+7 : whereEnd])
		parsed.Conditions = p.parseWhereClause(wherePart)
	}

	// Extract ORDER BY clause
	orderByIndex := strings.Index(lowerQuery, " order by ")
	if orderByIndex != -1 {
		// Find the end of ORDER BY clause
		orderByEnd := len(lowerQuery)
		for _, keyword := range []string{" limit ", " offset "} {
			if idx := strings.Index(lowerQuery[orderByIndex+10:], keyword); idx != -1 {
				if orderByIndex+10+idx < orderByEnd {
					orderByEnd = orderByIndex + 10 + idx
				}
			}
		}
		
		orderByPart := strings.TrimSpace(query[orderByIndex+10 : orderByEnd])
		parsed.OrderBy = p.parseOrderByClause(orderByPart)
	}

	// Extract LIMIT clause
	limitIndex := strings.Index(lowerQuery, " limit ")
	if limitIndex != -1 {
		// Find the end of LIMIT clause
		limitEnd := len(lowerQuery)
		if offsetIndex := strings.Index(lowerQuery[limitIndex+7:], " offset "); offsetIndex != -1 {
			if limitIndex+7+offsetIndex < limitEnd {
				limitEnd = limitIndex + 7 + offsetIndex
			}
		}
		
		limitStr := strings.TrimSpace(query[limitIndex+7 : limitEnd])
		if limitVal, err := parseInt64(limitStr); err == nil {
			parsed.Limit = &limitVal
		}
	}
}

// extractInsertInfo extracts basic information from INSERT queries
func (p *SimplePGParser) extractInsertInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name from INSERT INTO clause
	intoIndex := strings.Index(lowerQuery, " into ")
	if intoIndex != -1 {
		// Find the start of the VALUES or SELECT clause
		tableEnd := len(lowerQuery)
		for _, keyword := range []string{" values ", " select "} {
			if idx := strings.Index(lowerQuery[intoIndex+6:], keyword); idx != -1 {
				if intoIndex+6+idx < tableEnd {
					tableEnd = intoIndex + 6 + idx
				}
			}
		}
		
		tablePart := strings.TrimSpace(query[intoIndex+6 : tableEnd])
		// Remove any parentheses or column lists
		if parenIndex := strings.Index(tablePart, "("); parenIndex != -1 {
			tablePart = tablePart[:parenIndex]
		}
		parsed.Table = strings.TrimSpace(tablePart)
	}
}

// extractUpdateInfo extracts basic information from UPDATE queries
func (p *SimplePGParser) extractUpdateInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name from UPDATE clause
	updateEnd := len(query)
	if setIndex := strings.Index(lowerQuery, " set "); setIndex != -1 {
		if updateEnd > setIndex {
			updateEnd = setIndex
		}
	}
	
	tablePart := strings.TrimSpace(query[6:updateEnd])
	// Handle potential aliases
	parts := strings.Fields(tablePart)
	if len(parts) > 0 {
		parsed.Table = parts[0]
	}
}

// extractDeleteInfo extracts basic information from DELETE queries
func (p *SimplePGParser) extractDeleteInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name from DELETE FROM clause
	fromIndex := strings.Index(lowerQuery, " from ")
	if fromIndex != -1 {
		// Find the start of the WHERE clause
		tableEnd := len(lowerQuery)
		if whereIndex := strings.Index(lowerQuery[fromIndex+6:], " where "); whereIndex != -1 {
			if fromIndex+6+whereIndex < tableEnd {
				tableEnd = fromIndex + 6 + whereIndex
			}
		}
		
		tablePart := strings.TrimSpace(query[fromIndex+6 : tableEnd])
		// Handle potential aliases
		parts := strings.Fields(tablePart)
		if len(parts) > 0 {
			parsed.Table = parts[0]
		}
	}
}

// parseWhereClause parses a WHERE clause into conditions
func (p *SimplePGParser) parseWhereClause(wherePart string) []Condition {
	// This is a simplified parser for WHERE clauses
	// It handles basic conditions like "field = value" or "field > value"
	var conditions []Condition
	
	// Handle simple AND conditions
	andParts := strings.Split(strings.ToLower(wherePart), " and ")
	for _, part := range andParts {
		part = strings.TrimSpace(part)
		
		// Handle basic operators
		for _, op := range []string{"=", "!=", "<>", ">=", "<=", ">", "<"} {
			if strings.Contains(part, " "+op+" ") {
				parts := strings.Split(part, " "+op+" ")
				if len(parts) == 2 {
					field := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					
					// Try to convert value to appropriate type
					var typedValue interface{} = value
					if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
						typedValue = strings.Trim(value, "'")
					} else if intValue, err := parseInt64(value); err == nil {
						typedValue = intValue
					}
					
					conditions = append(conditions, Condition{
						Field:    field,
						Operator: op,
						Value:    typedValue,
					})
					break
				}
			}
		}
	}
	
	return conditions
}

// parseOrderByClause parses an ORDER BY clause
func (p *SimplePGParser) parseOrderByClause(orderByPart string) []OrderBy {
	var orderBy []OrderBy
	
	// Handle comma-separated fields
	fields := strings.Split(orderByPart, ",")
	for _, field := range fields {
		field = strings.TrimSpace(field)
		parts := strings.Fields(field)
		
		if len(parts) == 1 {
			orderBy = append(orderBy, OrderBy{
				Field: parts[0],
				Order: "ASC",
			})
		} else if len(parts) == 2 {
			order := strings.ToUpper(parts[1])
			if order != "ASC" && order != "DESC" {
				order = "ASC" // Default to ASC
			}
			orderBy = append(orderBy, OrderBy{
				Field: parts[0],
				Order: order,
			})
		}
	}
	
	return orderBy
}

// parseInt64 converts a string to int64, returning error if conversion fails
func parseInt64(s string) (int64, error) {
	// Remove any non-numeric characters except digits
	cleaned := ""
	for _, r := range s {
		if r >= '0' && r <= '9' {
			cleaned += string(r)
		}
	}
	
	if cleaned == "" {
		return 0, fmt.Errorf("invalid integer: %s", s)
	}
	
	return strconv.ParseInt(cleaned, 10, 64)
}