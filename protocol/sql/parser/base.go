package parser

import (
	"strconv"
	"strings"
)

// StatementType represents the type of SQL statement
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
	CreateDatabaseStatement
	DropDatabaseStatement
	AlterDatabaseStatement
	TruncateTableStatement
	UnknownStatement
)

// String returns the string representation of StatementType
func (s StatementType) String() string {
	switch s {
	case SelectStatement:
		return "SELECT"
	case InsertStatement:
		return "INSERT"
	case UpdateStatement:
		return "UPDATE"
	case DeleteStatement:
		return "DELETE"
	case BeginStatement:
		return "BEGIN"
	case CommitStatement:
		return "COMMIT"
	case RollbackStatement:
		return "ROLLBACK"
	case CreateTableStatement:
		return "CREATE_TABLE"
	case DropTableStatement:
		return "DROP_TABLE"
	case AlterTableStatement:
		return "ALTER_TABLE"
	case CreateIndexStatement:
		return "CREATE_INDEX"
	case DropIndexStatement:
		return "DROP_INDEX"
	case CreateViewStatement:
		return "CREATE_VIEW"
	case DropViewStatement:
		return "DROP_VIEW"
	case AnalyzeStatementType:
		return "ANALYZE"
	case CreateDatabaseStatement:
		return "CREATE_DATABASE"
	case DropDatabaseStatement:
		return "DROP_DATABASE"
	case AlterDatabaseStatement:
		return "ALTER_DATABASE"
	case TruncateTableStatement:
		return "TRUNCATE_TABLE"
	default:
		return "UNKNOWN"
	}
}

// DDLStatement represents a parsed DDL statement
type DDLStatement struct {
	Type            StatementType
	Query           string
	TableName       string
	NewTableName    string // For ALTER TABLE RENAME
	IfExists        bool
	IfNotExists     bool
	Columns         []ColumnDefinition
	AlterCommands   []AlterCommand
	IndexName       string
	IndexNames      []string
	IndexColumns    []string
	IndexType       string
	Unique          bool
	Concurrent      bool
	Cascade         bool
	Restrict        bool
	ViewName        string
	ViewNames       []string
	ViewQuery       string
	Replace         bool
	ViewColumnNames []string
	ViewOptions     map[string]string
	IndexOptions    map[string]string
	ColumnNames     []string // For ANALYZE statements
	WhereClause     string   // For partial indexes
	AllTables       bool     // For ANALYZE statements
	Statement       interface{} // For storing specific statement types like AnalyzeStatement
	TableNames      []string    // For TRUNCATE statements
	RestartSequences bool       // For TRUNCATE statements
}

// AlterCommand represents a single ALTER TABLE command
type AlterCommand struct {
	Action           interface{} // The type of alter action
	ColumnName       string
	ColumnType       string
	ConstraintName   string
	ConstraintType   string
	ConstraintTypes  []string
	ConstraintColumns []string
}

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

// ActionType returns the type of alter action
func (a *AddColumnAction) ActionType() string {
	return "ADD_COLUMN"
}

// DropColumnAction represents dropping a column
type DropColumnAction struct {
	ColumnName string
}

// ActionType returns the type of alter action
func (a *DropColumnAction) ActionType() string {
	return "DROP_COLUMN"
}

// AlterColumnTypeAction represents altering a column type
type AlterColumnTypeAction struct {
	ColumnName string
	NewType    string
}

// ActionType returns the type of alter action
func (a *AlterColumnTypeAction) ActionType() string {
	return "ALTER_COLUMN_TYPE"
}

// AddConstraintAction represents adding a constraint
type AddConstraintAction struct {
	ConstraintType string
	ConstraintName string
	Columns        []string
	References     []string // For foreign keys
}

// ActionType returns the type of alter action
func (a *AddConstraintAction) ActionType() string {
	return "ADD_CONSTRAINT"
}

// DropConstraintAction represents dropping a constraint
type DropConstraintAction struct {
	ConstraintName string
}

// ActionType returns the type of alter action
func (a *DropConstraintAction) ActionType() string {
	return "DROP_CONSTRAINT"
}

// Condition represents a WHERE clause condition
type Condition struct {
	Field    string
	Operator string
	Value    string
}

// OrderBy represents an ORDER BY clause
type OrderBy struct {
	Field      string
	Direction  string // ASC or DESC
	NullsOrder string // FIRST or LAST
}

// Join represents a JOIN clause
type Join struct {
	Type       string // INNER, LEFT, RIGHT, FULL
	Table      string
	Conditions []Condition
}

// Subquery represents a subquery in the FROM clause
type Subquery struct {
	Query string
	Alias string
}

// WindowFunction represents a window function
type WindowFunction struct {
	Function    string
	Arguments   []string
	PartitionBy []string
	OrderBy     []OrderBy
	FrameClause string
	Alias       string
}

// parseInt64 parses a string into an int64, returning an error if parsing fails
func parseInt64(s string) (int64, error) {
	// Remove any whitespace
	s = strings.TrimSpace(s)
	
	// Handle negative numbers
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	}
	
	// Parse the number
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	
	if negative {
		val = -val
	}
	
	return val, nil
}

// ParsedQuery represents a parsed SQL query
type ParsedQuery struct {
	StatementType    StatementType
	Fields           []string
	Table            string
	Joins            []Join
	Conditions       []Condition
	OrderBy          []OrderBy
	Limit            *int64
	Offset           *int64
	Values           [][]string
	SetClauses       map[string]string
	WhereClause      string
	GroupBy          []string
	HavingClause     string
	WindowFunctions  []WindowFunction
	ReturningColumns []string
	Updates          map[string]interface{}
	QueryString      string
	RawStmt          interface{}
	
	// DDL specific fields
	Columns          []ColumnDefinition
	TableName        string
	NewTableName     string    // For ALTER TABLE RENAME
	AlterActions     []AlterAction
	IndexName        string
	IndexColumns     []string
	ViewName         string
	ViewQuery        string
	IfExists         bool
	IfNotExists      bool
	
	// Subquery information
	Subqueries []Subquery
}