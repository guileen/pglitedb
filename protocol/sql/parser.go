package sql

import (
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