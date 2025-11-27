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

// AlterCommand represents an ALTER TABLE command
type AlterCommand struct {
	Action     pg_query.AlterTableType
	ColumnName string
	ColumnType string
}

// DDLStatement represents a parsed DDL statement
type DDLStatement struct {
	Type          StatementType
	Query         string
	TableName     string
	Columns       []ColumnDefinition
	AlterCommands []AlterCommand
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