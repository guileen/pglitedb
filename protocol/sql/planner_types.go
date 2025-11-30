package sql

import "github.com/guileen/pglitedb/protocol/sql/parser"

// Plan represents a query execution plan
type Plan struct {
	Type        parser.StatementType
	Operation   string
	Table       string
	Fields      []string
	Conditions  []Condition
	Limit       *int64
	Offset      *int64
	OrderBy     []parser.OrderBy
	GroupBy     []string
	Aggregates  []Aggregate
	QueryString string
	Values      map[string]interface{} // For INSERT operations
	Updates     map[string]interface{} // For UPDATE operations
	Subqueries  []parser.Subquery
	CaseExpressions []CaseExpression
	WindowFunctions []parser.WindowFunction
}

// Subquery represents a subquery in a SQL statement
type Subquery struct {
	Alias      string
	Query      string
	Correlated bool
	Columns    []string
	Type       SubqueryType // SCALAR, ROW, TABLE
}

type SubqueryType int

const (
	ScalarSubquery SubqueryType = iota
	RowSubquery
	TableSubquery
)

// CaseExpression represents a CASE expression
type CaseExpression struct {
	Alias      string
	Conditions []CaseCondition
	ElseValue  string
}

// CaseCondition represents a WHEN...THEN clause in a CASE expression
type CaseCondition struct {
	Condition string
	Result    string
}

// WindowFunction represents window functions like ROW_NUMBER(), RANK(), etc.
type WindowFunction struct {
	Function   string
	Arguments  []string
	PartitionBy []string
	OrderBy    []OrderBy
	FrameClause string
	Alias      string
}

// Aggregate represents an aggregation function
type Aggregate struct {
	Function  string // COUNT, SUM, AVG, etc.
	Field     string
	Alias     string
	Distinct  bool
	Arguments []string
	Filters   []Condition
}