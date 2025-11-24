package executor

import (
	"context"

	"github.com/guileen/pglitedb/internal/table"
	"github.com/guileen/pglitedb/types"
)

type QueryType int

const (
	QueryTypeSelect QueryType = iota
	QueryTypeInsert
	QueryTypeUpdate
	QueryTypeDelete
)

type Query struct {
	Type      QueryType
	TableName string
	TenantID  int64
	Select    *SelectQuery
	Insert    *InsertQuery
	Update    *UpdateQuery
	Delete    *DeleteQuery
}

type SelectQuery struct {
	Columns []string
	Where   []Filter
	OrderBy []OrderByClause
	Limit   int
	Offset  int
	Joins   []JoinClause
}

type InsertQuery struct {
	Values map[string]*table.Value
}

type UpdateQuery struct {
	Values map[string]*table.Value
	Where  []Filter
}

type DeleteQuery struct {
	Where []Filter
}

type Filter struct {
	Column   string
	Operator Operator
	Value    interface{}
	And      []Filter
	Or       []Filter
}

type Operator int

const (
	OpEqual Operator = iota
	OpNotEqual
	OpGreaterThan
	OpGreaterThanOrEqual
	OpLessThan
	OpLessThanOrEqual
	OpIn
	OpNotIn
	OpLike
	OpNotLike
	OpIsNull
	OpIsNotNull
	OpContains
	OpStartsWith
	OpEndsWith
)

type OrderByClause struct {
	Column     string
	Descending bool
}

type JoinClause struct {
	TableName string
	JoinType  JoinType
	On        []JoinCondition
}

type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
)

type JoinCondition struct {
	LeftColumn  string
	RightColumn string
	Operator    Operator
}

// QueryResult represents the result of a query for internal use
// Using the unified type from types package
type QueryResult = types.QueryResult

type QueryExecutor interface {
	Execute(ctx context.Context, query *Query) (*types.QueryResult, error)
	Explain(ctx context.Context, query *Query) (*QueryPlan, error)
	ValidateQuery(query *Query) error
}

type QueryPlan struct {
	Steps         []PlanStep
	EstimatedCost float64
	IndexUsed     []string
}

type PlanStep struct {
	Operation   string
	Description string
	Cost        float64
}
