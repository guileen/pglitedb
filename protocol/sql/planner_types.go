package sql

// Plan represents a query execution plan
type Plan struct {
	Type        StatementType
	Operation   string
	Table       string
	Fields      []string
	Conditions  []Condition
	Limit       *int64
	Offset      *int64
	OrderBy     []OrderBy
	GroupBy     []string
	Aggregates  []Aggregate
	QueryString string
	Values      map[string]interface{} // For INSERT operations
	Updates     map[string]interface{} // For UPDATE operations
}

// Condition represents a WHERE clause condition
type Condition struct {
	Field    string
	Operator string
	Value    interface{}
}

// OrderBy represents an ORDER BY clause
type OrderBy struct {
	Field string
	Order string // ASC or DESC
}

// Aggregate represents an aggregation function
type Aggregate struct {
	Function string // COUNT, SUM, AVG, etc.
	Field    string
	Alias    string
}