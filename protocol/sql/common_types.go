package sql

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

// SubqueryInfo represents information about a subquery in the FROM clause
type SubqueryInfo struct {
	Query    string // The subquery SQL
	Alias    string // The alias for the subquery (if any)
}