package components

// PreparedStatementManagerInterface defines the interface for prepared statement management
type PreparedStatementManagerInterface interface {
	ParseStatement(name, query string) error
	BindParameters(portalName, statementName string, parameters []interface{}) error
	ExecuteStatement(portalName string) (interface{}, error)
	DescribeStatement(name string) (interface{}, error)
	DescribePortal(name string) (interface{}, error)
}