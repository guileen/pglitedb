package pgserver

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/guileen/pglitedb/protocol/pgserver/components/parameter"
)

// ParameterBinder handles parameter binding for PostgreSQL queries
type ParameterBinder = parameter.ParameterBinder

// NewParameterBinder creates a new parameter binder
func NewParameterBinder(ast *pg_query.ParseResult, params []interface{}) *ParameterBinder {
	return parameter.NewParameterBinder(ast, params)
}

// BindParametersInQuery is a convenience method to handle query strings directly
func BindParametersInQuery(query string, params []interface{}) (string, error) {
	return parameter.BindParametersInQuery(query, params)
}

// convertParameterByOID converts a parameter byte slice to the appropriate type based on OID
func convertParameterByOID(param []byte, oid uint32) (interface{}, error) {
	return parameter.ConvertParameterByOID(param, oid)
}