package query

import (
	"strings"
)

// SystemTableQuery represents a parsed system table query
type SystemTableQuery struct {
	Schema    string
	TableName string
	Filter    map[string]interface{}
}

// ParseSystemTableQuery parses a full table name into schema and table components
func ParseSystemTableQuery(fullTableName string) *SystemTableQuery {
	parts := strings.Split(fullTableName, ".")
	if len(parts) != 2 {
		return nil
	}
	
	return &SystemTableQuery{
		Schema:    parts[0],
		TableName: parts[1],
		Filter:    nil,
	}
}