package sql

import (
	"strings"
)

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

func IsSystemTable(tableName string) bool {
	return isSystemTable(tableName)
}

func isSystemTable(tableName string) bool {
	// Normalize table name by removing any extra whitespace and converting to lowercase
	normalized := strings.TrimSpace(strings.ToLower(tableName))
	
	// Check for system table prefixes
	if strings.HasPrefix(normalized, "information_schema.") || 
	   strings.HasPrefix(normalized, "pg_catalog.") {
		return true
	}
	
	// Also check for common system table names without schema prefix
	if strings.HasPrefix(normalized, "pg_") {
		return true
	}
	
	// Check for information_schema tables without schema prefix
	if normalized == "tables" || normalized == "columns" || normalized == "views" {
		return true
	}
	
	return false
}

// mapPostgreSQLTypeToInternal maps PostgreSQL type names to internal column types
func mapPostgreSQLTypeToInternal(pgType string) string {
	switch pgType {
	case "int4":
		return "integer"
	case "int2":
		return "smallint"
	case "int8":
		return "bigint"
	case "float4":
		return "real"
	case "float8":
		return "double"
	case "bool":
		return "boolean"
	case "varchar", "character varying":
		return "varchar"
	case "char", "character", "bpchar":
		return "char"
	case "timestamp", "timestamp without time zone":
		return "timestamp"
	case "serial":
		return "serial"
	case "bigserial":
		return "bigserial"
	case "smallserial":
		return "smallserial"
	default:
		return pgType
	}
}