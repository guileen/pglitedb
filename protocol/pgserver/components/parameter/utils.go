package parameter

import (
	"fmt"
	"strconv"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// convertParameterByOID converts a parameter byte slice to the appropriate type based on OID
func ConvertParameterByOID(param []byte, oid uint32) (interface{}, error) {
	if param == nil {
		return nil, nil
	}

	paramStr := string(param)
	if paramStr == "" {
		return nil, nil
	}

	switch oid {
	case 16: // BOOLOID
		switch paramStr {
		case "t", "true", "TRUE":
			return true, nil
		case "f", "false", "FALSE":
			return false, nil
		default:
			return nil, fmt.Errorf("invalid boolean value: %s", paramStr)
		}
	case 20: // INT8OID
		val, err := strconv.ParseInt(paramStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse int8: %w", err)
		}
		return val, nil
	case 23: // INT4OID
		val, err := strconv.ParseInt(paramStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse int4: %w", err)
		}
		return int32(val), nil
	case 700: // FLOAT4OID
		val, err := strconv.ParseFloat(paramStr, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse float4: %w", err)
		}
		return float32(val), nil
	case 701: // FLOAT8OID
		val, err := strconv.ParseFloat(paramStr, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse float8: %w", err)
		}
		return val, nil
	case 25, 1043: // TEXTOID, VARCHAROID
		return paramStr, nil
	default:
		// For unknown OIDs, return as string
		return paramStr, nil
	}
}

// BindParametersInQuery is a convenience method to handle query strings directly
func BindParametersInQuery(query string, params []interface{}) (string, error) {
	// Parse query to AST
	parseResult, err := pg_query.Parse(query)
	if err != nil {
		return "", fmt.Errorf("failed to parse query: %w", err)
	}

	// Create binder and bind parameters
	binder := NewParameterBinder(parseResult, params)
	boundAST, err := binder.BindParameters()
	if err != nil {
		return "", fmt.Errorf("failed to bind parameters: %w", err)
	}

	// Convert bound AST back to SQL
	deparseResult, err := pg_query.Deparse(boundAST)
	if err != nil {
		return "", fmt.Errorf("failed to deparse AST: %w", err)
	}

	return deparseResult, nil
}