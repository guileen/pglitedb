package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// WhereParser handles WHERE clause parsing
type WhereParser struct{}

// NewWhereParser creates a new WhereParser
func NewWhereParser() *WhereParser {
	return &WhereParser{}
}

// parseLiteralValue parses a literal value string into the appropriate Go type
func (wp *WhereParser) parseLiteralValue(value string) interface{} {
	trimmed := strings.TrimSpace(value)
	
	// Handle string literals (single or double quotes)
	if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
	   (strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
		// Remove quotes
		unquoted := trimmed[1 : len(trimmed)-1]
		// Handle escaped quotes
		unquoted = strings.ReplaceAll(unquoted, "''", "'")
		unquoted = strings.ReplaceAll(unquoted, "\\\"", "\"")
		return unquoted
	}
	
	// Handle boolean values
	if strings.ToLower(trimmed) == "true" {
		return true
	}
	if strings.ToLower(trimmed) == "false" {
		return false
	}
	
	// Handle numeric values
	if i, err := strconv.ParseInt(trimmed, 10, 32); err == nil {
		return int32(i)
	}
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return f
	}
	
	// Return as string if no other type matches
	return trimmed
}

// ParseWhereClause parses a WHERE clause into conditions
func (wp *WhereParser) ParseWhereClause(wherePart string) []Condition {
	// This is a simplified implementation
	// A full implementation would handle complex expressions, nested conditions, etc.
	var conditions []Condition
	
	// Split by AND/OR operators
	// For now, we'll just create a simple representation
	if strings.TrimSpace(wherePart) != "" {
		// Try to parse simple conditions like "field = value"
		// This is a very basic implementation that handles only simple equality conditions
		parts := strings.Split(wherePart, " AND ")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			
			// Try to parse various comparison operators
			operators := []string{" = ", " > ", " < ", " >= ", " <= ", " != ", " <> "}
			foundOperator := false
			
			for _, op := range operators {
				if strings.Contains(part, op) {
					opParts := strings.Split(part, op)
					if len(opParts) == 2 {
						field := strings.TrimSpace(opParts[0])
						value := strings.TrimSpace(opParts[1])
						
						// Remove quotes if present
						if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
							value = strings.Trim(value, "'")
						} else if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
							value = strings.Trim(value, "\"")
						}
						
						condition := Condition{
							Field:    field,
							Operator: strings.TrimSpace(op),
							Value:    fmt.Sprintf("%v", wp.parseLiteralValue(value)),
						}
						conditions = append(conditions, condition)
						foundOperator = true
						break
					}
				}
			}
			
			// If no operator found, fall back to the original simplified representation
			if !foundOperator {
				condition := Condition{
					Field:    "*", // Simplified
					Operator: "*", // Simplified
					Value:    fmt.Sprintf("%v", wp.parseLiteralValue(part)),
				}
				conditions = append(conditions, condition)
			}
		}
	}
	
	return conditions
}