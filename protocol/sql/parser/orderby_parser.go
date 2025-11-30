package parser

import (
	"strings"
)

// OrderByParser handles ORDER BY clause parsing
type OrderByParser struct{}

// NewOrderByParser creates a new OrderByParser
func NewOrderByParser() *OrderByParser {
	return &OrderByParser{}
}

// ParseOrderByClause parses an ORDER BY clause
func (obp *OrderByParser) ParseOrderByClause(orderByPart string) []OrderBy {
	var orderByClauses []OrderBy
	
	// Split by comma
	parts := strings.Split(orderByPart, ",")
	for _, part := range parts {
		trimmedPart := strings.TrimSpace(part)
		orderField := trimmedPart
		direction := "ASC" // Default
		
		// Check for DESC keyword
		if strings.HasSuffix(strings.ToUpper(trimmedPart), " DESC") {
			orderField = strings.TrimSpace(trimmedPart[:len(trimmedPart)-5])
			direction = "DESC"
		} else if strings.HasSuffix(strings.ToUpper(trimmedPart), " ASC") {
			orderField = strings.TrimSpace(trimmedPart[:len(trimmedPart)-4])
			direction = "ASC"
		}
		
		orderBy := OrderBy{
			Field:     orderField,
			Direction: direction,
		}
		orderByClauses = append(orderByClauses, orderBy)
	}
	
	return orderByClauses
}