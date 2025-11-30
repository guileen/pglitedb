package parser

import (
	"strings"
)

// SelectParser handles SELECT statement parsing
type SelectParser struct {
	windowParser    *WindowParser
	fieldParser     *FieldParser
	subqueryParser  *SubqueryParser
	whereParser     *WhereParser
	orderByParser   *OrderByParser
	helperParser    *HelperParser
}

// NewSelectParser creates a new SelectParser
func NewSelectParser() *SelectParser {
	return &SelectParser{
		windowParser:    NewWindowParser(),
		fieldParser:     NewFieldParser(),
		subqueryParser:  NewSubqueryParser(),
		whereParser:     NewWhereParser(),
		orderByParser:   NewOrderByParser(),
		helperParser:    NewHelperParser(),
	}
}

// ExtractSelectInfo extracts information from a SELECT statement
func (sp *SelectParser) ExtractSelectInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract fields (everything between SELECT and FROM)
	fromIndex := strings.Index(lowerQuery, " from ")
	if fromIndex != -1 {
		fieldsPart := strings.TrimSpace(query[6:fromIndex])
		// Handle function calls and aliases
		fields := sp.fieldParser.ParseFields(fieldsPart)
		parsed.Fields = fields
		
		// Extract window functions from fields
		windowFunctions := sp.windowParser.ExtractWindowFunctions(fieldsPart, query)
		parsed.WindowFunctions = windowFunctions
		
		// Format fields to normalize function calls
		formattedFields := make([]string, len(fields))
		copy(formattedFields, fields)
		
		// Replace window function calls with normalized format
		winIndex := 0
		for i, field := range fields {
			trimmedField := strings.TrimSpace(field)
			if strings.Contains(strings.ToLower(trimmedField), "over") {
				// This might be a window function, check if we have a corresponding parsed window function
				if winIndex < len(windowFunctions) {
					// Replace with normalized format
					formattedFields[i] = "func:" + strings.ToLower(windowFunctions[winIndex].Function)
					winIndex++
				} else {
					formattedFields[i] = trimmedField
				}
			} else {
				formattedFields[i] = trimmedField
			}
		}
		parsed.Fields = formattedFields
	}

	// Extract table and JOINs
	fromIndex = strings.Index(lowerQuery, " from ")
	if fromIndex != -1 {
		fromEnd := sp.helperParser.FindFromClauseEnd(lowerQuery, fromIndex)
		tablePart := strings.TrimSpace(query[fromIndex+6 : fromEnd])
		
		// Check if this is a subquery
		if strings.HasPrefix(strings.TrimSpace(tablePart), "(") {
			sp.subqueryParser.ExtractSubqueryInfo(parsed, tablePart)
		} else {
			// Parse regular table and JOINs
			sp.extractTableAndJoins(parsed, tablePart)
		}
	}

	// Extract conditions from WHERE clause
	whereIndex := strings.Index(lowerQuery, " where ")
	if whereIndex != -1 {
		// Find the end of WHERE clause
		whereEnd := len(lowerQuery)
		for _, keyword := range []string{" order by ", " group by ", " limit ", " offset "} {
			if idx := strings.Index(lowerQuery[whereIndex+7:], keyword); idx != -1 {
				if whereIndex+7+idx < whereEnd {
					whereEnd = whereIndex + 7 + idx
				}
			}
		}
		
		wherePart := strings.TrimSpace(query[whereIndex+7 : whereEnd])
		parsed.Conditions = sp.whereParser.ParseWhereClause(wherePart)
	}

	// Extract ORDER BY clause
	orderByIndex := strings.Index(lowerQuery, " order by ")
	if orderByIndex != -1 {
		// Find the end of ORDER BY clause
		orderByEnd := len(lowerQuery)
		for _, keyword := range []string{" limit ", " offset "} {
			if idx := strings.Index(lowerQuery[orderByIndex+10:], keyword); idx != -1 {
				if orderByIndex+10+idx < orderByEnd {
					orderByEnd = orderByIndex + 10 + idx
				}
			}
		}
		
		orderByPart := strings.TrimSpace(query[orderByIndex+10 : orderByEnd])
		parsed.OrderBy = sp.orderByParser.ParseOrderByClause(orderByPart)
	}

	// Extract LIMIT clause
	limitIndex := strings.Index(lowerQuery, " limit ")
	if limitIndex != -1 {
		// Find the end of LIMIT clause
		limitEnd := len(lowerQuery)
		if offsetIndex := strings.Index(lowerQuery[limitIndex+7:], " offset "); offsetIndex != -1 {
			if limitIndex+7+offsetIndex < limitEnd {
				limitEnd = limitIndex + 7 + offsetIndex
			}
		}
		
		limitStr := strings.TrimSpace(query[limitIndex+7 : limitEnd])
		if limitVal, err := parseInt64(limitStr); err == nil {
			parsed.Limit = &limitVal
		}
	}
}







// extractTableAndJoins extracts table name and JOIN information
func (sp *SelectParser) extractTableAndJoins(parsed *ParsedQuery, tablePart string) {
	// For simplicity, we'll just extract the first table name
	// A full implementation would parse JOIN clauses properly
	parts := strings.Fields(tablePart)
	if len(parts) > 0 {
		parsed.Table = parts[0]
	}
}




