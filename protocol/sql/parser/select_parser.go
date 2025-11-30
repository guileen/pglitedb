package parser

import (
	"regexp"
	"strings"
)

// SelectParser handles SELECT statement parsing
type SelectParser struct{}

// NewSelectParser creates a new SelectParser
func NewSelectParser() *SelectParser {
	return &SelectParser{}
}

// ExtractSelectInfo extracts information from a SELECT statement
func (sp *SelectParser) ExtractSelectInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract fields (everything between SELECT and FROM)
	fromIndex := strings.Index(lowerQuery, " from ")
	if fromIndex != -1 {
		fieldsPart := strings.TrimSpace(query[6:fromIndex])
		// Handle function calls and aliases
		fields := sp.parseFields(fieldsPart)
		parsed.Fields = fields
		
		// Extract window functions from fields
		windowFunctions := sp.extractWindowFunctions(fieldsPart, query)
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
		fromEnd := sp.findFromClauseEnd(lowerQuery, fromIndex)
		tablePart := strings.TrimSpace(query[fromIndex+6 : fromEnd])
		
		// Check if this is a subquery
		if strings.HasPrefix(strings.TrimSpace(tablePart), "(") {
			sp.extractSubqueryInfo(parsed, tablePart)
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
		parsed.Conditions = sp.parseWhereClause(wherePart)
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
		parsed.OrderBy = sp.parseOrderByClause(orderByPart)
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

// parseFields parses the fields part of a SELECT statement
func (sp *SelectParser) parseFields(fieldsPart string) []string {
	// Handle function calls and nested parentheses
	var fields []string
	currentField := ""
	parenLevel := 0
	
	for i, char := range fieldsPart {
		switch char {
		case ',':
			if parenLevel == 0 {
				fields = append(fields, strings.TrimSpace(currentField))
				currentField = ""
			} else {
				currentField += string(char)
			}
		case '(':
			parenLevel++
			currentField += string(char)
		case ')':
			parenLevel--
			currentField += string(char)
		default:
			currentField += string(char)
		}
		
		// Handle end of string
		if i == len(fieldsPart)-1 && currentField != "" {
			fields = append(fields, strings.TrimSpace(currentField))
		}
	}
	
	// If no commas found, treat as single field
	if len(fields) == 0 && strings.TrimSpace(fieldsPart) != "" {
		fields = append(fields, strings.TrimSpace(fieldsPart))
	}
	
	return fields
}

// extractWindowFunctions extracts window functions from fields
func (sp *SelectParser) extractWindowFunctions(fieldsPart, originalQuery string) []WindowFunction {
	var windowFunctions []WindowFunction
	
	// Look for OVER clauses which indicate window functions
	overRegex := regexp.MustCompile(`(?i)(\w+)\s*\([^)]*\)\s+OVER\s*\(`)
	matches := overRegex.FindAllStringSubmatchIndex(originalQuery, -1)
	
	for _, match := range matches {
		if len(match) >= 4 {
			funcStart := match[2]
			funcEnd := match[3]
			winFunc := originalQuery[funcStart:funcEnd]
			
			// Extract arguments
			args := sp.extractFunctionArguments(winFunc)
			
			// For now, we'll create a simplified window function representation
			// In a full implementation, we would parse the OVER clause completely
			windowFunction := WindowFunction{
				Function:   winFunc,
				Arguments:  args,
				PartitionBy: []string{}, // Simplified
				OrderBy:    []OrderBy{}, // Simplified
				FrameClause: "", // Simplified
				Alias:      "", // Would need to extract from context
			}
			windowFunctions = append(windowFunctions, windowFunction)
		}
	}
	
	return windowFunctions
}

// extractFunctionArguments extracts arguments from a function call
func (sp *SelectParser) extractFunctionArguments(funcCall string) []string {
	// Find the opening parenthesis
	openParen := strings.Index(funcCall, "(")
	if openParen == -1 {
		return []string{}
	}
	
	// Find the closing parenthesis
	closeParen := sp.findMatchingParen(funcCall, openParen)
	if closeParen == -1 {
		return []string{}
	}
	
	// Extract arguments
	argsStr := funcCall[openParen+1 : closeParen]
	if strings.TrimSpace(argsStr) == "" {
		return []string{}
	}
	
	// Split by comma, but respect nested parentheses
	var args []string
	currentArg := ""
	parenLevel := 0
	
	for _, char := range argsStr {
		switch char {
		case ',':
			if parenLevel == 0 {
				args = append(args, strings.TrimSpace(currentArg))
				currentArg = ""
			} else {
				currentArg += string(char)
			}
		case '(':
			parenLevel++
			currentArg += string(char)
		case ')':
			parenLevel--
			currentArg += string(char)
		default:
			currentArg += string(char)
		}
	}
	
	// Add the last argument
	if currentArg != "" {
		args = append(args, strings.TrimSpace(currentArg))
	}
	
	return args
}

// extractSubqueryInfo extracts subquery information from the FROM clause
func (sp *SelectParser) extractSubqueryInfo(parsed *ParsedQuery, tablePart string) {
	// Find the opening parenthesis
	openParen := strings.Index(tablePart, "(")
	if openParen == -1 {
		return
	}
	
	// Find the matching closing parenthesis
	closeParen := sp.findMatchingParen(tablePart, openParen)
	if closeParen == -1 {
		return
	}
	
	// Extract the subquery
	subquery := strings.TrimSpace(tablePart[openParen+1 : closeParen])
	
	// Extract alias if present (after the closing parenthesis)
	alias := ""
	afterParen := strings.TrimSpace(tablePart[closeParen+1:])
	if afterParen != "" {
		// Skip "AS" keyword if present and take the next word as alias
		parts := strings.Fields(afterParen)
		if len(parts) > 0 {
			if strings.ToUpper(parts[0]) == "AS" {
				// Skip "AS" and take the next word as alias
				if len(parts) > 1 {
					alias = parts[1]
				}
			} else {
				// First word is the alias
				alias = parts[0]
			}
		}
	}
	
	// Create subquery info and add to Subqueries slice (new field)
	subqueryInfo := Subquery{
		Query: subquery,
		Alias: alias,
	}
	parsed.Subqueries = append(parsed.Subqueries, subqueryInfo)
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

// parseWhereClause parses a WHERE clause into conditions
func (sp *SelectParser) parseWhereClause(wherePart string) []Condition {
	// This is a simplified implementation
	// A full implementation would handle complex expressions, nested conditions, etc.
	var conditions []Condition
	
	// Split by AND/OR operators
	// For now, we'll just create a simple representation
	if strings.TrimSpace(wherePart) != "" {
		condition := Condition{
			Field:    "*", // Simplified
			Operator: "*", // Simplified
			Value:    wherePart,
		}
		conditions = append(conditions, condition)
	}
	
	return conditions
}

// parseOrderByClause parses an ORDER BY clause
func (sp *SelectParser) parseOrderByClause(orderByPart string) []OrderBy {
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

// findMatchingParen finds the matching closing parenthesis
func (sp *SelectParser) findMatchingParen(s string, openPos int) int {
	if openPos >= len(s) || s[openPos] != '(' {
		return -1
	}
	
	level := 1
	for i := openPos + 1; i < len(s); i++ {
		switch s[i] {
		case '(':
			level++
		case ')':
			level--
			if level == 0 {
				return i
			}
		}
	}
	
	return -1
}

// findFromClauseEnd finds the end of the FROM clause
func (sp *SelectParser) findFromClauseEnd(lowerQuery string, fromStart int) int {
	fromEnd := len(lowerQuery)
	keywords := []string{" where ", " group by ", " order by ", " limit ", " offset "}
	
	for _, keyword := range keywords {
		if idx := strings.Index(lowerQuery[fromStart+6:], keyword); idx != -1 {
			potentialEnd := fromStart + 6 + idx
			if potentialEnd < fromEnd {
				fromEnd = potentialEnd
			}
		}
	}
	
	return fromEnd
}