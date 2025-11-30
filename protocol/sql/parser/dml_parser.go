package parser

import (
	"regexp"
	"strings"
)

// DMLParser handles DML (INSERT, UPDATE, DELETE) statement parsing
type DMLParser struct {
	helperParser *HelperParser
}

// NewDMLParser creates a new DMLParser
func NewDMLParser() *DMLParser {
	return &DMLParser{
		helperParser: NewHelperParser(),
	}
}

// ExtractInsertInfo extracts information from an INSERT statement
func (dp *DMLParser) ExtractInsertInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name
	intoIndex := strings.Index(lowerQuery, " into ")
	if intoIndex != -1 {
		tableEnd := strings.Index(lowerQuery[intoIndex+6:], " ")
		if tableEnd == -1 {
			parsed.Table = strings.TrimSpace(query[intoIndex+6:])
		} else {
			parsed.Table = strings.TrimSpace(query[intoIndex+6 : intoIndex+6+tableEnd])
		}
	}

	// Extract columns if specified
	columnsStart := strings.Index(query, "(")
	if columnsStart != -1 && strings.Contains(lowerQuery, " values ") {
		columnsEnd := dp.helperParser.FindMatchingParen(query, columnsStart)
		if columnsEnd != -1 {
			columnsPart := query[columnsStart+1 : columnsEnd]
			columns := strings.Split(columnsPart, ",")
			for i, col := range columns {
				columns[i] = strings.TrimSpace(col)
			}
			parsed.Fields = columns
		}
	}

	// Extract VALUES
	valuesIndex := strings.Index(lowerQuery, " values ")
	if valuesIndex != -1 {
		valuesPart := query[valuesIndex+8:]
		parsed.Values = dp.parseValuesClause(valuesPart)
	}
}

// ExtractUpdateInfo extracts information from an UPDATE statement
func (dp *DMLParser) ExtractUpdateInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name
	updateLen := len("update ")
	tableEnd := strings.Index(lowerQuery[updateLen:], " set ")
	if tableEnd == -1 {
		parsed.Table = strings.TrimSpace(query[updateLen:])
	} else {
		parsed.Table = strings.TrimSpace(query[updateLen : updateLen+tableEnd])
	}

	// Extract SET clauses
	setIndex := strings.Index(lowerQuery, " set ")
	if setIndex != -1 {
		// Find end of SET clause (WHERE, if present)
		setEnd := len(query)
		whereIndex := strings.Index(lowerQuery, " where ")
		if whereIndex != -1 {
			setEnd = whereIndex
		}

		setPart := query[setIndex+5 : setEnd]
		parsed.SetClauses = dp.parseSetClause(setPart)
	}

	// Extract WHERE clause
	whereIndex := strings.Index(lowerQuery, " where ")
	if whereIndex != -1 {
		wherePart := query[whereIndex+7:]
		parsed.WhereClause = strings.TrimSpace(wherePart)
	}
}

// ExtractDeleteInfo extracts information from a DELETE statement
func (dp *DMLParser) ExtractDeleteInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name
	fromIndex := strings.Index(lowerQuery, " from ")
	if fromIndex != -1 {
		tableEnd := strings.Index(lowerQuery[fromIndex+6:], " where ")
		if tableEnd == -1 {
			parsed.Table = strings.TrimSpace(query[fromIndex+6:])
		} else {
			parsed.Table = strings.TrimSpace(query[fromIndex+6 : fromIndex+6+tableEnd])
		}
	}

	// Extract WHERE clause
	whereIndex := strings.Index(lowerQuery, " where ")
	if whereIndex != -1 {
		wherePart := query[whereIndex+7:]
		parsed.WhereClause = strings.TrimSpace(wherePart)
	}
}

// parseValuesClause parses the VALUES clause of an INSERT statement
func (dp *DMLParser) parseValuesClause(valuesPart string) [][]string {
	var allValues [][]string
	
	// Remove outer parentheses if present
	valuesPart = strings.TrimSpace(valuesPart)
	if strings.HasPrefix(valuesPart, "(") {
		endParen := dp.helperParser.FindMatchingParen(valuesPart, 0)
		if endParen != -1 {
			valuesPart = valuesPart[1:endParen]
		}
	}
	
	// Handle multiple rows: (val1, val2), (val3, val4)
	rowRegex := regexp.MustCompile(`\([^)]*\)`)
	rows := rowRegex.FindAllString(valuesPart, -1)
	
	if len(rows) > 0 {
		for _, row := range rows {
			// Remove parentheses
			rowContent := row[1 : len(row)-1]
			
			// Split by comma, but respect string literals
			values := dp.splitRespectingQuotes(rowContent)
			allValues = append(allValues, values)
		}
	} else {
		// Single row without parentheses
		values := dp.splitRespectingQuotes(valuesPart)
		allValues = append(allValues, values)
	}
	
	return allValues
}

// parseSetClause parses the SET clause of an UPDATE statement
func (dp *DMLParser) parseSetClause(setPart string) map[string]string {
	setClauses := make(map[string]string)
	
	// Split by comma, but respect string literals and nested expressions
	pairs := dp.splitRespectingQuotes(setPart)
	
	for _, pair := range pairs {
		trimmedPair := strings.TrimSpace(pair)
		equalsIndex := strings.Index(trimmedPair, "=")
		if equalsIndex != -1 {
			field := strings.TrimSpace(trimmedPair[:equalsIndex])
			value := strings.TrimSpace(trimmedPair[equalsIndex+1:])
			setClauses[field] = value
		}
	}
	
	return setClauses
}

// splitRespectingQuotes splits a string by commas while respecting quoted strings
func (dp *DMLParser) splitRespectingQuotes(s string) []string {
	var result []string
	current := ""
	inSingleQuote := false
	inDoubleQuote := false
	escaped := false
	
	for i, char := range s {
		if escaped {
			current += string(char)
			escaped = false
			continue
		}
		
		switch char {
		case '\\':
			escaped = true
			current += string(char)
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			current += string(char)
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
			current += string(char)
		case ',':
			if !inSingleQuote && !inDoubleQuote {
				result = append(result, strings.TrimSpace(current))
				current = ""
			} else {
				current += string(char)
			}
		default:
			current += string(char)
		}
		
		// Handle end of string
		if i == len(s)-1 && current != "" {
			result = append(result, strings.TrimSpace(current))
		}
	}
	
	// If no commas found, treat as single element
	if len(result) == 0 && strings.TrimSpace(s) != "" {
		result = append(result, strings.TrimSpace(s))
	}
	
	return result
}

