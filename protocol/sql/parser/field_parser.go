package parser

import (
	"strings"
)

// FieldParser handles field parsing for SELECT statements
type FieldParser struct{}

// NewFieldParser creates a new FieldParser
func NewFieldParser() *FieldParser {
	return &FieldParser{}
}

// ParseFields parses the fields part of a SELECT statement
func (fp *FieldParser) ParseFields(fieldsPart string) []string {
	// Handle function calls and nested parentheses
	var fields []string
	currentField := ""
	parenLevel := 0
	inQuotes := false
	quoteChar := rune(0)
	
	for i, char := range fieldsPart {
		switch char {
		case '\'', '"':
			if !inQuotes {
				inQuotes = true
				quoteChar = char
			} else if quoteChar == char {
				inQuotes = false
				quoteChar = rune(0)
			}
			currentField += string(char)
		case ',':
			if parenLevel == 0 && !inQuotes {
				fields = append(fields, strings.TrimSpace(currentField))
				currentField = ""
			} else {
				currentField += string(char)
			}
		case '(':
			if !inQuotes {
				parenLevel++
			}
			currentField += string(char)
		case ')':
			if !inQuotes {
				parenLevel--
			}
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