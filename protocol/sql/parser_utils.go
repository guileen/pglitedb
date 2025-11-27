package sql

import (
	"strings"
)

// splitByCommaOutsideQuotes splits a string by commas, but ignores commas inside quotes
func splitByCommaOutsideQuotes(s string) []string {
	var result []string
	var current strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	
	for _, r := range s {
		switch r {
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			current.WriteRune(r)
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
			current.WriteRune(r)
		case ',':
			if !inSingleQuote && !inDoubleQuote {
				result = append(result, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}
	
	// Add the last part
	if current.Len() > 0 {
		result = append(result, strings.TrimSpace(current.String()))
	}
	
	return result
}