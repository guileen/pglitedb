package parser

import (
	"strings"
)

// SubqueryParser handles subquery parsing
type SubqueryParser struct {
	helperParser *HelperParser
}

// NewSubqueryParser creates a new SubqueryParser
func NewSubqueryParser() *SubqueryParser {
	return &SubqueryParser{
		helperParser: NewHelperParser(),
	}
}

// ExtractSubqueryInfo extracts subquery information from the FROM clause
func (sqp *SubqueryParser) ExtractSubqueryInfo(parsed *ParsedQuery, tablePart string) {
	// Find the opening parenthesis
	openParen := strings.Index(tablePart, "(")
	if openParen == -1 {
		return
	}
	
	// Find the matching closing parenthesis
	closeParen := sqp.helperParser.FindMatchingParen(tablePart, openParen)
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

