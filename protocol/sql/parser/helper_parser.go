package parser

import (
	"strings"
)

// HelperParser provides helper functions for parsing
type HelperParser struct{}

// NewHelperParser creates a new HelperParser
func NewHelperParser() *HelperParser {
	return &HelperParser{}
}

// FindMatchingParen finds the matching closing parenthesis
func (hp *HelperParser) FindMatchingParen(s string, openPos int) int {
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

// FindFromClauseEnd finds the end of the FROM clause
func (hp *HelperParser) FindFromClauseEnd(lowerQuery string, fromStart int) int {
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