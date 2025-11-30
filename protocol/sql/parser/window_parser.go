package parser

import (
	"regexp"
	"strings"
)

// WindowParser handles window function parsing
type WindowParser struct {
	helperParser *HelperParser
}

// NewWindowParser creates a new WindowParser
func NewWindowParser() *WindowParser {
	return &WindowParser{
		helperParser: NewHelperParser(),
	}
}

// ExtractWindowFunctions extracts window functions from fields
func (wp *WindowParser) ExtractWindowFunctions(fieldsPart, originalQuery string) []WindowFunction {
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
			args := wp.extractFunctionArguments(winFunc)
			
			// For now, we'll create a simplified window function representation
			// In a full implementation, we would parse the OVER clause completely
			windowFunction := WindowFunction{
				Function:    winFunc,
				Arguments:   args,
				PartitionBy: []string{}, // Simplified
				OrderBy:     []OrderBy{}, // Simplified
				FrameClause: "", // Simplified
				Alias:       "", // Would need to extract from context
			}
			windowFunctions = append(windowFunctions, windowFunction)
		}
	}
	
	return windowFunctions
}

// extractFunctionArguments extracts arguments from a function call
func (wp *WindowParser) extractFunctionArguments(funcCall string) []string {
	// Find the opening parenthesis
	openParen := strings.Index(funcCall, "(")
	if openParen == -1 {
		return []string{}
	}
	
	// Find the closing parenthesis
	closeParen := wp.helperParser.FindMatchingParen(funcCall, openParen)
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

