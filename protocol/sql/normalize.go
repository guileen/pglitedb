package sql

import (
	"regexp"
	"strings"
)

// NormalizeQuery normalizes a SQL query by removing extra whitespace and standardizing formatting
// This is used for creating cache keys to improve cache hit rates
func NormalizeQuery(query string) string {
	// Remove comments
	query = regexp.MustCompile(`/\*.*?\*/`).ReplaceAllString(query, "")
	query = regexp.MustCompile(`--.*$`).ReplaceAllString(query, "")
	
	// Convert to lowercase for case-insensitive comparison
	normalized := strings.ToLower(strings.TrimSpace(query))
	
	// Replace multiple whitespace with single space
	whitespaceRegex := regexp.MustCompile(`\s+`)
	normalized = whitespaceRegex.ReplaceAllString(normalized, " ")
	
	// Standardize whitespace around common SQL keywords and operators
	normalized = regexp.MustCompile(`\s*,\s*`).ReplaceAllString(normalized, ", ")
	normalized = regexp.MustCompile(`\s*=\s*`).ReplaceAllString(normalized, " = ")
	
	// Replace numeric literals with placeholders for better cache hit rates
	normalized = regexp.MustCompile(`\b\d+(?:\.\d+)?\b`).ReplaceAllString(normalized, "?")
	
	// Replace string literals with placeholders
	normalized = regexp.MustCompile(`'(?:''|[^'])*'`).ReplaceAllString(normalized, "'?'")
	
	// Standardize comparison operators
	normalized = regexp.MustCompile(`\s*(=|!=|<>|<=|>=|<|>)\s*`).ReplaceAllString(normalized, " $1 ")
	
	// Enhanced normalization for better cache hit rates
	// Standardize boolean literals
	normalized = regexp.MustCompile(`\btrue\b`).ReplaceAllString(normalized, "true")
	normalized = regexp.MustCompile(`\bfalse\b`).ReplaceAllString(normalized, "false")
	
	// Standardize NULL values
	normalized = regexp.MustCompile(`\bnull\b`).ReplaceAllString(normalized, "null")
	
	// Remove trailing semicolons
	normalized = strings.TrimRight(normalized, ";")
	
	// Standardize function calls (remove extra spaces)
	normalized = regexp.MustCompile(`(\w+)\s*\(\s*([^)]*?)\s*\)`).ReplaceAllString(normalized, "$1($2)")
	
	// Standardize array literals
	normalized = regexp.MustCompile(`array\s*\[\s*([^\]]*?)\s*\]`).ReplaceAllString(normalized, "array[$1]")
	
	// Standardize BETWEEN expressions
	normalized = regexp.MustCompile(`\s+between\s+([^a]+?)\s+and\s+`).ReplaceAllString(normalized, " between $1 and ")
	
	// Standardize IN expressions with proper spacing
	normalized = regexp.MustCompile(`\s+in\s*\(\s*`).ReplaceAllString(normalized, " in (")
	
	// Remove extra spaces around parentheses
	normalized = regexp.MustCompile(`\s*\(\s*`).ReplaceAllString(normalized, "(")
	normalized = regexp.MustCompile(`\s+\)`).ReplaceAllString(normalized, ")")
	
	// Add space after "in (" for consistency
	normalized = regexp.MustCompile(`in\(`).ReplaceAllString(normalized, "in (")
	
	// Standardize alias keywords
	normalized = regexp.MustCompile(`\s+as\s+`).ReplaceAllString(normalized, " as ")
	
	return strings.TrimSpace(normalized)
}