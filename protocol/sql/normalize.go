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
	
	return strings.TrimSpace(normalized)
}