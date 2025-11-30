package sql

import (
	"regexp"
	"strings"
)

// normalizeLiterals controls whether literals should be normalized to placeholders
// This can be set to false for testing to ensure cache isolation
var normalizeLiterals = true

// Pre-compiled regex patterns for better performance
var (
	whitespaceRegex     = regexp.MustCompile(`\s+`)
	commentRegex1       = regexp.MustCompile(`/\*.*?\*/`)
	commentRegex2       = regexp.MustCompile(`--.*$`)
	numericLiteralRegex = regexp.MustCompile(`\b\d+(?:\.\d+)?\b`)
	stringLiteralRegex  = regexp.MustCompile(`'(?:''|[^'])*'`)
	functionCallRegex   = regexp.MustCompile(`(\w+)\s*\(\s*([^)]*?)\s*\)`)
	arrayLiteralRegex   = regexp.MustCompile(`array\s*\[\s*([^\]]*?)\s*\]`)
	betweenRegex        = regexp.MustCompile(`\s+between\s+([^a]+?)\s+and\s+`)
	inRegex             = regexp.MustCompile(`\s+in\s*\(\s*`)
	parenRegex1         = regexp.MustCompile(`\s*\(\s*`)
	parenRegex2         = regexp.MustCompile(`\s+\)`)
	inParenRegex        = regexp.MustCompile(`in\(`)
	asRegex             = regexp.MustCompile(`\s+as\s+`)
	andRegex            = regexp.MustCompile(`\s+and\s+`)
	orRegex             = regexp.MustCompile(`\s+or\s+`)
	notRegex            = regexp.MustCompile(`\s+not\s+`)
	innerJoinRegex      = regexp.MustCompile(`\s+inner\s+join\s+`)
	leftJoinRegex       = regexp.MustCompile(`\s+left\s+join\s+`)
	rightJoinRegex      = regexp.MustCompile(`\s+right\s+join\s+`)
	fullJoinRegex       = regexp.MustCompile(`\s+full\s+join\s+`)
	crossJoinRegex      = regexp.MustCompile(`\s+cross\s+join\s+`)
	orderByRegex        = regexp.MustCompile(`\s+order\s+by\s+`)
	groupByRegex        = regexp.MustCompile(`\s+group\s+by\s+`)
	limitRegex          = regexp.MustCompile(`\s+limit\s+`)
	offsetRegex         = regexp.MustCompile(`\s+offset\s+`)
	distinctRegex       = regexp.MustCompile(`\s+distinct\s+`)
	bracketRegex1       = regexp.MustCompile(`\s*\[\s*`)
	bracketRegex2       = regexp.MustCompile(`\s+\]`)
	likeRegex           = regexp.MustCompile(`\s+like\s+`)
	ilikeRegex          = regexp.MustCompile(`\s+ilike\s+`)
	commaRegex          = regexp.MustCompile(`\s*,\s*`)
	equalsRegex         = regexp.MustCompile(`\s*=\s*`)
	operatorRegex       = regexp.MustCompile(`\s*(=|!=|<>|<=|>=|<|>)\s*`)
	trueRegex           = regexp.MustCompile(`\btrue\b`)
	falseRegex          = regexp.MustCompile(`\bfalse\b`)
	nullRegex           = regexp.MustCompile(`\bnull\b`)
)

// EnableLiteralNormalization enables or disables literal normalization for cache keys
// This should be used in tests to ensure proper isolation
func EnableLiteralNormalization(enable bool) {
	normalizeLiterals = enable
}

// NormalizeQuery normalizes a SQL query by removing extra whitespace and standardizing formatting
// This is used for creating cache keys to improve cache hit rates
// Optimized version using pre-compiled regex patterns for better performance
func NormalizeQuery(query string) string {
	// Remove comments using pre-compiled regex
	query = commentRegex1.ReplaceAllString(query, "")
	query = commentRegex2.ReplaceAllString(query, "")
	
	// Convert to lowercase for case-insensitive comparison
	normalized := strings.ToLower(strings.TrimSpace(query))
	
	// Replace multiple whitespace with single space using pre-compiled regex
	normalized = whitespaceRegex.ReplaceAllString(normalized, " ")
	
	// Standardize whitespace around common SQL keywords and operators using pre-compiled regex
	normalized = commaRegex.ReplaceAllString(normalized, ", ")
	normalized = equalsRegex.ReplaceAllString(normalized, " = ")
	
	// Replace numeric literals with placeholders for better cache hit rates (if enabled)
	if normalizeLiterals {
		normalized = numericLiteralRegex.ReplaceAllString(normalized, "?")
	}
	
	// Replace string literals with placeholders (if enabled)
	if normalizeLiterals {
		normalized = stringLiteralRegex.ReplaceAllString(normalized, "'?'")
	}
	
	// Standardize comparison operators using pre-compiled regex
	normalized = operatorRegex.ReplaceAllString(normalized, " $1 ")
	
	// Enhanced normalization for better cache hit rates
	// Standardize boolean literals using pre-compiled regex
	normalized = trueRegex.ReplaceAllString(normalized, "true")
	normalized = falseRegex.ReplaceAllString(normalized, "false")
	
	// Standardize NULL values using pre-compiled regex
	normalized = nullRegex.ReplaceAllString(normalized, "null")
	
	// Remove trailing semicolons
	normalized = strings.TrimRight(normalized, ";")
	
	// Standardize function calls (remove extra spaces) using pre-compiled regex
	normalized = functionCallRegex.ReplaceAllString(normalized, "$1($2)")
	
	// Standardize array literals using pre-compiled regex
	normalized = arrayLiteralRegex.ReplaceAllString(normalized, "array[$1]")
	
	// Standardize BETWEEN expressions using pre-compiled regex
	normalized = betweenRegex.ReplaceAllString(normalized, " between $1 and ")
	
	// Standardize IN expressions with proper spacing using pre-compiled regex
	normalized = inRegex.ReplaceAllString(normalized, " in (")
	
	// Remove extra spaces around parentheses using pre-compiled regex
	normalized = parenRegex1.ReplaceAllString(normalized, "(")
	normalized = parenRegex2.ReplaceAllString(normalized, ")")
	
	// Add space after "in (" for consistency using pre-compiled regex
	normalized = inParenRegex.ReplaceAllString(normalized, "in (")
	
	// Standardize alias keywords using pre-compiled regex
	normalized = asRegex.ReplaceAllString(normalized, " as ")
	
	// Additional normalization for better cache hit rates
	// Standardize logical operators using pre-compiled regex
	normalized = andRegex.ReplaceAllString(normalized, " and ")
	normalized = orRegex.ReplaceAllString(normalized, " or ")
	normalized = notRegex.ReplaceAllString(normalized, " not ")
	
	// Standardize JOIN keywords using pre-compiled regex
	normalized = innerJoinRegex.ReplaceAllString(normalized, " inner join ")
	normalized = leftJoinRegex.ReplaceAllString(normalized, " left join ")
	normalized = rightJoinRegex.ReplaceAllString(normalized, " right join ")
	normalized = fullJoinRegex.ReplaceAllString(normalized, " full join ")
	normalized = crossJoinRegex.ReplaceAllString(normalized, " cross join ")
	
	// Standardize ORDER BY, GROUP BY, LIMIT clauses using pre-compiled regex
	normalized = orderByRegex.ReplaceAllString(normalized, " order by ")
	normalized = groupByRegex.ReplaceAllString(normalized, " group by ")
	normalized = limitRegex.ReplaceAllString(normalized, " limit ")
	normalized = offsetRegex.ReplaceAllString(normalized, " offset ")
	
	// Standardize DISTINCT keyword using pre-compiled regex
	normalized = distinctRegex.ReplaceAllString(normalized, " distinct ")
	
	// Remove extra spaces around brackets using pre-compiled regex
	normalized = bracketRegex1.ReplaceAllString(normalized, "[")
	normalized = bracketRegex2.ReplaceAllString(normalized, "]")
	
	// Standardize LIKE operators using pre-compiled regex
	normalized = likeRegex.ReplaceAllString(normalized, " like ")
	normalized = ilikeRegex.ReplaceAllString(normalized, " ilike ")
	
	// Final trim and space normalization
	normalized = strings.TrimSpace(normalized)
	normalized = whitespaceRegex.ReplaceAllString(normalized, " ")
	
	return normalized
}