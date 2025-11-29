//go:build pgquery_simple
// +build pgquery_simple

package sql

// NewPGParser creates a new PostgreSQL parser instance
// This is the simple build variant that uses the simplified parser
func NewPGParser() *SimplePGParser {
	return NewSimplePGParser()
}