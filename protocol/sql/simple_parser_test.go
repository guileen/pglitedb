package sql

import (
	"testing"

	parser "github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimplePGParser_DropDatabaseStatement(t *testing.T) {
	simpleParser := NewSimplePGParser()

	tests := []struct {
		query    string
		expected parser.StatementType
	}{
		{"DROP DATABASE testdb", parser.DropDatabaseStatement},
		{"DROP DATABASE IF EXISTS testdb", parser.DropDatabaseStatement},
		{"drop database testdb", parser.DropDatabaseStatement},
		{"drop database if exists testdb", parser.DropDatabaseStatement},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			parsed, err := simpleParser.Parse(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, parsed.StatementType, "Query: %s", tt.query)
		})
	}
}