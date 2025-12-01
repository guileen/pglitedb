package sql

import (
	"testing"

	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
)

func TestParser_TruncateStatement(t *testing.T) {
	// Create a simple parser
	sqlParser := NewSimplePGParser()
	
	// Test cases for TRUNCATE statements
	tests := []struct {
		name        string
		query       string
		expectedType parser.StatementType
	}{
		{
			name:        "Simple TRUNCATE TABLE",
			query:       "TRUNCATE TABLE users",
			expectedType: parser.TruncateTableStatement,
		},
		{
			name:        "Truncate with lowercase",
			query:       "truncate table users",
			expectedType: parser.TruncateTableStatement,
		},
		{
			name:        "Truncate with restart identity",
			query:       "TRUNCATE TABLE users RESTART IDENTITY",
			expectedType: parser.TruncateTableStatement,
		},
		{
			name:        "Truncate with cascade",
			query:       "TRUNCATE TABLE users CASCADE",
			expectedType: parser.TruncateTableStatement,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that the statement type is correctly identified
			parsed, err := sqlParser.Parse(tt.query)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedType, parsed.StatementType)
		})
	}
}

func TestDDLParser_TruncateStatement(t *testing.T) {
	// Create a DDL parser
	ddlParser := NewDDLParser()
	
	// Test cases for TRUNCATE statements
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "Simple TRUNCATE TABLE",
			query:       "TRUNCATE TABLE users",
			expectError: false,
		},
		{
			name:        "Truncate with restart identity",
			query:       "TRUNCATE TABLE users RESTART IDENTITY",
			expectError: false,
		},
		{
			name:        "Truncate with cascade",
			query:       "TRUNCATE TABLE users CASCADE",
			expectError: false,
		},
		{
			name:        "Truncate with restart identity and cascade",
			query:       "TRUNCATE TABLE users RESTART IDENTITY CASCADE",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ddlParser.Parse(tt.query)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// We can't easily test the specific statement type without exposing more internals
			}
		})
	}
}