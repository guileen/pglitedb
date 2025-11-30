package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/guileen/pglitedb/protocol/sql/parser"
)

func TestPostgreSQLParser_Parse(t *testing.T) {
	sqlParser := NewPostgreSQLParser()

	query := "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10"
	parsed, err := sqlParser.Parse(query)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Equal(t, parser.SelectStatement, parsed.StatementType)
	assert.Equal(t, query, parsed.QueryString)
}

func TestPostgreSQLParser_ParseWithReturning(t *testing.T) {
	sqlParser := NewPostgreSQLParser()

	query := "INSERT INTO users (name, age) VALUES ('Alice', 30) RETURNING id"
	parsed, err := sqlParser.Parse(query)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Equal(t, parser.InsertStatement, parsed.StatementType)
	assert.NotNil(t, parsed.ReturningColumns)
}

func TestPostgreSQLParser_Validate(t *testing.T) {
	sqlParser := NewPostgreSQLParser()

	err := sqlParser.Validate("SELECT id, name FROM users")
	assert.NoError(t, err)

	err = sqlParser.Validate("SELECT id, name FROM")
	assert.Error(t, err)
}