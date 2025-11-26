package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLParser_Parse(t *testing.T) {
	parser := NewPostgreSQLParser()

	query := "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10"
	parsed, err := parser.Parse(query)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Equal(t, SelectStatement, parsed.Type)
	assert.Equal(t, query, parsed.Query)
}

func TestPostgreSQLParser_ParseWithReturning(t *testing.T) {
	parser := NewPostgreSQLParser()

	query := "INSERT INTO users (name, age) VALUES ('Alice', 30) RETURNING id"
	parsed, err := parser.Parse(query)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Equal(t, InsertStatement, parsed.Type)
	assert.NotNil(t, parsed.ReturningColumns)
}

func TestPostgreSQLParser_Validate(t *testing.T) {
	parser := NewPostgreSQLParser()

	err := parser.Validate("SELECT id, name FROM users")
	assert.NoError(t, err)

	err = parser.Validate("SELECT id, name FROM")
	assert.Error(t, err)
}
