package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLParser_Parse(t *testing.T) {
	parser := NewMySQLParser()

	query := "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10"
	parsed, err := parser.Parse(query)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Equal(t, SelectStatement, parsed.Type)
	assert.Equal(t, query, parsed.Query)
}

func TestMySQLParser_Validate(t *testing.T) {
	parser := NewMySQLParser()

	// Valid query
	err := parser.Validate("SELECT id, name FROM users")
	assert.NoError(t, err)

	// Invalid query
	err = parser.Validate("SELECT id, name FROM")
	assert.Error(t, err)
}

func TestPlanner_CreatePlan(t *testing.T) {
	parser := NewMySQLParser()
	planner := NewPlanner(parser)

	query := "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10"
	plan, err := planner.CreatePlan(query)
	require.NoError(t, err)
	require.NotNil(t, plan)
	
	assert.Equal(t, SelectStatement, plan.Type)
	assert.Equal(t, "select", plan.Operation)
	assert.Equal(t, "users", plan.Table)
	assert.Contains(t, plan.Fields, "id")
	assert.Contains(t, plan.Fields, "name")
}

func TestExecutor_Execute(t *testing.T) {
	parser := NewMySQLParser()
	planner := NewPlanner(parser)
	executor := NewExecutor(planner)

	query := "SELECT id, name FROM users"
	result, err := executor.Execute(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, result)
	
	assert.GreaterOrEqual(t, len(result.Columns), 0)
	assert.GreaterOrEqual(t, result.Count, 0)
}