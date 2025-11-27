package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPGParser_Parse(t *testing.T) {
	parser := NewPGParser()

	query := "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10"
	parsed, err := parser.Parse(query)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Equal(t, SelectStatement, parsed.Type)
	assert.Equal(t, query, parsed.Query)
}

func TestPGParser_Validate(t *testing.T) {
	parser := NewPGParser()

	// Valid query
	err := parser.Validate("SELECT id, name FROM users")
	assert.NoError(t, err)

	// Invalid query
	err = parser.Validate("SELECT id, name FROM")
	assert.Error(t, err)
}

func TestPlanner_CreatePlan(t *testing.T) {
	parser := NewPGParser()
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
	parser := NewPGParser()
	planner := NewPlanner(parser)
	executor := NewExecutorWithCatalog(planner, nil) // Pass nil catalog for this test

	query := "SELECT id, name FROM users"
	_, err := executor.Execute(context.Background(), query)
	// This test is mainly to check that the executor doesn't panic
	// Since we pass nil catalog, we expect an error about catalog not being initialized
	require.Error(t, err)
	assert.Contains(t, err.Error(), "catalog not initialized")
}

func TestExecutor_DropTable(t *testing.T) {
	parser := NewPGParser()
	planner := NewPlanner(parser)
	executor := NewExecutorWithCatalog(planner, nil) // Pass nil catalog for this test

	// Test DROP TABLE without IF EXISTS
	query := "DROP TABLE test_table"
	_, err := executor.Execute(context.Background(), query)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "catalog not initialized")

	// Test DROP TABLE IF EXISTS
	query = "DROP TABLE IF EXISTS test_table"
	_, err = executor.Execute(context.Background(), query)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "catalog not initialized")
}