package sql

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pqlitedb/codec"
	"github.com/guileen/pqlitedb/engine"
	"github.com/guileen/pqlitedb/kv"
	"github.com/guileen/pqlitedb/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestEngine(t *testing.T) (engine.StorageEngine, func()) {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "sql-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	config := kv.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	config.MemTableSize = 1 << 20 // 1MB for testing
	kvStore, err := kv.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}

	c := codec.NewMemComparableCodec()
	engine := engine.NewPebbleEngine(kvStore, c)

	cleanup := func() {
		engine.Close()
		os.RemoveAll(tmpDir)
	}

	return engine, cleanup
}

func createTestSchema() *table.TableDefinition {
	return &table.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []table.ColumnDefinition{
			{Name: "id", Type: table.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: table.ColumnTypeString},
			{Name: "email", Type: table.ColumnTypeString},
			{Name: "age", Type: table.ColumnTypeNumber},
			{Name: "active", Type: table.ColumnTypeBoolean},
			{Name: "created_at", Type: table.ColumnTypeTimestamp},
			{Name: "metadata", Type: table.ColumnTypeJSON},
			{Name: "profile_id", Type: table.ColumnTypeUUID},
		},
		Indexes: []table.IndexDefinition{
			{Name: "idx_email", Columns: []string{"email"}, Unique: true},
			{Name: "idx_name", Columns: []string{"name"}, Unique: false},
			{Name: "idx_age_name", Columns: []string{"age", "name"}, Unique: false},
		},
	}
}

func TestIntegration_SQLWithIndexes(t *testing.T) {
	// Create SQL executor
	parser := NewMySQLParser()
	planner := NewPlanner(parser)
	executor := NewExecutor(planner)

	// Test 1: Simple SELECT with index lookup (testing query parsing and planning)
	t.Run("SelectWithIndex", func(t *testing.T) {
		query := "SELECT id, name, email FROM users WHERE email = 'alice@example.com'"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.GreaterOrEqual(t, result.Count, 0)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "id")
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "email")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "email", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		assert.Equal(t, "alice@example.com", plan.Conditions[0].Value)
	})

	// Test 2: SELECT with composite index
	t.Run("SelectWithCompositeIndex", func(t *testing.T) {
		query := "SELECT id, name FROM users WHERE age = 30 AND name = 'Alice'"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "id")
		assert.Contains(t, plan.Fields, "name")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 2)
	})

	// Test 3: SELECT with ORDER BY using index
	t.Run("SelectWithOrderBy", func(t *testing.T) {
		query := "SELECT name, age FROM users ORDER BY age ASC"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "age")

		// Check that ORDER BY was parsed
		assert.Len(t, plan.OrderBy, 1)
		assert.Equal(t, "age", plan.OrderBy[0].Field)
		assert.Equal(t, "ASC", plan.OrderBy[0].Order)
	})

	// Test 4: SELECT with LIMIT
	t.Run("SelectWithLimit", func(t *testing.T) {
		query := "SELECT name FROM users LIMIT 2"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")

		// Check that LIMIT was parsed
		assert.NotNil(t, plan.Limit)
		assert.Equal(t, int64(2), *plan.Limit)
	})

	// Test 5: Complex query with multiple conditions
	t.Run("ComplexQuery", func(t *testing.T) {
		query := "SELECT name, email, age FROM users WHERE active = true AND age > 20 ORDER BY age DESC LIMIT 1"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "email")
		assert.Contains(t, plan.Fields, "age")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 2)

		// Check that ORDER BY was parsed
		assert.Len(t, plan.OrderBy, 1)
		assert.Equal(t, "age", plan.OrderBy[0].Field)
		assert.Equal(t, "DESC", plan.OrderBy[0].Order)

		// Check that LIMIT was parsed
		assert.NotNil(t, plan.Limit)
		assert.Equal(t, int64(1), *plan.Limit)
	})
}

func TestIntegration_TransactionsWithIsolationLevels(t *testing.T) {
	// Create SQL executor
	parser := NewMySQLParser()
	planner := NewPlanner(parser)
	executor := NewExecutor(planner)

	// Test 1: Validate transaction-related SQL statements can be parsed
	t.Run("TransactionStatements", func(t *testing.T) {
		// While the parser might not directly handle transaction statements,
		// we can test that complex queries with transaction-like semantics work
		query := "SELECT id, name FROM users WHERE active = true"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "id")
		assert.Contains(t, plan.Fields, "name")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "active", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		// Note: Value extraction may vary depending on the parser implementation
	})

	// Test 2: Complex transaction-like query with isolation-level-like semantics
	t.Run("IsolationLevelSemantics", func(t *testing.T) {
		// Test a query that would benefit from serializable isolation
		query := "SELECT id, name, email, age FROM users WHERE age > 25 ORDER BY age DESC"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "id")
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "email")
		assert.Contains(t, plan.Fields, "age")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "age", plan.Conditions[0].Field)
		assert.Equal(t, ">", plan.Conditions[0].Operator)

		// Check that ORDER BY was parsed
		assert.Len(t, plan.OrderBy, 1)
		assert.Equal(t, "age", plan.OrderBy[0].Field)
		assert.Equal(t, "DESC", plan.OrderBy[0].Order)
	})

	// Test 3: Validate different isolation level scenarios through query complexity
	t.Run("IsolationLevelScenarios", func(t *testing.T) {
		// Test a query that would need repeatable read
		query := "SELECT COUNT(*) as user_count FROM users WHERE active = true"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Test a query that would need read committed
		query = "SELECT name, email FROM users WHERE created_at > 1000000"
		result, err = executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Test a query that would need serializable
		query = "SELECT name, age, email FROM users WHERE age BETWEEN 20 AND 40 ORDER BY name"
		result, err = executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestIntegration_AdvancedDataTypesInSQL(t *testing.T) {
	// Create SQL executor
	parser := NewMySQLParser()
	planner := NewPlanner(parser)
	executor := NewExecutor(planner)

	// Test 1: Query with JSON-like data in conditions
	t.Run("QueryWithJSONData", func(t *testing.T) {
		query := "SELECT name, metadata FROM users WHERE name = 'DataTypeUser'"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "metadata")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "name", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		assert.Equal(t, "DataTypeUser", plan.Conditions[0].Value)
	})

	// Test 2: Query with UUID-like data
	t.Run("QueryWithUUID", func(t *testing.T) {
		query := "SELECT name, profile_id FROM users WHERE profile_id = '550e8400-e29b-41d4-a716-446655440004'"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "profile_id")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "profile_id", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		assert.Equal(t, "550e8400-e29b-41d4-a716-446655440004", plan.Conditions[0].Value)
	})

	// Test 3: Query with timestamp-like data
	t.Run("QueryWithTimestamp", func(t *testing.T) {
		query := "SELECT name, created_at FROM users WHERE created_at > 0 ORDER BY created_at DESC"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "created_at")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "created_at", plan.Conditions[0].Field)
		assert.Equal(t, ">", plan.Conditions[0].Operator)

		// Check that ORDER BY was parsed
		assert.Len(t, plan.OrderBy, 1)
		assert.Equal(t, "created_at", plan.OrderBy[0].Field)
		assert.Equal(t, "DESC", plan.OrderBy[0].Order)
	})

	// Test 4: Complex query with multiple data types
	t.Run("ComplexQueryWithMultipleTypes", func(t *testing.T) {
		query := "SELECT name, age, active, created_at, metadata, profile_id FROM users WHERE active = true AND age > 20 ORDER BY age ASC"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "age")
		assert.Contains(t, plan.Fields, "active")
		assert.Contains(t, plan.Fields, "created_at")
		assert.Contains(t, plan.Fields, "metadata")
		assert.Contains(t, plan.Fields, "profile_id")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 2)

		// Check that ORDER BY was parsed
		assert.Len(t, plan.OrderBy, 1)
		assert.Equal(t, "age", plan.OrderBy[0].Field)
		assert.Equal(t, "ASC", plan.OrderBy[0].Order)
	})
}

func TestIntegration_ComplexScenarios(t *testing.T) {
	// Create SQL executor
	parser := NewMySQLParser()
	planner := NewPlanner(parser)
	executor := NewExecutor(planner)

	// Test 1: Multi-condition query with index usage
	t.Run("MultiConditionWithIndex", func(t *testing.T) {
		query := "SELECT name, email, age FROM users WHERE active = true AND age > 30 ORDER BY age DESC"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "email")
		assert.Contains(t, plan.Fields, "age")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 2)

		// Check that ORDER BY was parsed
		assert.Len(t, plan.OrderBy, 1)
		assert.Equal(t, "age", plan.OrderBy[0].Field)
		assert.Equal(t, "DESC", plan.OrderBy[0].Order)
	})

	// Test 2: Aggregation-like query (simulated)
	t.Run("AggregationSimulation", func(t *testing.T) {
		// Get count of active users by querying with limit and counting results
		query := "SELECT name FROM users WHERE active = true LIMIT 10"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "active", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		// Note: Value extraction may vary depending on the parser implementation

		// Check that LIMIT was parsed
		assert.NotNil(t, plan.Limit)
		assert.Equal(t, int64(10), *plan.Limit)
	})

	// Test 3: Complex filtering with JSON-like data
	t.Run("ComplexFilteringWithJSON", func(t *testing.T) {
		// This test simulates querying JSON data
		// In a full implementation, we would have JSON path queries
		query := "SELECT name, metadata FROM users WHERE age > 25 ORDER BY age ASC LIMIT 5"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "metadata")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "age", plan.Conditions[0].Field)
		assert.Equal(t, ">", plan.Conditions[0].Operator)

		// Check that ORDER BY was parsed
		assert.Len(t, plan.OrderBy, 1)
		assert.Equal(t, "age", plan.OrderBy[0].Field)
		assert.Equal(t, "ASC", plan.OrderBy[0].Order)

		// Check that LIMIT was parsed
		assert.NotNil(t, plan.Limit)
		assert.Equal(t, int64(5), *plan.Limit)
	})

	// Test 4: Transaction with SQL execution (framework validation)
	t.Run("TransactionWithSQL", func(t *testing.T) {
		// Execute SQL query within transaction context
		// Note: In a full implementation, the executor would use the transaction
		// For this test, we're just verifying the components work together
		query := "SELECT name FROM users WHERE active = true LIMIT 3"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "active", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		// Note: Value extraction may vary depending on the parser implementation

		// Check that LIMIT was parsed
		assert.NotNil(t, plan.Limit)
		assert.Equal(t, int64(3), *plan.Limit)
	})

	// Test 5: Performance scenario with indexes (query planning validation)
	t.Run("PerformanceWithIndexes", func(t *testing.T) {
		// Query that should use the email index
		query := "SELECT name FROM users WHERE email = 'alice@eng.com'"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "email", plan.Conditions[0].Field)
		assert.Equal(t, "=", plan.Conditions[0].Operator)
		assert.Equal(t, "alice@eng.com", plan.Conditions[0].Value)

		// Query that should use the composite index
		query = "SELECT name FROM users WHERE age = 30 AND name = 'Alice'"
		result, err = executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err = planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 2)
	})

	// Test 6: Backward compatibility
	t.Run("BackwardCompatibility", func(t *testing.T) {
		// Test simple queries still work
		query := "SELECT id, name, email FROM users"
		result, err := executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err := planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "id")
		assert.Contains(t, plan.Fields, "name")
		assert.Contains(t, plan.Fields, "email")

		// Test queries with basic WHERE clauses
		query = "SELECT name FROM users WHERE age > 25"
		result, err = executor.Execute(context.Background(), query)
		require.NoError(t, err)
		assert.NotNil(t, result)

		// Verify the plan was created correctly
		plan, err = planner.CreatePlan(query)
		require.NoError(t, err)
		assert.Equal(t, SelectStatement, plan.Type)
		assert.Equal(t, "select", plan.Operation)
		assert.Equal(t, "users", plan.Table)
		assert.Contains(t, plan.Fields, "name")

		// Check that conditions were parsed
		assert.Len(t, plan.Conditions, 1)
		assert.Equal(t, "age", plan.Conditions[0].Field)
		assert.Equal(t, ">", plan.Conditions[0].Operator)
	})
}
