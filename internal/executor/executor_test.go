package executor

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/internal/codec"
	"github.com/guileen/pglitedb/internal/engine"
	"github.com/guileen/pglitedb/internal/kv"
	"github.com/guileen/pglitedb/internal/manager"
	"github.com/guileen/pglitedb/internal/table"
)

func setupTestExecutor(t *testing.T) (QueryExecutor, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	config := kv.DefaultPebbleConfig(tmpDir)
	config.FlushInterval = time.Second
	kvStore, err := kv.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("failed to create KV store: %v", err)
	}

	c := codec.NewMemComparableCodec()
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := manager.NewTableManager(eng)
	exec := NewExecutor(mgr, eng)

	ctx := context.Background()
	schema := &table.TableDefinition{
		Name:   "users",
		Schema: "tenant_1",
		Columns: []table.ColumnDefinition{
			{Name: "name", Type: table.ColumnTypeString, Nullable: false},
			{Name: "email", Type: table.ColumnTypeString, Nullable: false},
			{Name: "age", Type: table.ColumnTypeNumber, Nullable: true},
			{Name: "active", Type: table.ColumnTypeBoolean, Nullable: true},
		},
	}

	err = mgr.CreateTable(ctx, 1, schema)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	
	// Create index on name column for ORDER BY testing
	indexDef := &table.IndexDefinition{
		Name:    "idx_users_name",
		Columns: []string{"name"},
		Unique:  false,
		Type:    "btree",
	}
	
	err = mgr.CreateIndex(ctx, 1, "users", indexDef)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	return exec, func() { kvStore.Close() }
}

func TestQueryExecutor_Select(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	insertQuery := &Query{
		Type:      QueryTypeInsert,
		TableName: "users",
		TenantID:  1,
		Insert: &InsertQuery{
			Values: map[string]*table.Value{
				"name":   {Type: table.ColumnTypeString, Data: "Alice"},
				"email":  {Type: table.ColumnTypeString, Data: "alice@example.com"},
				"age":    {Type: table.ColumnTypeNumber, Data: float64(30)},
				"active": {Type: table.ColumnTypeBoolean, Data: true},
			},
		},
	}

	_, err := exec.Execute(ctx, insertQuery)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	selectQuery := &Query{
		Type:      QueryTypeSelect,
		TableName: "users",
		TenantID:  1,
		Select: &SelectQuery{
			Columns: []string{"name", "email"},
			Limit:   10,
		},
	}

	result, err := exec.Execute(ctx, selectQuery)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}

	if result.Count != 1 {
		t.Errorf("expected 1 row, got %d", result.Count)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if row["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", row["name"])
	}

	if row["email"] != "alice@example.com" {
		t.Errorf("expected email=alice@example.com, got %v", row["email"])
	}
}

func TestQueryExecutor_Insert(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	query := &Query{
		Type:      QueryTypeInsert,
		TableName: "users",
		TenantID:  1,
		Insert: &InsertQuery{
			Values: map[string]*table.Value{
				"name":   {Type: table.ColumnTypeString, Data: "Bob"},
				"email":  {Type: table.ColumnTypeString, Data: "bob@example.com"},
				"age":    {Type: table.ColumnTypeNumber, Data: float64(25)},
				"active": {Type: table.ColumnTypeBoolean, Data: false},
			},
		},
	}

	result, err := exec.Execute(ctx, query)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	if result.Count != 1 {
		t.Errorf("expected count=1, got %d", result.Count)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0]["__rowid__"] == "" {
		t.Errorf("expected valid row ID, got empty string")
	}
}

func TestQueryExecutor_Update(t *testing.T) {
	t.Skip("UPDATE requires rowID tracking - will be implemented in Layer 6 REST API")
}

func TestQueryExecutor_Delete(t *testing.T) {
	t.Skip("DELETE requires rowID tracking - will be implemented in Layer 6 REST API")
}

func TestQueryExecutor_Explain(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	query := &Query{
		Type:      QueryTypeSelect,
		TableName: "users",
		TenantID:  1,
		Select: &SelectQuery{
			Columns: []string{"name", "email"},
			Where: []Filter{
				{Column: "active", Operator: OpEqual, Value: true},
			},
			OrderBy: []OrderByClause{
				{Column: "name", Descending: false},
			},
			Limit: 10,
		},
	}

	plan, err := exec.Explain(ctx, query)
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}

	if len(plan.Steps) == 0 {
		t.Errorf("expected plan steps, got none")
	}

	if plan.EstimatedCost == 0 {
		t.Errorf("expected non-zero cost, got 0")
	}

	expectedSteps := []string{"TABLE_SCAN", "FILTER", "SORT", "PROJECT"}
	if len(plan.Steps) != len(expectedSteps) {
		t.Errorf("expected %d steps, got %d", len(expectedSteps), len(plan.Steps))
	}

	for i, step := range plan.Steps {
		if step.Operation != expectedSteps[i] {
			t.Errorf("step %d: expected %s, got %s", i, expectedSteps[i], step.Operation)
		}
	}
}

func TestQueryExecutor_OrderBy(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	ctx := context.Background()

	users := []struct {
		name  string
		email string
		age   float64
	}{
		{"Zara", "zara@example.com", 28},
		{"Alice", "alice@example.com", 30},
		{"Bob", "bob@example.com", 25},
	}

	for _, u := range users {
		insertQuery := &Query{
			Type:      QueryTypeInsert,
			TableName: "users",
			TenantID:  1,
			Insert: &InsertQuery{
				Values: map[string]*table.Value{
					"name":   {Type: table.ColumnTypeString, Data: u.name},
					"email":  {Type: table.ColumnTypeString, Data: u.email},
					"age":    {Type: table.ColumnTypeNumber, Data: u.age},
					"active": {Type: table.ColumnTypeBoolean, Data: true},
				},
			},
		}
		_, err := exec.Execute(ctx, insertQuery)
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	selectQuery := &Query{
		Type:      QueryTypeSelect,
		TableName: "users",
		TenantID:  1,
		Select: &SelectQuery{
			OrderBy: []OrderByClause{
				{Column: "name", Descending: false},
			},
		},
	}

	result, err := exec.Execute(ctx, selectQuery)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}
	
	expectedNames := []string{"Alice", "Bob", "Zara"}
	for i, row := range result.Rows {
		name, _ := row["name"].(string)
		if name != expectedNames[i] {
			t.Errorf("row %d: expected name=%s, got %s", i, expectedNames[i], name)
		}
	}
}

func BenchmarkQueryExecutor_Select(b *testing.B) {
	kvStore, err := kv.NewPebbleKV(&kv.PebbleConfig{
		Path:          b.TempDir(),
		CacheSize:     64 * 1024 * 1024,
		MemTableSize:  8 * 1024 * 1024,
		FlushInterval: time.Second,
	})
	if err != nil {
		b.Fatalf("failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := manager.NewTableManager(eng)
	exec := NewExecutor(mgr, eng)

	ctx := context.Background()
	schema := &table.TableDefinition{
		Name:   "users",
		Schema: "tenant_1",
		Columns: []table.ColumnDefinition{
			{Name: "name", Type: table.ColumnTypeString, Nullable: false},
			{Name: "email", Type: table.ColumnTypeString, Nullable: false},
			{Name: "age", Type: table.ColumnTypeNumber, Nullable: true},
		},
	}

	err = mgr.CreateTable(ctx, 1, schema)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	insertQuery := &Query{
		Type:      QueryTypeInsert,
		TableName: "users",
		TenantID:  1,
		Insert: &InsertQuery{
			Values: map[string]*table.Value{
				"name":  {Type: table.ColumnTypeString, Data: "Test User"},
				"email": {Type: table.ColumnTypeString, Data: "test@example.com"},
				"age":   {Type: table.ColumnTypeNumber, Data: float64(30)},
			},
		},
	}

	_, err = exec.Execute(ctx, insertQuery)
	if err != nil {
		b.Fatalf("insert failed: %v", err)
	}

	b.ResetTimer()

	selectQuery := &Query{
		Type:      QueryTypeSelect,
		TableName: "users",
		TenantID:  1,
		Select: &SelectQuery{
			Limit: 10,
		},
	}

	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(ctx, selectQuery)
		if err != nil {
			b.Fatalf("select failed: %v", err)
		}
	}
}

func BenchmarkQueryExecutor_Insert(b *testing.B) {
	kvStore, err := kv.NewPebbleKV(&kv.PebbleConfig{
		Path:          b.TempDir(),
		CacheSize:     64 * 1024 * 1024,
		MemTableSize:  8 * 1024 * 1024,
		FlushInterval: time.Second,
	})
	if err != nil {
		b.Fatalf("failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := manager.NewTableManager(eng)
	exec := NewExecutor(mgr, eng)

	ctx := context.Background()
	schema := &table.TableDefinition{
		Name:   "users",
		Schema: "tenant_1",
		Columns: []table.ColumnDefinition{
			{Name: "name", Type: table.ColumnTypeString, Nullable: false},
			{Name: "email", Type: table.ColumnTypeString, Nullable: false},
			{Name: "age", Type: table.ColumnTypeNumber, Nullable: true},
		},
	}

	err = mgr.CreateTable(ctx, 1, schema)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		insertQuery := &Query{
			Type:      QueryTypeInsert,
			TableName: "users",
			TenantID:  1,
			Insert: &InsertQuery{
				Values: map[string]*table.Value{
					"name":  {Type: table.ColumnTypeString, Data: "Test User"},
					"email": {Type: table.ColumnTypeString, Data: "test@example.com"},
					"age":   {Type: table.ColumnTypeNumber, Data: float64(30)},
				},
			},
		}

		_, err := exec.Execute(ctx, insertQuery)
		if err != nil {
			b.Fatalf("insert failed: %v", err)
		}
	}
}
