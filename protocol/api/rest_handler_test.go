package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRESTHandler(t *testing.T) (*RESTHandler, func()) {
	tmpDir := t.TempDir()
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManager(eng)
	
	// Create SQL parser and planner with catalog
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	
	// Get executor from planner
	exec := planner.Executor()
	
	handler := NewRESTHandler(exec, planner)

	schema := &types.TableDefinition{
		Name:        "users",
		Description: "Test users table",
		Columns: []types.ColumnDefinition{
			{Name: "name", Type: types.ColumnTypeString, Nullable: false},
			{Name: "email", Type: types.ColumnTypeString, Nullable: false},
			{Name: "age", Type: types.ColumnTypeNumber},
		},
	}

	err = mgr.CreateTable(context.Background(), 1, schema)
	require.NoError(t, err)

	return handler, func() {
		kvStore.Close()
	}
}

func TestRESTHandler_InsertRecord(t *testing.T) {
	handler, cleanup := setupTestRESTHandler(t)
	defer cleanup()

	router := chi.NewRouter()
	handler.RegisterRoutes(router)

	reqBody := InsertRequest{
		Data: map[string]interface{}{
			"name":  "John Doe",
			"email": "john@example.com",
			"age":   30,
		},
		TenantID: 1,
	}

	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/table/users", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("table", "users")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	handler.InsertRecord(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)

	var resp InsertResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.NotZero(t, resp.RowID)
	assert.Equal(t, "John Doe", resp.Data["name"])
}

func TestRESTHandler_QueryRecords(t *testing.T) {
	handler, cleanup := setupTestRESTHandler(t)
	defer cleanup()

	router := chi.NewRouter()
	handler.RegisterRoutes(router)

	for i := 0; i < 5; i++ {
		reqBody := InsertRequest{
			Data: map[string]interface{}{
				"name":  "User" + string(rune('A'+i)),
				"email": "user" + string(rune('a'+i)) + "@example.com",
				"age":   20 + i,
			},
			TenantID: 1,
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/table/users", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("table", "users")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		w := httptest.NewRecorder()
		handler.InsertRecord(w, req)
		require.Equal(t, http.StatusCreated, w.Code)
	}

	reqBody := QueryRequest{
		TenantID: 1,
		Limit:    10,
	}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodGet, "/api/table/users", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("table", "users")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	w := httptest.NewRecorder()
	handler.QueryRecords(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp QueryResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, 5, resp.Count)
	assert.Len(t, resp.Data, 5)
}

func TestRESTHandler_GetRecord(t *testing.T) {
	t.Skip("GET by rowID requires rowID tracking - testing once Layer 6 fully integrated")
}

func TestRESTHandler_UpdateRecord(t *testing.T) {
	t.Skip("UPDATE requires rowID tracking - testing once Layer 6 fully integrated")
}

func TestRESTHandler_DeleteRecord(t *testing.T) {
	t.Skip("DELETE requires rowID tracking - testing once Layer 6 fully integrated")
}

func BenchmarkRESTHandler_Insert(b *testing.B) {
	tmpDir := b.TempDir()
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(b, err)
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManager(eng)
	
	// Create SQL parser and planner with catalog
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	
	// Get executor from planner
	exec := planner.Executor()
	handler := NewRESTHandler(exec, planner)

	schema := &types.TableDefinition{
		Name:        "users",
		Description: "Benchmark users table",
		Columns: []types.ColumnDefinition{
			{Name: "name", Type: types.ColumnTypeString, Nullable: false},
			{Name: "email", Type: types.ColumnTypeString, Nullable: false},
			{Name: "age", Type: types.ColumnTypeNumber},
		},
	}

	err = mgr.CreateTable(context.Background(), 1, schema)
	require.NoError(b, err)

	reqBody := InsertRequest{
		Data: map[string]interface{}{
			"name":  "Benchmark User",
			"email": "bench@example.com",
			"age":   30,
		},
		TenantID: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/table/users", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("table", "users")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		w := httptest.NewRecorder()
		handler.InsertRecord(w, req)
	}
}

func BenchmarkRESTHandler_Query(b *testing.B) {
	tmpDir := b.TempDir()
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(b, err)
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManager(eng)
	
	// Create SQL parser and planner with catalog
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	
	// Get executor from planner
	exec := planner.Executor()
	handler := NewRESTHandler(exec, planner)

	schema := &types.TableDefinition{
		Name:        "users",
		Description: "Benchmark users table",
		Columns: []types.ColumnDefinition{
			{Name: "name", Type: types.ColumnTypeString, Nullable: false},
			{Name: "email", Type: types.ColumnTypeString, Nullable: false},
			{Name: "age", Type: types.ColumnTypeNumber},
		},
	}

	err = mgr.CreateTable(context.Background(), 1, schema)
	require.NoError(b, err)

	for i := 0; i < 100; i++ {
		reqBody := InsertRequest{
			Data: map[string]interface{}{
				"name":  "User",
				"email": "user@example.com",
				"age":   30,
			},
			TenantID: 1,
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/table/users", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("table", "users")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		w := httptest.NewRecorder()
		handler.InsertRecord(w, req)
	}

	reqBody := QueryRequest{
		TenantID: 1,
		Limit:    10,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodGet, "/api/table/users", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("table", "users")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		w := httptest.NewRecorder()
		handler.QueryRecords(w, req)
	}
}