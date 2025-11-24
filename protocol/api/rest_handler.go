package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/guileen/pglitedb/protocol/executor"
	"github.com/guileen/pglitedb/types"
)

type RESTHandler struct {
	executor executor.QueryExecutor
}

func NewRESTHandler(exec executor.QueryExecutor) *RESTHandler {
	return &RESTHandler{
		executor: exec,
	}
}

func (h *RESTHandler) RegisterRoutes(r chi.Router) {
	r.Route("/api/table/{table}", func(r chi.Router) {
		r.Get("/", h.QueryRecords)
		r.Post("/", h.InsertRecord)
		r.Patch("/{rowID}", h.UpdateRecord)
		r.Delete("/{rowID}", h.DeleteRecord)
		r.Get("/{rowID}", h.GetRecord)
	})
}

type QueryRequest struct {
	Select   []string               `json:"select,omitempty"`
	Where    map[string]interface{} `json:"where,omitempty"`
	OrderBy  []string               `json:"order_by,omitempty"`
	Limit    int                    `json:"limit,omitempty"`
	Offset   int                    `json:"offset,omitempty"`
	TenantID int64                  `json:"tenant_id,omitempty"`
}

type QueryResponse struct {
	Data    []map[string]interface{} `json:"data"`
	Count   int                      `json:"count"`
	HasMore bool                     `json:"has_more,omitempty"`
}

type InsertRequest struct {
	Data     map[string]interface{} `json:"data"`
	TenantID int64                  `json:"tenant_id,omitempty"`
}

type InsertResponse struct {
	RowID int64                  `json:"row_id"`
	Data  map[string]interface{} `json:"data"`
}

type UpdateRequest struct {
	Data     map[string]interface{} `json:"data"`
	TenantID int64                  `json:"tenant_id,omitempty"`
}

type DeleteRequest struct {
	TenantID int64 `json:"tenant_id,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func (h *RESTHandler) QueryRecords(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "table")

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		req.TenantID = getTenantID(r)
		req.Limit = getIntQueryParam(r, "limit", 10)
		req.Offset = getIntQueryParam(r, "offset", 0)
	}

	if req.TenantID == 0 {
		req.TenantID = getTenantID(r)
	}

	orderByClauses := make([]executor.OrderByClause, 0, len(req.OrderBy))
	for _, field := range req.OrderBy {
		orderByClauses = append(orderByClauses, executor.OrderByClause{
			Column: field,
		})
	}

	selectQuery := &executor.SelectQuery{
		Columns: req.Select,
		Where:   convertWhere(req.Where),
		OrderBy: orderByClauses,
		Limit:   req.Limit,
		Offset:  req.Offset,
	}

	query := &executor.Query{
		Type:      executor.QueryTypeSelect,
		TableName: tableName,
		TenantID:  req.TenantID,
		Select:    selectQuery,
	}

	result, err := h.executor.Execute(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	rows := make([]map[string]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		// row is already a map[string]interface{}, no need to convert
		rows[i] = row
	}

	response := &QueryResponse{
		Data:    rows,
		Count:   int(result.Count),
		HasMore: result.HasMore,
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *RESTHandler) InsertRecord(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "table")

	var req InsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))
		return
	}

	if req.TenantID == 0 {
		req.TenantID = getTenantID(r)
	}

	convertedValues := make(map[string]*types.Value)
	for k, v := range req.Data {
		convertedValues[k] = &types.Value{Data: v}
	}

	insertQuery := &executor.InsertQuery{
		Values: convertedValues,
	}

	query := &executor.Query{
		Type:      executor.QueryTypeInsert,
		TableName: tableName,
		TenantID:  req.TenantID,
		Insert:    insertQuery,
	}

	result, err := h.executor.Execute(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	response := &InsertResponse{
		RowID: result.Count,
		Data:  req.Data,
	}

	writeJSON(w, http.StatusCreated, response)
}

func (h *RESTHandler) UpdateRecord(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "table")
	rowIDStr := chi.URLParam(r, "rowID")

	rowID, err := strconv.ParseInt(rowIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid row ID: %w", err))
		return
	}

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))
		return
	}

	if req.TenantID == 0 {
		req.TenantID = getTenantID(r)
	}

	convertedValues := make(map[string]*types.Value)
	for k, v := range req.Data {
		convertedValues[k] = &types.Value{Data: v}
	}

	updateQuery := &executor.UpdateQuery{
		Where: []executor.Filter{
			{Column: "__rowid__", Operator: executor.OpEqual, Value: rowID},
		},
		Values: convertedValues,
	}

	query := &executor.Query{
		Type:      executor.QueryTypeUpdate,
		TableName: tableName,
		TenantID:  req.TenantID,
		Update:    updateQuery,
	}

	result, err := h.executor.Execute(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"updated": result.Count,
		"data":    req.Data,
	})
}

func (h *RESTHandler) DeleteRecord(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "table")
	rowIDStr := chi.URLParam(r, "rowID")

	rowID, err := strconv.ParseInt(rowIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid row ID: %w", err))
		return
	}

	tenantID := getTenantID(r)

	deleteQuery := &executor.DeleteQuery{
		Where: []executor.Filter{
			{Column: "__rowid__", Operator: executor.OpEqual, Value: rowID},
		},
	}

	query := &executor.Query{
		Type:      executor.QueryTypeDelete,
		TableName: tableName,
		TenantID:  tenantID,
		Delete:    deleteQuery,
	}

	result, err := h.executor.Execute(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"deleted": result.Count,
	})
}

func (h *RESTHandler) GetRecord(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "table")
	rowIDStr := chi.URLParam(r, "rowID")

	rowID, err := strconv.ParseInt(rowIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid row ID: %w", err))
		return
	}

	tenantID := getTenantID(r)

	selectQuery := &executor.SelectQuery{
		Where: []executor.Filter{
			{Column: "__rowid__", Operator: executor.OpEqual, Value: rowID},
		},
		Limit: 1,
	}

	query := &executor.Query{
		Type:      executor.QueryTypeSelect,
		TableName: tableName,
		TenantID:  tenantID,
		Select:    selectQuery,
	}

	result, err := h.executor.Execute(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if len(result.Rows) == 0 {
		writeError(w, http.StatusNotFound, fmt.Errorf("record not found"))
		return
	}

	recordData := result.Rows[0]

	writeJSON(w, http.StatusOK, recordData)
}

func convertWhere(where map[string]interface{}) []executor.Filter {
	if where == nil {
		return nil
	}

	filters := make([]executor.Filter, 0, len(where))
	for field, value := range where {
		filters = append(filters, executor.Filter{
			Column:   field,
			Operator: executor.OpEqual,
			Value:    value,
		})
	}
	return filters
}

func getTenantID(r *http.Request) int64 {
	if tenantIDStr := r.Header.Get("X-Tenant-ID"); tenantIDStr != "" {
		if tenantID, err := strconv.ParseInt(tenantIDStr, 10, 64); err == nil {
			return tenantID
		}
	}

	return 1
}

func getIntQueryParam(r *http.Request, param string, defaultValue int) int {
	if value := r.URL.Query().Get(param); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, &ErrorResponse{Error: err.Error()})
}
