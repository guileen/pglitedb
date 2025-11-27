package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/guileen/pglitedb/protocol/sql"
)

type RESTHandler struct {
	executor *sql.Executor
	planner  *sql.Planner
}

func NewRESTHandler(exec *sql.Executor, planner *sql.Planner) *RESTHandler {
	return &RESTHandler{
		executor: exec,
		planner:  planner,
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

	// Generate SQL query
	var sb strings.Builder
	sb.WriteString("SELECT ")
	
	if len(req.Select) == 0 {
		sb.WriteString("*")
	} else {
		sb.WriteString(strings.Join(req.Select, ", "))
	}
	
	sb.WriteString(fmt.Sprintf(" FROM %s", tableName))
	
	// Convert where conditions to SQL WHERE clause
	if req.Where != nil && len(req.Where) > 0 {
		sb.WriteString(" WHERE ")
		whereClauses := make([]string, 0, len(req.Where))
		for column, value := range req.Where {
			switch v := value.(type) {
			case string:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = '%s'", column, v))
			case int, int32, int64:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %v", column, v))
			case float32, float64:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %v", column, v))
			case bool:
				if v {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = true", column))
				} else {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = false", column))
				}
			default:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = '%v'", column, v))
			}
		}
		sb.WriteString(strings.Join(whereClauses, " AND "))
	}
	
	// Add ORDER BY clause
	if len(req.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(req.OrderBy, ", "))
	}
	
	// Add LIMIT and OFFSET clauses
	if req.Limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", req.Limit))
	}
	
	if req.Offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", req.Offset))
	}
	
	sqlQuery := sb.String()
	
	resultSet, err := h.executor.Execute(r.Context(), sqlQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Convert [][]interface{} to []map[string]interface{} for JSON response
	rows := make([]map[string]interface{}, len(resultSet.Rows))
	for i, row := range resultSet.Rows {
		rowMap := make(map[string]interface{})
		for j, col := range resultSet.Columns {
			if j < len(row) {
				rowMap[col] = row[j]
			} else {
				rowMap[col] = nil
			}
		}
		rows[i] = rowMap
	}

	response := QueryResponse{
		Data:    rows,
		Count:   resultSet.Count,
		HasMore: false, // Placeholder
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

	// Convert data map to SQL INSERT statement
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (", tableName))
	
	columns := make([]string, 0, len(req.Data))
	values := make([]string, 0, len(req.Data))
	
	for column, value := range req.Data {
		columns = append(columns, column)
		switch v := value.(type) {
		case string:
			values = append(values, fmt.Sprintf("'%s'", v))
		case int, int32, int64:
			values = append(values, fmt.Sprintf("%v", v))
		case float32, float64:
			values = append(values, fmt.Sprintf("%v", v))
		case bool:
			if v {
				values = append(values, "true")
			} else {
				values = append(values, "false")
			}
		default:
			values = append(values, fmt.Sprintf("'%v'", v))
		}
	}
	
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES (")
	sb.WriteString(strings.Join(values, ", "))
	sb.WriteString(")")
	
	sqlQuery := sb.String()
	
	resultSet, err := h.executor.Execute(r.Context(), sqlQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	response := InsertResponse{
		RowID: resultSet.LastInsertID,
		Data:  req.Data,
	}

	writeJSON(w, http.StatusCreated, response)
}

func (h *RESTHandler) UpdateRecord(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "table")
	rowID := chi.URLParam(r, "rowID")

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))
		return
	}

	if req.TenantID == 0 {
		req.TenantID = getTenantID(r)
	}

	// Generate SQL UPDATE statement
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("UPDATE %s SET ", tableName))
	
	// Convert data map to SET clause
	setClauses := make([]string, 0, len(req.Data))
	for column, value := range req.Data {
		switch v := value.(type) {
		case string:
			setClauses = append(setClauses, fmt.Sprintf("%s = '%s'", column, v))
		case int, int32, int64:
			setClauses = append(setClauses, fmt.Sprintf("%s = %v", column, v))
		case float32, float64:
			setClauses = append(setClauses, fmt.Sprintf("%s = %v", column, v))
		case bool:
			if v {
				setClauses = append(setClauses, fmt.Sprintf("%s = true", column))
			} else {
				setClauses = append(setClauses, fmt.Sprintf("%s = false", column))
			}
		default:
			setClauses = append(setClauses, fmt.Sprintf("%s = '%v'", column, v))
		}
	}
	
	sb.WriteString(strings.Join(setClauses, ", "))
	sb.WriteString(fmt.Sprintf(" WHERE id = %s", rowID))
	
	sqlQuery := sb.String()
	
	resultSet, err := h.executor.Execute(r.Context(), sqlQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	response := map[string]interface{}{
		"updated_rows": resultSet.Count,
		"data":         req.Data,
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *RESTHandler) DeleteRecord(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "table")
	rowID := chi.URLParam(r, "rowID")

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// If no body, use query params or default
		req.TenantID = getTenantID(r)
	}

	if req.TenantID == 0 {
		req.TenantID = getTenantID(r)
	}

	// Generate SQL DELETE statement
	sqlQuery := fmt.Sprintf("DELETE FROM %s WHERE id = %s", tableName, rowID)
	
	resultSet, err := h.executor.Execute(r.Context(), sqlQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	response := map[string]interface{}{
		"deleted_rows": resultSet.Count,
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *RESTHandler) GetRecord(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "table")
	rowID := chi.URLParam(r, "rowID")

	// Generate SQL SELECT statement
	sqlQuery := fmt.Sprintf("SELECT * FROM %s WHERE id = %s", tableName, rowID)
	
	resultSet, err := h.executor.Execute(r.Context(), sqlQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if len(resultSet.Rows) == 0 {
		writeError(w, http.StatusNotFound, fmt.Errorf("record not found"))
		return
	}

	// Convert first row to map
	rowMap := make(map[string]interface{})
	row := resultSet.Rows[0]
	for j, col := range resultSet.Columns {
		if j < len(row) {
			rowMap[col] = row[j]
		} else {
			rowMap[col] = nil
		}
	}

	writeJSON(w, http.StatusOK, rowMap)
}

// Helper functions
func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
}

func getTenantID(r *http.Request) int64 {
	// In a real implementation, this would extract tenant ID from auth context
	// For now, we'll use a default tenant ID
	return 1
}

func getIntQueryParam(r *http.Request, key string, defaultValue int) int {
	valueStr := r.URL.Query().Get(key)
	if valueStr == "" {
		return defaultValue
	}
	
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	
	return value
}