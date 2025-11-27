package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
)

type queryExecutor struct {
	manager catalog.Manager
	engine  engine.StorageEngine
}

// GetCatalog returns the catalog manager
func (e *queryExecutor) GetCatalog() catalog.Manager {
	return e.manager
}

func NewExecutor(mgr catalog.Manager, eng engine.StorageEngine) QueryExecutor {
	return &queryExecutor{
		manager: mgr,
		engine:  eng,
	}
}

func (e *queryExecutor) Execute(ctx context.Context, query *Query) (*QueryResult, error) {
	if err := e.ValidateQuery(query); err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}

	switch query.Type {
	case QueryTypeSelect:
		return e.executeSelect(ctx, query)
	case QueryTypeInsert:
		return e.executeInsert(ctx, query)
	case QueryTypeUpdate:
		return e.executeUpdate(ctx, query)
	case QueryTypeDelete:
		return e.executeDelete(ctx, query)
	default:
		return nil, fmt.Errorf("unsupported query type: %d", query.Type)
	}
}

func (e *queryExecutor) executeSelect(ctx context.Context, query *Query) (*QueryResult, error) {
	filters := e.convertFilters(query.Select.Where)
	qOpts := &types.QueryOptions{
		Where: filters,
	}
	if query.Select.Limit > 0 {
		qOpts.Limit = &query.Select.Limit
	}
	if query.Select.Offset > 0 {
		qOpts.Offset = &query.Select.Offset
	}
	if len(query.Select.Columns) > 0 {
		qOpts.Columns = query.Select.Columns
	}
	// Add ORDER BY to query options
	if len(query.Select.OrderBy) > 0 {
		orderBy := make([]string, len(query.Select.OrderBy))
		for i, clause := range query.Select.OrderBy {
			orderBy[i] = clause.Column
		}
		qOpts.OrderBy = orderBy
	}
	result, err := e.manager.Query(ctx, query.TenantID, query.TableName, qOpts)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	return &QueryResult{
		Rows:    convertRecordsToRows(result.Records),
		Count:   result.Count,
		HasMore: result.HasMore,
	}, nil
}

func (e *queryExecutor) executeInsert(ctx context.Context, query *Query) (*QueryResult, error) {
	data := make(map[string]interface{})
	for k, v := range query.Insert.Values {
		data[k] = v.Data
	}

	record, err := e.manager.Insert(ctx, query.TenantID, query.TableName, data)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	return &QueryResult{
		Count: 1,
		Rows:  convertRecordsToRows([]*types.Record{record}),
	}, nil
}

func (e *queryExecutor) executeUpdate(ctx context.Context, query *Query) (*QueryResult, error) {
	filters := e.convertFilters(query.Update.Where)
	qOpts := &types.QueryOptions{
		Where: filters,
	}
	
	result, err := e.manager.Query(ctx, query.TenantID, query.TableName, qOpts)
	if err != nil {
		return nil, fmt.Errorf("query for update failed: %w", err)
	}
	
	records, ok := result.Records.([]*types.Record)
	if !ok {
		return nil, fmt.Errorf("invalid records type")
	}
	
	updatedCount := int64(0)
	for _, record := range records {
		rowIDValue, ok := record.Data["_rowid"]
		if !ok {
			continue
		}
		rowID, ok := rowIDValue.Data.(int64)
		if !ok {
			continue
		}
		
		updateData := make(map[string]interface{})
		for k, v := range query.Update.Values {
			updateData[k] = v.Data
		}
		
		_, err := e.manager.Update(ctx, query.TenantID, query.TableName, rowID, updateData)
		if err != nil {
			return nil, fmt.Errorf("update row %d: %w", rowID, err)
		}
		updatedCount++
	}
	
	return &QueryResult{
		Count: updatedCount,
	}, nil
}

func (e *queryExecutor) executeDelete(ctx context.Context, query *Query) (*QueryResult, error) {
	filters := e.convertFilters(query.Delete.Where)
	qOpts := &types.QueryOptions{
		Where: filters,
	}
	
	result, err := e.manager.Query(ctx, query.TenantID, query.TableName, qOpts)
	if err != nil {
		return nil, fmt.Errorf("query for delete failed: %w", err)
	}
	
	records, ok := result.Records.([]*types.Record)
	if !ok {
		return nil, fmt.Errorf("invalid records type")
	}
	
	deletedCount := int64(0)
	for _, record := range records {
		rowIDValue, ok := record.Data["_rowid"]
		if !ok {
			continue
		}
		rowID, ok := rowIDValue.Data.(int64)
		if !ok {
			continue
		}
		
		err := e.manager.Delete(ctx, query.TenantID, query.TableName, rowID)
		if err != nil {
			return nil, fmt.Errorf("delete row %d: %w", rowID, err)
		}
		deletedCount++
	}
	
	return &QueryResult{
		Count: deletedCount,
	}, nil
}

// convertRecordsToRows converts []*types.Record to [][]interface{}
func convertRecordsToRows(records interface{}) [][]interface{} {
	if records == nil {
		return [][]interface{}{}
	}
	
	tableRecords, ok := records.([]*types.Record)
	if !ok {
		return [][]interface{}{}
	}
	
	if len(tableRecords) == 0 {
		return [][]interface{}{}
	}
	
	// Extract column names from first record to maintain consistent column order
	var columnNames []string
	for key := range tableRecords[0].Data {
		columnNames = append(columnNames, key)
	}
	
	rows := make([][]interface{}, len(tableRecords))
	for i, record := range tableRecords {
		row := make([]interface{}, len(columnNames))
		for j, colName := range columnNames {
			if value, ok := record.Data[colName]; ok && value != nil {
				row[j] = value.Data
			} else {
				row[j] = nil
			}
		}
		rows[i] = row
	}
	
	return rows
}

func (e *queryExecutor) Explain(ctx context.Context, query *Query) (*QueryPlan, error) {
	if err := e.ValidateQuery(query); err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}

	plan := &QueryPlan{
		Steps: []PlanStep{},
	}

	switch query.Type {
	case QueryTypeSelect:
		plan.Steps = append(plan.Steps, PlanStep{
			Operation:   "TABLE_SCAN",
			Description: fmt.Sprintf("Scan table %s", query.TableName),
			Cost:        100.0,
		})

		if len(query.Select.Where) > 0 {
			plan.Steps = append(plan.Steps, PlanStep{
				Operation:   "FILTER",
				Description: fmt.Sprintf("Apply %d filters", len(query.Select.Where)),
				Cost:        10.0 * float64(len(query.Select.Where)),
			})
		}

		if len(query.Select.OrderBy) > 0 {
			plan.Steps = append(plan.Steps, PlanStep{
				Operation:   "SORT",
				Description: fmt.Sprintf("Sort by %d columns", len(query.Select.OrderBy)),
				Cost:        50.0,
			})
		}

		if len(query.Select.Columns) > 0 {
			plan.Steps = append(plan.Steps, PlanStep{
				Operation:   "PROJECT",
				Description: fmt.Sprintf("Project %d columns", len(query.Select.Columns)),
				Cost:        5.0,
			})
		}

	case QueryTypeInsert:
		plan.Steps = append(plan.Steps, PlanStep{
			Operation:   "INSERT",
			Description: fmt.Sprintf("Insert into %s", query.TableName),
			Cost:        20.0,
		})

	case QueryTypeUpdate:
		plan.Steps = append(plan.Steps, PlanStep{
			Operation:   "TABLE_SCAN",
			Description: fmt.Sprintf("Scan table %s", query.TableName),
			Cost:        100.0,
		})
		plan.Steps = append(plan.Steps, PlanStep{
			Operation:   "UPDATE",
			Description: fmt.Sprintf("Update %d columns", len(query.Update.Values)),
			Cost:        30.0,
		})

	case QueryTypeDelete:
		plan.Steps = append(plan.Steps, PlanStep{
			Operation:   "TABLE_SCAN",
			Description: fmt.Sprintf("Scan table %s", query.TableName),
			Cost:        100.0,
		})
		plan.Steps = append(plan.Steps, PlanStep{
			Operation:   "DELETE",
			Description: "Delete matching rows",
			Cost:        20.0,
		})
	}

	totalCost := 0.0
	for _, step := range plan.Steps {
		totalCost += step.Cost
	}
	plan.EstimatedCost = totalCost

	return plan, nil
}

func (e *queryExecutor) ValidateQuery(query *Query) error {
	if query.TableName == "" {
		return fmt.Errorf("table name is required")
	}

	switch query.Type {
	case QueryTypeSelect:
		if query.Select == nil {
			return fmt.Errorf("select query is nil")
		}

	case QueryTypeInsert:
		if query.Insert == nil {
			return fmt.Errorf("insert query is nil")
		}
		if len(query.Insert.Values) == 0 {
			return fmt.Errorf("insert values are empty")
		}

	case QueryTypeUpdate:
		if query.Update == nil {
			return fmt.Errorf("update query is nil")
		}
		if len(query.Update.Values) == 0 {
			return fmt.Errorf("update values are empty")
		}

	case QueryTypeDelete:
		if query.Delete == nil {
			return fmt.Errorf("delete query is nil")
		}
	}

	return nil
}

func (e *queryExecutor) convertFilters(filters []Filter) map[string]interface{} {
	result := make(map[string]interface{})
	for _, f := range filters {
		if f.Operator == OpEqual {
			result[f.Column] = f.Value
		}
	}
	return result
}

func (e *queryExecutor) sortRows(rows []*types.Record, orderBy []OrderByClause) []*types.Record {
	if len(orderBy) == 0 {
		return rows
	}

	sorted := make([]*types.Record, len(rows))
	copy(sorted, rows)

	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if e.compareRows(sorted[i], sorted[j], orderBy) > 0 {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

func (e *queryExecutor) compareRows(a, b *types.Record, orderBy []OrderByClause) int {
	for _, clause := range orderBy {
		aVal, aOk := a.Data[clause.Column]
		bVal, bOk := b.Data[clause.Column]

		if !aOk && !bOk {
			continue
		}
		if !aOk {
			return -1
		}
		if !bOk {
			return 1
		}

		cmp := e.compareValues(aVal, bVal)
		if clause.Descending {
			cmp = -cmp
		}

		if cmp != 0 {
			return cmp
		}
	}

	return 0
}

func (e *queryExecutor) compareValues(a, b *types.Value) int {
	if a == nil || b == nil {
		return 0
	}
	if a.Type != b.Type {
		return 0
	}

	switch a.Type {
	case types.ColumnTypeString:
		aStr, _ := a.Data.(string)
		bStr, _ := b.Data.(string)
		return strings.Compare(aStr, bStr)

	case types.ColumnTypeNumber:
		aNum, _ := a.Data.(float64)
		bNum, _ := b.Data.(float64)
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0

	case types.ColumnTypeBoolean:
		aBool, _ := a.Data.(bool)
		bBool, _ := b.Data.(bool)
		if !aBool && bBool {
			return -1
		} else if aBool && !bBool {
			return 1
		}
		return 0

	default:
		return 0
	}
}