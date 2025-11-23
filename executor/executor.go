package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/guileen/metabase/pkg/infra/auth/rls"
	"github.com/guileen/pqlitedb/engine"
	"github.com/guileen/pqlitedb/manager"
	"github.com/guileen/pqlitedb/table"
)

type queryExecutor struct {
	manager   manager.Manager
	engine    engine.StorageEngine
	rlsEngine *rls.RLSEngine
}

func NewExecutor(mgr manager.Manager, eng engine.StorageEngine) QueryExecutor {
	return &queryExecutor{
		manager:   mgr,
		engine:    eng,
		rlsEngine: nil,
	}
}

func NewExecutorWithRLS(mgr manager.Manager, eng engine.StorageEngine, rlsEng *rls.RLSEngine) QueryExecutor {
	return &queryExecutor{
		manager:   mgr,
		engine:    eng,
		rlsEngine: rlsEng,
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
	rlsCtx := ctx.Value("rlsContext")
	if e.rlsEngine != nil && rlsCtx != nil {
		execCtx, ok := rlsCtx.(*rls.ExecutionContext)
		if ok {
			result, err := e.rlsEngine.CheckPermission(ctx, query.TableName, "SELECT", execCtx)
			if err != nil {
				return nil, fmt.Errorf("RLS check failed: %w", err)
			}
			if !result.Allowed {
				return nil, fmt.Errorf("access denied: %s", result.Reason)
			}
		}
	}

	filters := e.convertFilters(query.Select.Where)
	qOpts := &table.QueryOptions{
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

	result, err := e.manager.Query(ctx, query.TenantID, query.TableName, qOpts)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	rows := result.Records
	if len(query.Select.OrderBy) > 0 {
		rows = e.sortRows(rows, query.Select.OrderBy)
	}

	return &QueryResult{
		Rows:    rows,
		Count:   result.Count,
		HasMore: result.HasMore,
	}, nil
}

func (e *queryExecutor) executeInsert(ctx context.Context, query *Query) (*QueryResult, error) {
	rlsCtx := ctx.Value("rlsContext")
	if e.rlsEngine != nil && rlsCtx != nil {
		execCtx, ok := rlsCtx.(*rls.ExecutionContext)
		if ok {
			data := make(map[string]interface{})
			for k, v := range query.Insert.Values {
				data[k] = v.Data
			}

			result, err := e.rlsEngine.ValidateInsert(ctx, query.TableName, data, execCtx)
			if err != nil {
				return nil, fmt.Errorf("RLS validation failed: %w", err)
			}
			if !result.Allowed {
				return nil, fmt.Errorf("access denied: %s", result.Reason)
			}
		}
	}

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
		Rows:  []*table.Record{record},
	}, nil
}

func (e *queryExecutor) executeUpdate(ctx context.Context, query *Query) (*QueryResult, error) {
	return nil, fmt.Errorf("UPDATE not yet fully implemented - needs rowID tracking")
}

func (e *queryExecutor) executeDelete(ctx context.Context, query *Query) (*QueryResult, error) {
	return nil, fmt.Errorf("DELETE not yet fully implemented - needs rowID tracking")
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

func (e *queryExecutor) sortRows(rows []*table.Record, orderBy []OrderByClause) []*table.Record {
	if len(orderBy) == 0 {
		return rows
	}

	sorted := make([]*table.Record, len(rows))
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

func (e *queryExecutor) compareRows(a, b *table.Record, orderBy []OrderByClause) int {
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

func (e *queryExecutor) compareValues(a, b *table.Value) int {
	if a == nil || b == nil {
		return 0
	}
	if a.Type != b.Type {
		return 0
	}

	switch a.Type {
	case table.ColumnTypeString:
		aStr, _ := a.Data.(string)
		bStr, _ := b.Data.(string)
		return strings.Compare(aStr, bStr)

	case table.ColumnTypeNumber:
		aNum, _ := a.Data.(float64)
		bNum, _ := b.Data.(float64)
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0

	case table.ColumnTypeBoolean:
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
