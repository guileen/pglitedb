package sql

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
)

type Executor struct {
	planner *Planner
	catalog catalog.Manager
}

type ResultSet struct {
	Columns []string
	Rows    [][]interface{}
	Count   int
}

func NewExecutor(planner *Planner) *Executor {
	return &Executor{
		planner: planner,
	}
}

func NewExecutorWithCatalog(planner *Planner, catalog catalog.Manager) *Executor {
	return &Executor{
		planner: planner,
		catalog: catalog,
	}
}

func (e *Executor) Execute(ctx context.Context, query string) (*ResultSet, error) {
	plan, err := e.planner.CreatePlan(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	switch plan.Operation {
	case "select":
		return e.executeSelect(ctx, plan)
	case "ddl":
		return e.executeDDL(ctx, query)
	default:
		return nil, fmt.Errorf("unsupported operation: %v", plan.Operation)
	}
}

func (e *Executor) executeDDL(ctx context.Context, query string) (*ResultSet, error) {
	return &ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

func (e *Executor) executeSelect(ctx context.Context, plan *Plan) (*ResultSet, error) {
	if e.catalog == nil {
		result := &ResultSet{
			Columns: plan.Fields,
			Count:   0,
		}
		result.Rows = append(result.Rows, []interface{}{"mock_value1", "mock_value2"})
		result.Count = len(result.Rows)
		return result, nil
	}

	tenantID := int64(1)

	orderByStrings := make([]string, len(plan.OrderBy))
	for i, ob := range plan.OrderBy {
		orderByStrings[i] = ob.Field
	}

	var limit, offset *int
	if plan.Limit != nil {
		l := int(*plan.Limit)
		limit = &l
	}
	if plan.Offset != nil {
		o := int(*plan.Offset)
		offset = &o
	}

	opts := &types.QueryOptions{
		Columns: plan.Fields,
		OrderBy: orderByStrings,
		Limit:   limit,
		Offset:  offset,
	}

	queryResult, err := e.catalog.Query(ctx, tenantID, plan.Table, opts)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	result := &ResultSet{
		Columns: plan.Fields,
		Rows:    make([][]interface{}, len(queryResult.Rows)),
		Count:   int(queryResult.Count),
	}

	for i, row := range queryResult.Rows {
		rowData := make([]interface{}, len(plan.Fields))
		for j, col := range plan.Fields {
			if val, ok := row[col]; ok {
				rowData[j] = val
			}
		}
		result.Rows[i] = rowData
	}

	return result, nil
}

func (e *Executor) ExecuteParsed(ctx context.Context, parsed *ParsedQuery) (*ResultSet, error) {
	plan, err := e.planner.CreatePlan(parsed.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	switch plan.Type {
	case SelectStatement:
		return e.executeSelect(ctx, plan)
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", plan.Type)
	}
}

func (e *Executor) ValidateQuery(query string) error {
	_, err := e.planner.CreatePlan(query)
	return err
}

func (e *Executor) Explain(query string) (*Plan, error) {
	return e.planner.CreatePlan(query)
}