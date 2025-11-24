package operators

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type AggFunction interface {
	Init()
	Update(value *types.Value) error
	Finalize() (*types.Value, error)
	Column() string
	Alias() string
}

type CountAgg struct {
	count  int64
	column string
	alias  string
}

func NewCountAgg(column, alias string) *CountAgg {
	return &CountAgg{column: column, alias: alias}
}

func (a *CountAgg) Init() {
	a.count = 0
}

func (a *CountAgg) Update(v *types.Value) error {
	if a.column == "*" || v.Data != nil {
		a.count++
	}
	return nil
}

func (a *CountAgg) Finalize() (*types.Value, error) {
	return &types.Value{Type: types.ColumnTypeBigInt, Data: a.count}, nil
}

func (a *CountAgg) Column() string { return a.column }
func (a *CountAgg) Alias() string  { return a.alias }

type SumAgg struct {
	sum    float64
	count  int64
	column string
	alias  string
}

func NewSumAgg(column, alias string) *SumAgg {
	return &SumAgg{column: column, alias: alias}
}

func (a *SumAgg) Init() {
	a.sum = 0
	a.count = 0
}

func (a *SumAgg) Update(v *types.Value) error {
	if v.Data == nil {
		return nil
	}

	switch val := v.Data.(type) {
	case int64:
		a.sum += float64(val)
		a.count++
	case float64:
		a.sum += val
		a.count++
	case int32:
		a.sum += float64(val)
		a.count++
	case int16:
		a.sum += float64(val)
		a.count++
	default:
		return fmt.Errorf("cannot sum non-numeric type: %T", v.Data)
	}
	return nil
}

func (a *SumAgg) Finalize() (*types.Value, error) {
	return &types.Value{Type: types.ColumnTypeDouble, Data: a.sum}, nil
}

func (a *SumAgg) Column() string { return a.column }
func (a *SumAgg) Alias() string  { return a.alias }

type AvgAgg struct {
	sum    float64
	count  int64
	column string
	alias  string
}

func NewAvgAgg(column, alias string) *AvgAgg {
	return &AvgAgg{column: column, alias: alias}
}

func (a *AvgAgg) Init() {
	a.sum = 0
	a.count = 0
}

func (a *AvgAgg) Update(v *types.Value) error {
	if v.Data == nil {
		return nil
	}

	switch val := v.Data.(type) {
	case int64:
		a.sum += float64(val)
		a.count++
	case float64:
		a.sum += val
		a.count++
	case int32:
		a.sum += float64(val)
		a.count++
	case int16:
		a.sum += float64(val)
		a.count++
	default:
		return fmt.Errorf("cannot average non-numeric type: %T", v.Data)
	}
	return nil
}

func (a *AvgAgg) Finalize() (*types.Value, error) {
	if a.count == 0 {
		return &types.Value{Type: types.ColumnTypeDouble, Data: nil}, nil
	}
	return &types.Value{Type: types.ColumnTypeDouble, Data: a.sum / float64(a.count)}, nil
}

func (a *AvgAgg) Column() string { return a.column }
func (a *AvgAgg) Alias() string  { return a.alias }

type MaxAgg struct {
	max    interface{}
	column string
	alias  string
}

func NewMaxAgg(column, alias string) *MaxAgg {
	return &MaxAgg{column: column, alias: alias}
}

func (a *MaxAgg) Init() {
	a.max = nil
}

func (a *MaxAgg) Update(v *types.Value) error {
	if v.Data == nil {
		return nil
	}

	if a.max == nil {
		a.max = v.Data
		return nil
	}

	switch val := v.Data.(type) {
	case int64:
		if maxVal, ok := a.max.(int64); ok && val > maxVal {
			a.max = val
		}
	case float64:
		if maxVal, ok := a.max.(float64); ok && val > maxVal {
			a.max = val
		}
	case string:
		if maxVal, ok := a.max.(string); ok && val > maxVal {
			a.max = val
		}
	}
	return nil
}

func (a *MaxAgg) Finalize() (*types.Value, error) {
	return &types.Value{Data: a.max}, nil
}

func (a *MaxAgg) Column() string { return a.column }
func (a *MaxAgg) Alias() string  { return a.alias }

type MinAgg struct {
	min    interface{}
	column string
	alias  string
}

func NewMinAgg(column, alias string) *MinAgg {
	return &MinAgg{column: column, alias: alias}
}

func (a *MinAgg) Init() {
	a.min = nil
}

func (a *MinAgg) Update(v *types.Value) error {
	if v.Data == nil {
		return nil
	}

	if a.min == nil {
		a.min = v.Data
		return nil
	}

	switch val := v.Data.(type) {
	case int64:
		if minVal, ok := a.min.(int64); ok && val < minVal {
			a.min = val
		}
	case float64:
		if minVal, ok := a.min.(float64); ok && val < minVal {
			a.min = val
		}
	case string:
		if minVal, ok := a.min.(string); ok && val < minVal {
			a.min = val
		}
	}
	return nil
}

func (a *MinAgg) Finalize() (*types.Value, error) {
	return &types.Value{Data: a.min}, nil
}

func (a *MinAgg) Column() string { return a.column }
func (a *MinAgg) Alias() string  { return a.alias }

type AggregateOperator struct {
	input          PhysicalOperator
	groupByColumns []string
	aggFuncs       []AggFunction

	groups       map[string][]AggFunction
	groupIdx     int
	groupKeys    []string
	finished     bool
	maxMemory    int64
	spillManager *engine.SpillManager
	ctx          context.Context
}

func NewAggregate(input PhysicalOperator, groupBy []string, aggFuncs []AggFunction) *AggregateOperator {
	return &AggregateOperator{
		input:          input,
		groupByColumns: groupBy,
		aggFuncs:       aggFuncs,
		groups:         make(map[string][]AggFunction),
		maxMemory:      256 * 1024 * 1024,
	}
}

func NewAggregateWithSpill(ctx context.Context, input PhysicalOperator, groupBy []string, aggFuncs []AggFunction, kv storage.KV, queryID string) *AggregateOperator {
	return &AggregateOperator{
		input:          input,
		groupByColumns: groupBy,
		aggFuncs:       aggFuncs,
		groups:         make(map[string][]AggFunction),
		maxMemory:      256 * 1024 * 1024,
		ctx:            ctx,
	}
}

func (op *AggregateOperator) Open() error {
	if err := op.input.Open(); err != nil {
		return err
	}

	for {
		row, err := op.input.Next()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}

		groupKey := op.buildGroupKey(row)
		if _, exists := op.groups[groupKey]; !exists {
			op.groups[groupKey] = op.initAggFuncs()
			op.groupKeys = append(op.groupKeys, groupKey)
		}

		for i, agg := range op.groups[groupKey] {
			colName := op.aggFuncs[i].Column()
			if colName == "*" {
				agg.Update(&types.Value{Data: 1})
			} else if val, ok := row.Data[colName]; ok {
				agg.Update(val)
			}
		}
	}

	return nil
}

func (op *AggregateOperator) Next() (*types.Record, error) {
	if op.finished || op.groupIdx >= len(op.groupKeys) {
		return nil, EOF
	}

	groupKey := op.groupKeys[op.groupIdx]
	op.groupIdx++

	record := &types.Record{
		Data: make(map[string]*types.Value),
	}

	for _, agg := range op.groups[groupKey] {
		val, err := agg.Finalize()
		if err != nil {
			return nil, err
		}
		alias := agg.Alias()
		if alias == "" {
			alias = agg.Column()
		}
		record.Data[alias] = val
	}

	return record, nil
}

func (op *AggregateOperator) Close() error {
	return op.input.Close()
}

func (op *AggregateOperator) buildGroupKey(row *types.Record) string {
	if len(op.groupByColumns) == 0 {
		return "__all__"
	}

	key := ""
	for _, col := range op.groupByColumns {
		if val, ok := row.Data[col]; ok && val.Data != nil {
			key += fmt.Sprintf("%v|", val.Data)
		} else {
			key += "NULL|"
		}
	}
	return key
}

func (op *AggregateOperator) initAggFuncs() []AggFunction {
	funcs := make([]AggFunction, len(op.aggFuncs))
	for i, agg := range op.aggFuncs {
		switch agg.(type) {
		case *CountAgg:
			funcs[i] = NewCountAgg(agg.Column(), agg.Alias())
		case *SumAgg:
			funcs[i] = NewSumAgg(agg.Column(), agg.Alias())
		case *AvgAgg:
			funcs[i] = NewAvgAgg(agg.Column(), agg.Alias())
		case *MaxAgg:
			funcs[i] = NewMaxAgg(agg.Column(), agg.Alias())
		case *MinAgg:
			funcs[i] = NewMinAgg(agg.Column(), agg.Alias())
		}
		funcs[i].Init()
	}
	return funcs
}
