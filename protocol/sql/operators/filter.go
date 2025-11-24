package operators

import (
	"fmt"

	"github.com/guileen/pglitedb/types"
)

type FilterOperator struct {
	input     PhysicalOperator
	predicate FilterPredicate
}

type FilterPredicate interface {
	Evaluate(record *types.Record) (bool, error)
}

type SimpleCondition struct {
	Column   string
	Operator string
	Value    interface{}
}

func NewFilter(input PhysicalOperator, predicate FilterPredicate) *FilterOperator {
	return &FilterOperator{
		input:     input,
		predicate: predicate,
	}
}

func (op *FilterOperator) Open() error {
	return op.input.Open()
}

func (op *FilterOperator) Next() (*types.Record, error) {
	for {
		record, err := op.input.Next()
		if err != nil {
			return nil, err
		}

		match, err := op.predicate.Evaluate(record)
		if err != nil {
			return nil, err
		}

		if match {
			return record, nil
		}
	}
}

func (op *FilterOperator) Close() error {
	return op.input.Close()
}

func (sc *SimpleCondition) Evaluate(record *types.Record) (bool, error) {
	val, ok := record.Data[sc.Column]
	if !ok {
		return false, nil
	}

	switch sc.Operator {
	case "=":
		return val.Data == sc.Value, nil
	case ">":
		return compareGreater(val.Data, sc.Value)
	case "<":
		return compareLess(val.Data, sc.Value)
	case ">=":
		return compareGreaterOrEqual(val.Data, sc.Value)
	case "<=":
		return compareLessOrEqual(val.Data, sc.Value)
	default:
		return false, fmt.Errorf("unsupported operator: %s", sc.Operator)
	}
}

func compareGreater(a, b interface{}) (bool, error) {
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			return va > vb, nil
		}
	case float64:
		if vb, ok := b.(float64); ok {
			return va > vb, nil
		}
	case string:
		if vb, ok := b.(string); ok {
			return va > vb, nil
		}
	}
	return false, fmt.Errorf("cannot compare %T with %T", a, b)
}

func compareLess(a, b interface{}) (bool, error) {
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			return va < vb, nil
		}
	case float64:
		if vb, ok := b.(float64); ok {
			return va < vb, nil
		}
	case string:
		if vb, ok := b.(string); ok {
			return va < vb, nil
		}
	}
	return false, fmt.Errorf("cannot compare %T with %T", a, b)
}

func compareGreaterOrEqual(a, b interface{}) (bool, error) {
	greater, err := compareGreater(a, b)
	if err == nil && greater {
		return true, nil
	}
	return a == b, err
}

func compareLessOrEqual(a, b interface{}) (bool, error) {
	less, err := compareLess(a, b)
	if err == nil && less {
		return true, nil
	}
	return a == b, err
}
