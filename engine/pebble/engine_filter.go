package pebble

import (
	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
)

// EvaluateFilter evaluates a filter expression against a record
func (e *pebbleEngine) EvaluateFilter(filter *engineTypes.FilterExpression, record *dbTypes.Record) bool {
	return e.filterEvaluator.EvaluateFilter(filter, record)
}

// BuildFilterExpression builds a filter expression from conditions
func (e *pebbleEngine) BuildFilterExpression(conditions map[string]interface{}) *engineTypes.FilterExpression {
	return e.buildFilterExpression(conditions)
}

// buildFilterExpression converts a simple map filter to a complex FilterExpression
func (e *pebbleEngine) buildFilterExpression(conditions map[string]interface{}) *engineTypes.FilterExpression {
	if len(conditions) == 0 {
		return nil
	}
	
	if len(conditions) == 1 {
		// Single condition
		for col, val := range conditions {
			return &engineTypes.FilterExpression{
				Type:     "simple",
				Column:   col,
				Operator: "=",
				Value:    val,
			}
		}
	}
	
	// Multiple conditions - combine with AND
	children := make([]*engineTypes.FilterExpression, 0, len(conditions))
	for col, val := range conditions {
		children = append(children, &engineTypes.FilterExpression{
			Type:     "simple",
			Column:   col,
			Operator: "=",
			Value:    val,
		})
	}
	
	return &engineTypes.FilterExpression{
		Type:     "and",
		Children: children,
	}
}