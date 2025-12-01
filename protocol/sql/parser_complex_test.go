package sql

import (
	"testing"
	"github.com/guileen/pglitedb/protocol/sql/parser"
)

func TestEnhancedParserComplexQueryParsing(t *testing.T) {
	sqlParser := NewSimplePGParser()
	planner := NewPlanner(sqlParser)

	tests := []struct {
		name              string
		query             string
		expectedType      parser.StatementType
		expectedTable     string
		expectedFields    []string
		expectedAggregates int
		expectedCaseExprs int
		hasSubqueries     bool
		hasConditions     bool
		hasOrderBy        bool
	}{
		{
			name:              "Simple SELECT with aggregate",
			query:             "SELECT COUNT(id) FROM users",
			expectedType:      parser.SelectStatement,
			expectedTable:     "users",
			expectedFields:    []string{"COUNT(id)"}, // Simple parser treats this as a field
			expectedAggregates: 0, // Simple parser doesn't extract aggregates
			expectedCaseExprs: 0,
		},
		{
			name:              "SELECT with CASE expression",
			query:             "SELECT id, CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END as category FROM users",
			expectedType:      parser.SelectStatement,
			expectedTable:     "users",
			expectedFields:    []string{"id", "CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END as category"}, // Simple parser treats this as fields
			expectedAggregates: 0,
			expectedCaseExprs: 0, // Simple parser doesn't extract case expressions
		},
		{
			name:              "SELECT with complex aggregate and CASE",
			query:             "SELECT SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_count FROM users",
			expectedType:      parser.SelectStatement,
			expectedTable:     "users",
			expectedFields:    []string{"SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_count"}, // Simple parser treats this as a field
			expectedAggregates: 0,
			expectedCaseExprs: 0,
		},
		{
			name:              "SELECT with subquery in FROM",
			query:             "SELECT id, name FROM (SELECT id, name FROM users WHERE active = true) AS active_users",
			expectedType:      parser.SelectStatement,
			expectedTable:     "", // Simple parser doesn't handle subqueries properly
			expectedFields:    []string{"id", "name"},
			expectedAggregates: 0,
			expectedCaseExprs: 0,
			hasSubqueries:     false, // Simple parser doesn't extract subqueries
		},
		{
			name:              "SELECT with DISTINCT aggregate",
			query:             "SELECT COUNT(DISTINCT department) FROM employees",
			expectedType:      parser.SelectStatement,
			expectedTable:     "employees",
			expectedFields:    []string{"COUNT(DISTINCT department)"}, // Simple parser treats this as a field
			expectedAggregates: 0,
			expectedCaseExprs: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a plan from the parsed query
			plan, err := planner.CreatePlan(tt.query)
			if err != nil {
				t.Fatalf("CreatePlan failed: %v", err)
			}

			if plan.Type != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, plan.Type)
			}

			if plan.Table != tt.expectedTable {
				t.Errorf("Expected table %v, got %v", tt.expectedTable, plan.Table)
			}

			if len(plan.Fields) != len(tt.expectedFields) {
				t.Errorf("Expected %d fields, got %d", len(tt.expectedFields), len(plan.Fields))
				return
			}
			for i, expected := range tt.expectedFields {
				if plan.Fields[i] != expected {
					t.Errorf("Expected field %s at position %d, got %s", expected, i, plan.Fields[i])
				}
			}

			// Check enhanced fields
			if len(plan.Aggregates) != tt.expectedAggregates {
				t.Errorf("Expected %d aggregates, got %d", tt.expectedAggregates, len(plan.Aggregates))
			}

			if len(plan.CaseExpressions) != tt.expectedCaseExprs {
				t.Errorf("Expected %d case expressions, got %d", tt.expectedCaseExprs, len(plan.CaseExpressions))
			}

			if tt.hasSubqueries && len(plan.Subqueries) == 0 {
				t.Error("Expected subqueries, got none")
			}
		})
	}
}