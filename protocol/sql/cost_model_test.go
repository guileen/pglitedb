package sql

import (
	"testing"
	
	"github.com/guileen/pglitedb/catalog"
	"github.com/stretchr/testify/assert"
)

func TestCostModel_SeqScanCost(t *testing.T) {
	cm := NewCostModel()
	
	// Test sequential scan cost calculation
	cost := cm.SeqScanCost(100, 10000)
	
	// Expected cost: 100 pages * 1.0 (seq_page_cost) + 10000 tuples * 0.01 (cpu_tuple_cost)
	// = 100 + 100 = 200
	assert.Equal(t, 0.0, cost.StartupCost)
	assert.Equal(t, 200.0, cost.TotalCost)
	assert.Equal(t, 10000.0, cost.Rows)
}

func TestCostModel_IndexScanCost(t *testing.T) {
	cm := NewCostModel()
	
	// Test index scan cost calculation
	cost := cm.IndexScanCost(10, 100, 1000)
	
	// Expected cost: 
	// startup = 10 index pages * 4.0 (random_page_cost) = 40
	// total = 40 + 1000 tuples * 0.005 (cpu_index_tuple_cost) + 1000 tuples * 4.0 (random_page_cost)
	// = 40 + 5 + 4000 = 4045
	assert.Equal(t, 40.0, cost.StartupCost)
	assert.Equal(t, 4045.0, cost.TotalCost)
	assert.Equal(t, 1000.0, cost.Rows)
}

func TestCostModel_NestedLoopJoinCost(t *testing.T) {
	cm := NewCostModel()
	
	leftCost := PlanCost{
		StartupCost: 0,
		TotalCost:   100,
		Rows:        1000,
		Width:       50,
	}
	
	rightCost := PlanCost{
		StartupCost: 0,
		TotalCost:   50,
		Rows:        500,
		Width:       30,
	}
	
	joinSelectivity := 0.1
	cost := cm.NestedLoopJoinCost(leftCost, rightCost, joinSelectivity)
	
	// Expected cost:
	// startup = 0 + 0 = 0
	// total = 100 + (1000 * 50) = 100 + 50000 = 50100
	// rows = 1000 * 500 * 0.1 = 50000
	assert.Equal(t, 0.0, cost.StartupCost)
	assert.Equal(t, 50100.0, cost.TotalCost)
	assert.Equal(t, 50000.0, cost.Rows)
	assert.Equal(t, 80, cost.Width)
}

func TestCostModel_HashJoinCost(t *testing.T) {
	cm := NewCostModel()
	
	leftCost := PlanCost{
		StartupCost: 0,
		TotalCost:   100,
		Rows:        1000,
		Width:       50,
	}
	
	rightCost := PlanCost{
		StartupCost: 0,
		TotalCost:   50,
		Rows:        500,
		Width:       30,
	}
	
	joinSelectivity := 0.1
	cost := cm.HashJoinCost(leftCost, rightCost, joinSelectivity)
	
	// Expected cost:
	// startup = 50 (right cost - build phase)
	// total = 50 + 100 = 150
	// rows = 1000 * 500 * 0.1 = 50000
	assert.Equal(t, 50.0, cost.StartupCost)
	assert.Equal(t, 150.0, cost.TotalCost)
	assert.Equal(t, 50000.0, cost.Rows)
	assert.Equal(t, 80, cost.Width)
}

func TestCostModel_MergeJoinCost(t *testing.T) {
	cm := NewCostModel()
	
	leftCost := PlanCost{
		StartupCost: 0,
		TotalCost:   100,
		Rows:        1000,
		Width:       50,
	}
	
	rightCost := PlanCost{
		StartupCost: 0,
		TotalCost:   50,
		Rows:        500,
		Width:       30,
	}
	
	joinSelectivity := 0.1
	cost := cm.MergeJoinCost(leftCost, rightCost, joinSelectivity)
	
	// Expected cost:
	// startup = 100 + 50 = 150 (sort costs)
	// total = 150 + 1000 + 500 = 1650
	// rows = 1000 * 500 * 0.1 = 50000
	assert.Equal(t, 150.0, cost.StartupCost)
	assert.Equal(t, 1650.0, cost.TotalCost)
	assert.Equal(t, 50000.0, cost.Rows)
	assert.Equal(t, 80, cost.Width)
}

func TestCostModel_Selectivity(t *testing.T) {
	cm := NewCostModel()
	
	// Test selectivity with no statistics (default)
	selectivity := cm.Selectivity(nil, "=", "value")
	assert.Equal(t, 0.1, selectivity)
	
	// Test selectivity with statistics
	stats := &catalog.ColumnStatistics{
		NDistinct:       100,
		MostCommonFreqs: []float64{0.05, 0.03, 0.02},
	}
	
	// Test equality selectivity
	selectivity = cm.Selectivity(stats, "=", "value")
	assert.Equal(t, 0.05, selectivity) // Should use most common frequency
	
	// Test inequality selectivity
	selectivity = cm.Selectivity(stats, "!=", "value")
	assert.Equal(t, 0.95, selectivity) // Should be 1 - most common frequency
	
	// Test range selectivity
	selectivity = cm.Selectivity(stats, "<", "value")
	assert.Equal(t, 0.33, selectivity)
	
	// Test LIKE selectivity
	selectivity = cm.Selectivity(stats, "LIKE", "value")
	assert.Equal(t, 0.1, selectivity)
	
	// Test unknown operator selectivity
	selectivity = cm.Selectivity(stats, "UNKNOWN", "value")
	assert.Equal(t, 0.1, selectivity)
}

func TestLog2(t *testing.T) {
	// Test log2 function
	assert.InDelta(t, 0.0, log2(1), 0.0001)
	assert.InDelta(t, 1.0, log2(2), 0.0001)
	assert.InDelta(t, 2.0, log2(4), 0.0001)
	assert.InDelta(t, 3.0, log2(8), 0.0001)
}