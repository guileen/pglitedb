package sql

import (
	"math"
	
	"github.com/guileen/pglitedb/catalog"
)

// CostModel represents the cost model for query optimization
type CostModel struct {
	seqPageCost       float64 // Cost of sequential page access
	randomPageCost    float64 // Cost of random page access
	cpuTupleCost      float64 // Cost of processing a tuple
	cpuIndexTupleCost float64 // Cost of processing an index tuple
	cpuOperatorCost   float64 // Cost of processing an operator
}

// PlanCost represents the cost of a query plan
type PlanCost struct {
	StartupCost float64 // Cost to start the plan
	TotalCost   float64 // Total cost of the plan
	Rows        float64 // Estimated number of rows
	Width       int     // Average row width in bytes
}

// NewCostModel creates a new cost model with default parameters
func NewCostModel() *CostModel {
	return &CostModel{
		seqPageCost:       1.0,
		randomPageCost:    4.0,
		cpuTupleCost:      0.01,
		cpuIndexTupleCost: 0.005,
		cpuOperatorCost:   0.0025,
	}
}

// SeqScanCost calculates the cost of a sequential scan
func (cm *CostModel) SeqScanCost(pages, tuples int64) PlanCost {
	startupCost := 0.0
	totalCost := float64(pages)*cm.seqPageCost + float64(tuples)*cm.cpuTupleCost
	
	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		Rows:        float64(tuples),
		Width:       0, // Will be set based on actual columns
	}
}

// IndexScanCost calculates the cost of an index scan
func (cm *CostModel) IndexScanCost(indexPages, tablePages, tuples int64) PlanCost {
	startupCost := float64(indexPages) * cm.randomPageCost
	totalCost := startupCost + float64(tuples)*cm.cpuIndexTupleCost + float64(tuples)*cm.randomPageCost
	
	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		Rows:        float64(tuples),
		Width:       0, // Will be set based on actual columns
	}
}

// NestedLoopJoinCost calculates the cost of a nested loop join
func (cm *CostModel) NestedLoopJoinCost(leftCost, rightCost PlanCost, joinSelectivity float64) PlanCost {
	startupCost := leftCost.StartupCost + rightCost.StartupCost
	totalCost := leftCost.TotalCost + (leftCost.Rows * rightCost.TotalCost)
	
	// Apply selectivity to estimate output rows
	estimatedRows := leftCost.Rows * rightCost.Rows * joinSelectivity
	
	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		Rows:        estimatedRows,
		Width:       leftCost.Width + rightCost.Width,
	}
}

// HashJoinCost calculates the cost of a hash join
func (cm *CostModel) HashJoinCost(leftCost, rightCost PlanCost, joinSelectivity float64) PlanCost {
	// Hash join builds a hash table from the smaller relation
	buildCost := rightCost.TotalCost
	probeCost := leftCost.TotalCost
	
	startupCost := buildCost
	totalCost := buildCost + probeCost
	
	// Apply selectivity to estimate output rows
	estimatedRows := leftCost.Rows * rightCost.Rows * joinSelectivity
	
	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		Rows:        estimatedRows,
		Width:       leftCost.Width + rightCost.Width,
	}
}

// MergeJoinCost calculates the cost of a merge join
func (cm *CostModel) MergeJoinCost(leftCost, rightCost PlanCost, joinSelectivity float64) PlanCost {
	// Merge join requires sorting both relations first
	sortCost := leftCost.TotalCost + rightCost.TotalCost
	
	startupCost := sortCost
	totalCost := sortCost + leftCost.Rows + rightCost.Rows // Linear merge
	
	// Apply selectivity to estimate output rows
	estimatedRows := leftCost.Rows * rightCost.Rows * joinSelectivity
	
	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		Rows:        estimatedRows,
		Width:       leftCost.Width + rightCost.Width,
	}
}

// SortCost calculates the cost of sorting
func (cm *CostModel) SortCost(inputCost PlanCost) PlanCost {
	// Simplified sort cost calculation using n*log(n) complexity
	sortOperations := inputCost.Rows * log2(inputCost.Rows)
	totalCost := inputCost.TotalCost + sortOperations*cm.cpuOperatorCost
	
	return PlanCost{
		StartupCost: inputCost.StartupCost,
		TotalCost:   totalCost,
		Rows:        inputCost.Rows,
		Width:       inputCost.Width,
	}
}

// Selectivity calculates the selectivity of a condition based on column statistics
func (cm *CostModel) Selectivity(stats *catalog.ColumnStatistics, operator string, value interface{}) float64 {
	if stats == nil {
		// Default selectivity if no statistics available
		return 0.1
	}
	
	switch operator {
	case "=", "==":
		if len(stats.MostCommonFreqs) > 0 {
			// Use most common value frequency if available
			return stats.MostCommonFreqs[0]
		}
		// Default equality selectivity
		if stats.NDistinct > 0 {
			return 1.0 / float64(stats.NDistinct)
		}
		return 0.1
	case "!=", "<>":
		if len(stats.MostCommonFreqs) > 0 {
			// Complement of most common value frequency
			return 1.0 - stats.MostCommonFreqs[0]
		}
		// Default inequality selectivity
		return 0.9
	case "<", "<=", ">", ">=":
		// Range selectivity
		return 0.33
	case "LIKE":
		// Pattern matching selectivity
		return 0.1
	default:
		// Default selectivity for unknown operators
		return 0.1
	}
}

// log2 calculates the base-2 logarithm
func log2(x float64) float64 {
	if x <= 0 {
		return 0
	}
	
	return math.Log2(x)
}