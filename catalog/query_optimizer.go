package catalog

import (
	"fmt"
	"strings"

	"github.com/guileen/pglitedb/types"
)

type QueryOptimizer struct {
	statsProvider StatisticsProvider
}

type StatisticsProvider interface {
	GetTableStats(tenantID, tableID int64) (*TableStats, error)
}

type TableStats struct {
	RowCount   int64
	AvgRowSize int64
}

type IndexCandidate struct {
	IndexID     int64
	IndexDef    types.IndexDefinition
	Coverage    int
	Selectivity float64
	Cost        float64
}

func NewQueryOptimizer(statsProvider StatisticsProvider) *QueryOptimizer {
	return &QueryOptimizer{
		statsProvider: statsProvider,
	}
}

func (o *QueryOptimizer) SelectBestIndex(
	opts *types.QueryOptions,
	schema *types.TableDefinition,
) (*IndexCandidate, error) {
	if opts == nil || len(opts.OrderBy) == 0 {
		return nil, nil
	}

	candidates := []IndexCandidate{}

	for i, idx := range schema.Indexes {
		candidate := IndexCandidate{
			IndexID:  int64(i + 1),
			IndexDef: idx,
		}

		candidate.Coverage = o.calculateOrderByCoverage(idx, opts.OrderBy)

		candidate.Cost = o.estimateCostHeuristic(candidate, opts)

		if candidate.Coverage > 0 {
			candidates = append(candidates, candidate)
		}
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	bestCandidate := &candidates[0]
	for i := 1; i < len(candidates); i++ {
		if candidates[i].Cost < bestCandidate.Cost {
			bestCandidate = &candidates[i]
		}
	}

	return bestCandidate, nil
}

func (o *QueryOptimizer) calculateOrderByCoverage(idx types.IndexDefinition, orderBy []string) int {
	coverage := 0
	for i, orderItem := range orderBy {
		if i >= len(idx.Columns) {
			break
		}
		
		// Extract field name (strip ASC/DESC)
		fieldName := orderItem
		if strings.HasSuffix(strings.ToUpper(orderItem), " DESC") {
			fieldName = strings.TrimSpace(orderItem[:len(orderItem)-5])
		} else if strings.HasSuffix(strings.ToUpper(orderItem), " ASC") {
			fieldName = strings.TrimSpace(orderItem[:len(orderItem)-4])
		}
		
		if idx.Columns[i] == fieldName {
			coverage++
		} else {
			break
		}
	}
	return coverage
}

func (o *QueryOptimizer) estimateCostHeuristic(candidate IndexCandidate, opts *types.QueryOptions) float64 {
	baseCost := 1000.0

	if candidate.Coverage > 0 {
		baseCost /= float64(candidate.Coverage * 10)
	}

	if len(candidate.IndexDef.Columns) > 1 {
		baseCost *= 0.8
	}

	return baseCost
}

func (o *QueryOptimizer) ExplainPlan(
	opts *types.QueryOptions,
	schema *types.TableDefinition,
) string {
	candidate, err := o.SelectBestIndex(opts, schema)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	if candidate == nil {
		return "Execution Plan: Full Table Scan"
	}

	return fmt.Sprintf(
		"Execution Plan: Index Scan using index %d (%v), Coverage: %d, Estimated Cost: %.2f",
		candidate.IndexID,
		candidate.IndexDef.Columns,
		candidate.Coverage,
		candidate.Cost,
	)
}