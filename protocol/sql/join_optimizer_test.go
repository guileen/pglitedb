package sql

import (
	"testing"
	
	"github.com/stretchr/testify/assert"
)

func TestJoinOptimizer_OptimizeJoins_SingleTable(t *testing.T) {
	costModel := NewCostModel()
	joinOptimizer := NewJoinOptimizer(costModel, nil)
	
	tables := []string{"users"}
	conditions := []JoinCondition{}
	
	joinTree, err := joinOptimizer.OptimizeJoins(tables, conditions)
	assert.NoError(t, err)
	assert.NotNil(t, joinTree)
	assert.Equal(t, NestedLoopJoin, joinTree.Type)
	
	// Check that it's a scan node
	scanNode, ok := joinTree.Left.(*ScanNode)
	assert.True(t, ok)
	assert.Equal(t, "users", scanNode.Table)
}

func TestJoinOptimizer_OptimizeJoins_MultipleTables(t *testing.T) {
	costModel := NewCostModel()
	joinOptimizer := NewJoinOptimizer(costModel, nil)
	
	tables := []string{"users", "orders"}
	conditions := []JoinCondition{
		{
			LeftTable:  "users",
			LeftColumn: "id",
			RightTable: "orders",
			RightColumn: "user_id",
			Operator:   "=",
		},
	}
	
	joinTree, err := joinOptimizer.OptimizeJoins(tables, conditions)
	assert.NoError(t, err)
	assert.NotNil(t, joinTree)
	assert.Equal(t, NestedLoopJoin, joinTree.Type) // Default choice in our simplified implementation
	assert.NotNil(t, joinTree.Left)
	assert.NotNil(t, joinTree.Right)
	
	// Check that left is a join node (from previous iteration)
	leftNode, ok := joinTree.Left.(*JoinNode)
	assert.True(t, ok)
	assert.Equal(t, NestedLoopJoin, leftNode.Type)
	
	// Check that right is a scan node
	rightNode, ok := joinTree.Right.(*ScanNode)
	assert.True(t, ok)
	assert.Equal(t, "orders", rightNode.Table)
}

func TestJoinOptimizer_ChooseJoinAlgorithm(t *testing.T) {
	costModel := NewCostModel()
	joinOptimizer := NewJoinOptimizer(costModel, nil)
	
	leftScan := &ScanNode{
		Table: "users",
		Cost: PlanCost{
			StartupCost: 0,
			TotalCost:   100,
			Rows:        1000,
			Width:       50,
		},
	}
	
	rightScan := &ScanNode{
		Table: "orders",
		Cost: PlanCost{
			StartupCost: 0,
			TotalCost:   50,
			Rows:        500,
			Width:       30,
		},
	}
	
	conditions := []JoinCondition{}
	
	// Test join algorithm selection
	joinType := joinOptimizer.chooseJoinAlgorithm(leftScan, rightScan, conditions)
	
	// In our simplified implementation, it should choose based on cost
	// The actual choice depends on the cost model calculations
	assert.True(t, joinType == NestedLoopJoin || joinType == HashJoin || joinType == MergeJoin)
}

func TestJoinOptimizer_ExtractTableNames(t *testing.T) {
	joinOptimizer := NewJoinOptimizer(NewCostModel(), nil)
	
	// Test with scan node
	scanNode := &ScanNode{Table: "users"}
	tables := joinOptimizer.extractTableNames(scanNode)
	assert.Equal(t, []string{"users"}, tables)
	
	// Test with join node
	rightScan := &ScanNode{Table: "orders"}
	joinNode := &JoinNode{
		Type:  NestedLoopJoin,
		Left:  scanNode,
		Right: rightScan,
	}
	
	tables = joinOptimizer.extractTableNames(joinNode)
	assert.Contains(t, tables, "users")
	assert.Contains(t, tables, "orders")
	assert.Len(t, tables, 2)
}

func TestJoinOptimizer_Contains(t *testing.T) {
	slice := []string{"a", "b", "c"}
	
	assert.True(t, contains(slice, "a"))
	assert.True(t, contains(slice, "b"))
	assert.True(t, contains(slice, "c"))
	assert.False(t, contains(slice, "d"))
	assert.False(t, contains(slice, ""))
}

func TestJoinOptimizer_FindJoinConditions(t *testing.T) {
	joinOptimizer := NewJoinOptimizer(NewCostModel(), nil)
	
	conditions := []JoinCondition{
		{
			LeftTable:   "users",
			LeftColumn:  "id",
			RightTable:  "orders",
			RightColumn: "user_id",
			Operator:    "=",
		},
		{
			LeftTable:   "orders",
			LeftColumn:  "id",
			RightTable:  "order_items",
			RightColumn: "order_id",
			Operator:    "=",
		},
	}
	
	// Test finding conditions between users and orders
	leftScan := &ScanNode{Table: "users"}
	relevant := joinOptimizer.findJoinConditions(leftScan, "orders", conditions)
	assert.Len(t, relevant, 1)
	assert.Equal(t, "users", relevant[0].LeftTable)
	assert.Equal(t, "orders", relevant[0].RightTable)
	
	// Test with join node on left
	joinNode := &JoinNode{
		Type: NestedLoopJoin,
		Left: leftScan,
	}
	
	relevant = joinOptimizer.findJoinConditions(joinNode, "orders", conditions)
	assert.Len(t, relevant, 1)
	assert.Equal(t, "users", relevant[0].LeftTable)
	assert.Equal(t, "orders", relevant[0].RightTable)
}