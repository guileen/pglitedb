package operators

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/types"
)

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

type JoinCondition struct {
	LeftKeys  []string
	RightKeys []string
}

type HashJoinOperator struct {
	leftInput   PhysicalOperator
	rightInput  PhysicalOperator
	onCondition JoinCondition
	joinType    JoinType
	hashTable   map[string][]*types.Record
	maxMemory   int64

	currentLeft    *types.Record
	currentMatches []*types.Record
	matchIndex     int
	leftExhausted  bool
}

func NewHashJoin(left, right PhysicalOperator, cond JoinCondition, joinType JoinType) *HashJoinOperator {
	return &HashJoinOperator{
		leftInput:   left,
		rightInput:  right,
		onCondition: cond,
		joinType:    joinType,
		hashTable:   make(map[string][]*types.Record),
		maxMemory:   256 * 1024 * 1024,
	}
}

func (op *HashJoinOperator) Open() error {
	if err := op.rightInput.Open(); err != nil {
		return err
	}

	for {
		row, err := op.rightInput.Next()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}

		key := op.buildJoinKey(row, op.onCondition.RightKeys)
		op.hashTable[key] = append(op.hashTable[key], row)
	}

	if err := op.leftInput.Open(); err != nil {
		return err
	}

	return nil
}

func (op *HashJoinOperator) Next() (*types.Record, error) {
	for {
		if op.currentMatches == nil || op.matchIndex >= len(op.currentMatches) {
			if op.leftExhausted {
				return nil, EOF
			}

			leftRow, err := op.leftInput.Next()
			if err == EOF {
				op.leftExhausted = true
				return nil, EOF
			}
			if err != nil {
				return nil, err
			}

			key := op.buildJoinKey(leftRow, op.onCondition.LeftKeys)
			op.currentMatches = op.hashTable[key]
			op.currentLeft = leftRow
			op.matchIndex = 0

			if len(op.currentMatches) == 0 && op.joinType == LeftJoin {
				return op.mergeRows(op.currentLeft, nil), nil
			}
		}

		if op.matchIndex < len(op.currentMatches) {
			result := op.mergeRows(op.currentLeft, op.currentMatches[op.matchIndex])
			op.matchIndex++
			return result, nil
		}
	}
}

func (op *HashJoinOperator) Close() error {
	if err := op.leftInput.Close(); err != nil {
		return err
	}
	return op.rightInput.Close()
}

func (op *HashJoinOperator) buildJoinKey(row *types.Record, keys []string) string {
	key := ""
	for _, k := range keys {
		if val, ok := row.Data[k]; ok && val.Data != nil {
			key += fmt.Sprintf("%v|", val.Data)
		} else {
			key += "NULL|"
		}
	}
	return key
}

func (op *HashJoinOperator) mergeRows(left, right *types.Record) *types.Record {
	result := &types.Record{
		Data: make(map[string]*types.Value),
	}

	if left != nil {
		for k, v := range left.Data {
			result.Data[k] = v
		}
	}

	if right != nil {
		for k, v := range right.Data {
			result.Data[k] = v
		}
	}

	return result
}

type SpillableHashJoinOperator struct {
	leftInput    PhysicalOperator
	rightInput   PhysicalOperator
	onCondition  JoinCondition
	joinType     JoinType
	maxMemory    int64
	numPartitions int
	ctx          context.Context
}

func NewSpillableHashJoin(ctx context.Context, left, right PhysicalOperator, cond JoinCondition, joinType JoinType) *SpillableHashJoinOperator {
	return &SpillableHashJoinOperator{
		leftInput:     left,
		rightInput:    right,
		onCondition:   cond,
		joinType:      joinType,
		maxMemory:     256 * 1024 * 1024,
		numPartitions: 32,
		ctx:           ctx,
	}
}

func (op *SpillableHashJoinOperator) Open() error {
	return nil
}

func (op *SpillableHashJoinOperator) Next() (*types.Record, error) {
	return nil, EOF
}

func (op *SpillableHashJoinOperator) Close() error {
	return nil
}
