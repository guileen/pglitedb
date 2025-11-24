package operators

import (
	"github.com/guileen/pglitedb/types"
)

type JoinType string

const (
	JoinTypeInner JoinType = "INNER"
	JoinTypeLeft  JoinType = "LEFT"
	JoinTypeRight JoinType = "RIGHT"
)

type JoinCondition struct {
	LeftColumn  string
	RightColumn string
}

type HashJoinOperator struct {
	leftInput   PhysicalOperator
	rightInput  PhysicalOperator
	joinType    JoinType
	onCondition JoinCondition

	hashTable map[interface{}][]*types.Record
	leftRow   *types.Record
	matchIdx  int
	matches   []*types.Record
	finished  bool
}

func NewHashJoin(left, right PhysicalOperator, joinType JoinType, on JoinCondition) *HashJoinOperator {
	return &HashJoinOperator{
		leftInput:   left,
		rightInput:  right,
		joinType:    joinType,
		onCondition: on,
		hashTable:   make(map[interface{}][]*types.Record),
	}
}

func (op *HashJoinOperator) Open() error {
	if err := op.rightInput.Open(); err != nil {
		return err
	}

	for {
		rr, err := op.rightInput.Next()
		if err == EOF {
			break
		}
		if err != nil {
			return err
		}

		key := op.getRightKey(rr)
		op.hashTable[key] = append(op.hashTable[key], rr)
	}

	if err := op.leftInput.Open(); err != nil {
		return err
	}

	return nil
}

func (op *HashJoinOperator) Next() (*types.Record, error) {
	for {
		if op.matchIdx < len(op.matches) {
			result := op.mergeRows(op.leftRow, op.matches[op.matchIdx])
			op.matchIdx++
			return result, nil
		}

		lr, err := op.leftInput.Next()
		if err == EOF {
			return nil, EOF
		}
		if err != nil {
			return nil, err
		}

		op.leftRow = lr
		op.matchIdx = 0

		leftKey := op.getLeftKey(lr)
		op.matches = op.hashTable[leftKey]

		if len(op.matches) > 0 {
			continue
		}

		if op.joinType == JoinTypeLeft {
			return op.mergeRowsWithNull(lr), nil
		}
	}
}

func (op *HashJoinOperator) Close() error {
	if err := op.leftInput.Close(); err != nil {
		return err
	}
	return op.rightInput.Close()
}

func (op *HashJoinOperator) getLeftKey(row *types.Record) interface{} {
	if val, ok := row.Data[op.onCondition.LeftColumn]; ok {
		return val.Data
	}
	return nil
}

func (op *HashJoinOperator) getRightKey(row *types.Record) interface{} {
	if val, ok := row.Data[op.onCondition.RightColumn]; ok {
		return val.Data
	}
	return nil
}

func (op *HashJoinOperator) mergeRows(left, right *types.Record) *types.Record {
	result := &types.Record{
		Data: make(map[string]*types.Value),
	}

	for k, v := range left.Data {
		result.Data[k] = v
	}

	for k, v := range right.Data {
		result.Data[k] = v
	}

	return result
}

func (op *HashJoinOperator) mergeRowsWithNull(left *types.Record) *types.Record {
	result := &types.Record{
		Data: make(map[string]*types.Value),
	}

	for k, v := range left.Data {
		result.Data[k] = v
	}

	return result
}
