package sql

import (
	"context"

	"github.com/guileen/pglitedb/types"
)

// =============================================================================
// TRANSACTION METHODS
// =============================================================================

func (e *Executor) executeBegin(ctx context.Context) (*types.ResultSet, error) {
	// For now, we'll just update the transaction state
	// In a full implementation, we would begin a transaction in the storage engine
	e.inTransaction = true
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

func (e *Executor) executeCommit(ctx context.Context) (*types.ResultSet, error) {
	// For now, we'll just update the transaction state
	// In a full implementation, we would commit the transaction in the storage engine
	e.inTransaction = false
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

func (e *Executor) executeRollback(ctx context.Context) (*types.ResultSet, error) {
	// For now, we'll just update the transaction state
	// In a full implementation, we would rollback the transaction in the storage engine
	e.inTransaction = false
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}