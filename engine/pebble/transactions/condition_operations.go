// Package transactions provides transaction implementations for the pebble engine.
package transactions

import (
	"context"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/engine/pebble/transactions/errors"
	"github.com/guileen/pglitedb/engine/pebble/constants"
)

// Dummy variable to ensure codec package is considered used
var _ = codec.Codec(nil)

// ConditionOperations provides condition-based operation methods for transactions
type ConditionOperations struct {
	// This struct is intentionally empty as we'll use methods with transaction receivers
}

// UpdateRows updates multiple rows that match the given conditions
func (t *RegularTransaction) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*dbTypes.Value, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if t.closed {
		return 0, errors.ErrClosed
	}

	// Collect all matching row IDs by scanning through the table
	var matchingRowIDs []int64
	
	// Create iterator options to scan the entire table
	iterOpts := &storage.IteratorOptions{
		LowerBound: t.codec.EncodeTableKey(tenantID, tableID, 0),
		UpperBound: t.codec.EncodeTableKey(tenantID, tableID, constants.MaxRowID),
	}
	
	iter := t.kvTxn.NewIterator(iterOpts)
	if iter == nil {
		return 0, errors.ErrIteratorCreation
	}
	defer iter.Close()
	
	// Call First() and check if it succeeded
	if !iter.First() {
		// Check if there was an error
		if err := iter.Error(); err != nil {
			return 0, errors.Wrap(err, "iterator_failure", "iterator error")
		}
		// No rows to iterate over, which is fine
		return 0, nil
	}
	
	// Continue with the iteration
	for iter.Valid() {
		_, _, rowID, err := t.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode table key")
		}
		
		// Decode the row
		value := iter.Value()
		record, err := t.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode row")
		}
		
		// Check conditions
		if t.matchesConditions(record, conditions) {
			matchingRowIDs = append(matchingRowIDs, rowID)
		}
		
		// Move to next row
		if !iter.Next() {
			break
		}
	}
	
	// If no rows match, return early
	if len(matchingRowIDs) == 0 {
		return 0, nil
	}
	
	// Use batch update if there are multiple rows
	if len(matchingRowIDs) > 1 {
		rowUpdates := make(map[int64]map[string]*dbTypes.Value)
		for _, rowID := range matchingRowIDs {
			rowUpdates[rowID] = updates
		}
		if err := t.UpdateRowsBatch(ctx, tenantID, tableID, rowUpdates, schemaDef); err != nil {
			return 0, errors.Wrap(err, "batch_update_failure", "batch update rows")
		}
		return int64(len(matchingRowIDs)), nil
	}
	
	// Fallback to individual updates
	var count int64
	for _, rowID := range matchingRowIDs {
		if err := t.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return count, errors.Wrap(err, "update_failure", "update row %d", rowID)
		}
		count++
	}
	
	return count, nil
}

// DeleteRows deletes multiple rows that match the given conditions
func (t *RegularTransaction) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if t.closed {
		return 0, errors.ErrClosed
	}

	// Collect all matching row IDs by scanning through the table
	var matchingRowIDs []int64
	
	// Create iterator options to scan the entire table
	iterOpts := &storage.IteratorOptions{
		LowerBound: t.codec.EncodeTableKey(tenantID, tableID, 0),
		UpperBound: t.codec.EncodeTableKey(tenantID, tableID, constants.MaxRowID),
	}
	
	iter := t.kvTxn.NewIterator(iterOpts)
	if iter == nil {
		return 0, errors.ErrIteratorCreation
	}
	defer iter.Close()
	
	// Call First() and check if it succeeded
	if !iter.First() {
		// Check if there was an error
		if err := iter.Error(); err != nil {
			return 0, errors.Wrap(err, "iterator_failure", "iterator error")
		}
		// No rows to iterate over, which is fine
		return 0, nil
	}
	
	// Continue with the iteration
	for iter.Valid() {
		_, _, rowID, err := t.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode table key")
		}
		
		// Decode the row
		value := iter.Value()
		record, err := t.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode row")
		}
		
		// Check conditions
		if t.matchesConditions(record, conditions) {
			matchingRowIDs = append(matchingRowIDs, rowID)
		}
		
		// Move to next row
		if !iter.Next() {
			break
		}
	}
	
	// If no rows match, return early
	if len(matchingRowIDs) == 0 {
		return 0, nil
	}
	
	// Use batch delete if there are multiple rows
	if len(matchingRowIDs) > 1 {
		if err := t.DeleteRowsBatch(ctx, tenantID, tableID, matchingRowIDs, schemaDef); err != nil {
			return 0, errors.Wrap(err, "batch_delete_failure", "batch delete rows")
		}
		return int64(len(matchingRowIDs)), nil
	}
	
	// Fallback to individual deletions
	var count int64
	for _, rowID := range matchingRowIDs {
		if err := t.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return count, errors.Wrap(err, "delete_failure", "delete row %d", rowID)
		}
		count++
	}
	
	return count, nil
}

// UpdateRows for SnapshotTransaction
func (tx *SnapshotTransaction) UpdateRows(ctx context.Context, tenantID, tableID int64, updates map[string]*dbTypes.Value, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, errors.ErrClosed
	}

	// Collect all matching row IDs by scanning through the table
	var matchingRowIDs []int64
	
	// Create iterator options to scan the entire table
	iterOpts := &storage.IteratorOptions{
		LowerBound: tx.codec.EncodeTableKey(tenantID, tableID, 0),
		UpperBound: tx.codec.EncodeTableKey(tenantID, tableID, constants.MaxRowID),
	}
	
	iter := tx.snapshot.NewIterator(iterOpts)
	if iter == nil {
		return 0, errors.ErrIteratorCreation
	}
	defer iter.Close()
	
	// Call First() and check if it succeeded
	if !iter.First() {
		// Check if there was an error
		if err := iter.Error(); err != nil {
			return 0, errors.Wrap(err, "iterator_failure", "iterator error")
		}
		// No rows to iterate over, which is fine
		return 0, nil
	}
	
	// Continue with the iteration
	for iter.Valid() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		
		_, _, rowID, err := tx.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode table key")
		}
		
		// Decode the row
		value := iter.Value()
		record, err := tx.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode row")
		}
		
		// Check conditions
		if tx.matchesConditions(record, conditions) {
			matchingRowIDs = append(matchingRowIDs, rowID)
		}
		
		// Move to next row
		if !iter.Next() {
			break
		}
	}
	
	// If no rows match, return early
	if len(matchingRowIDs) == 0 {
		return 0, nil
	}
	
	// Use batch update if there are multiple rows
	if len(matchingRowIDs) > 1 {
		rowUpdates := make(map[int64]map[string]*dbTypes.Value)
		for _, rowID := range matchingRowIDs {
			rowUpdates[rowID] = updates
		}
		if err := tx.UpdateRowsBatch(ctx, tenantID, tableID, rowUpdates, schemaDef); err != nil {
			return 0, errors.Wrap(err, "batch_update_failure", "batch update rows")
		}
		return int64(len(matchingRowIDs)), nil
	}
	
	// Fallback to individual updates
	var count int64
	for _, rowID := range matchingRowIDs {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}
		
		if err := tx.UpdateRow(ctx, tenantID, tableID, rowID, updates, schemaDef); err != nil {
			return count, errors.Wrap(err, "update_failure", "update row %d", rowID)
		}
		count++
	}
	
	return count, nil
}

// DeleteRows for SnapshotTransaction
func (tx *SnapshotTransaction) DeleteRows(ctx context.Context, tenantID, tableID int64, conditions map[string]interface{}, schemaDef *dbTypes.TableDefinition) (int64, error) {
	if tx.closed {
		return 0, errors.ErrClosed
	}

	// Collect all matching row IDs by scanning through the table
	var matchingRowIDs []int64
	
	// Create iterator options to scan the entire table
	iterOpts := &storage.IteratorOptions{
		LowerBound: tx.codec.EncodeTableKey(tenantID, tableID, 0),
		UpperBound: tx.codec.EncodeTableKey(tenantID, tableID, constants.MaxRowID),
	}
	
	iter := tx.snapshot.NewIterator(iterOpts)
	if iter == nil {
		return 0, errors.ErrIteratorCreation
	}
	defer iter.Close()
	
	// Call First() and check if it succeeded
	if !iter.First() {
		// Check if there was an error
		if err := iter.Error(); err != nil {
			return 0, errors.Wrap(err, "iterator_failure", "iterator error")
		}
		// No rows to iterate over, which is fine
		return 0, nil
	}
	
	// Continue with the iteration
	for iter.Valid() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		
		_, _, rowID, err := tx.codec.DecodeTableKey(iter.Key())
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode table key")
		}
		
		// Decode the row
		value := iter.Value()
		record, err := tx.codec.DecodeRow(value, schemaDef)
		if err != nil {
			return 0, errors.Wrap(err, "decoding_failure", "decode row")
		}
		
		// Check conditions
		if tx.matchesConditions(record, conditions) {
			matchingRowIDs = append(matchingRowIDs, rowID)
		}
		
		// Move to next row
		if !iter.Next() {
			break
		}
	}
	
	// If no rows match, return early
	if len(matchingRowIDs) == 0 {
		return 0, nil
	}
	
	// Use batch delete if there are multiple rows
	if len(matchingRowIDs) > 1 {
		if err := tx.DeleteRowsBatch(ctx, tenantID, tableID, matchingRowIDs, schemaDef); err != nil {
			return 0, errors.Wrap(err, "batch_delete_failure", "batch delete rows")
		}
		return int64(len(matchingRowIDs)), nil
	}
	
	// Fallback to individual deletions
	var count int64
	for _, rowID := range matchingRowIDs {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}
		
		if err := tx.DeleteRow(ctx, tenantID, tableID, rowID, schemaDef); err != nil {
			return count, errors.Wrap(err, "delete_failure", "delete row %d", rowID)
		}
		count++
	}
	
	return count, nil
}