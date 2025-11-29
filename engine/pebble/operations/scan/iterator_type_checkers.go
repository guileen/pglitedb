package scan

import (
	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// IsIndexOnlyIterator performs an efficient type assertion to check if the given
// RowIterator is specifically an IndexOnlyIterator. This is used primarily for
// testing purposes to verify that the correct iterator type is being used.
func IsIndexOnlyIterator(iter engineTypes.RowIterator) bool {
	// Check for direct IndexOnlyIterator
	if _, ok := iter.(*IndexOnlyIterator); ok {
		return true
	}
	// Check for pooled IndexOnlyIterator
	if pooled, ok := iter.(*PooledIndexOnlyIterator); ok {
		return pooled.iter != nil
	}
	return false
}

// IsIndexIterator performs an efficient type assertion to check if the given
// RowIterator is specifically an IndexIterator. This is used primarily for
// testing purposes to verify that the correct iterator type is being used.
func IsIndexIterator(iter engineTypes.RowIterator) bool {
	// Check for direct IndexIterator
	if _, ok := iter.(*IndexIterator); ok {
		return true
	}
	// Check for pooled IndexIterator
	if pooled, ok := iter.(*PooledIndexIterator); ok {
		return pooled.iter != nil
	}
	return false
}

// IsRowIterator performs an efficient type assertion to check if the given
// RowIterator is specifically a RowIterator. This is used primarily for
// testing purposes to verify that the correct iterator type is being used.
func IsRowIterator(iter engineTypes.RowIterator) bool {
	// Check for direct RowIterator
	if _, ok := iter.(*RowIterator); ok {
		return true
	}
	// Check for pooled RowIterator
	if pooled, ok := iter.(*PooledRowIterator); ok {
		return pooled.iter != nil
	}
	return false
}