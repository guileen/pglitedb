package scan

import (
	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// IsIndexOnlyIterator performs an efficient type assertion to check if the given
// RowIterator is specifically an IndexOnlyIterator. This is used primarily for
// testing purposes to verify that the correct iterator type is being used.
func IsIndexOnlyIterator(iter engineTypes.RowIterator) bool {
	_, ok := iter.(*IndexOnlyIterator)
	return ok
}

// IsIndexIterator performs an efficient type assertion to check if the given
// RowIterator is specifically an IndexIterator. This is used primarily for
// testing purposes to verify that the correct iterator type is being used.
func IsIndexIterator(iter engineTypes.RowIterator) bool {
	_, ok := iter.(*IndexIterator)
	return ok
}

// IsRowIterator performs an efficient type assertion to check if the given
// RowIterator is specifically a RowIterator. This is used primarily for
// testing purposes to verify that the correct iterator type is being used.
func IsRowIterator(iter engineTypes.RowIterator) bool {
	_, ok := iter.(*RowIterator)
	return ok
}