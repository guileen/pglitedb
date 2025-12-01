package kv

import (
	"bytes"
	"github.com/cockroachdb/pebble"
	"github.com/guileen/pglitedb/storage/shared"
)

// MergingIterator combines database and batch iterators
type MergingIterator struct {
	dbIter    *pebble.Iterator
	batchIter *pebble.Iterator
	batch     *pebble.Batch
	reverse   bool
	err       error
	
	// Current state
	dbValid    bool
	batchValid bool
	dbKey      []byte
	batchKey   []byte
	useBatch   bool
	
	// Direction tracking
	atStart bool
	atEnd   bool
}

// NewMergingIterator creates a new merging iterator
func NewMergingIterator(db *pebble.DB, batch *pebble.Batch, opts *shared.IteratorOptions) (*MergingIterator, error) {
	var pebbleOpts *pebble.IterOptions
	if opts != nil {
		pebbleOpts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}
	
	dbIter, err := db.NewIter(pebbleOpts)
	if err != nil {
		return nil, err
	}
	
	var batchIter *pebble.Iterator
	if batch.Count() > 0 {
		batchIter, err = batch.NewIter(pebbleOpts)
		if err != nil {
			dbIter.Close()
			return nil, err
		}
	}
	
	iter := &MergingIterator{
		dbIter:    dbIter,
		batchIter: batchIter,
		batch:     batch,
		reverse:   opts != nil && opts.Reverse,
		atStart:   true,
	}
	
	return iter, nil
}

func (i *MergingIterator) Valid() bool {
	if i == nil {
		return false
	}
	
	// If we're using the batch and it's a deletion, then we're not valid
	if i.useBatch && i.batchValid {
		// Check if this is a deletion marker in the batch
		if i.batchIter.Value() == nil {
			// This is a deleted key, so we're not valid
			return false
		}
	}
	
	return i.dbValid || i.batchValid
}

func (i *MergingIterator) Next() bool {
	if i == nil {
		return false
	}
	
	if i.atStart {
		i.First()
		i.atStart = false
		return i.Valid()
	}
	
	if i.atEnd {
		return false
	}
	
	// Move the appropriate iterator forward
	if i.useBatch {
		i.batchValid = i.batchIter.Next()
		if i.batchValid {
			i.batchKey = cloneBytes(i.batchIter.Key())
		}
	} else {
		i.dbValid = i.dbIter.Next()
		if i.dbValid {
			i.dbKey = cloneBytes(i.dbIter.Key())
		}
	}
	
	// Reposition both iterators and determine which to use
	return i.reposition()
}

func (i *MergingIterator) Prev() bool {
	if i == nil {
		return false
	}
	
	if i.atStart {
		return false
	}
	
	if i.atEnd {
		i.Last()
		i.atEnd = false
		return i.Valid()
	}
	
	// Move the appropriate iterator backward
	if i.useBatch {
		i.batchValid = i.batchIter.Prev()
		if i.batchValid {
			i.batchKey = cloneBytes(i.batchIter.Key())
		}
	} else {
		i.dbValid = i.dbIter.Prev()
		if i.dbValid {
			i.dbKey = cloneBytes(i.dbIter.Key())
		}
	}
	
	// Reposition both iterators and determine which to use
	return i.reposition()
}

func (i *MergingIterator) Key() []byte {
	if i == nil {
		return nil
	}
	if i.useBatch {
		return i.batchKey
	}
	return i.dbKey
}

func (i *MergingIterator) Value() []byte {
	if i == nil {
		return nil
	}
	if i.useBatch {
		// Check if this is a deletion marker in the batch
		if i.batchIter.Value() == nil {
			return nil
		}
		return i.batchIter.Value()
	}
	return i.dbIter.Value()
}

func (i *MergingIterator) Error() error {
	if i == nil {
		return nil
	}
	if i.err != nil {
		return i.err
	}
	if i.dbIter != nil && i.dbIter.Error() != nil {
		return i.dbIter.Error()
	}
	if i.batchIter != nil && i.batchIter.Error() != nil {
		return i.batchIter.Error()
	}
	return nil
}

func (i *MergingIterator) SeekGE(key []byte) bool {
	if i == nil {
		return false
	}
	
	i.atStart = false
	i.atEnd = false
	
	// Seek both iterators
	i.dbValid = i.dbIter.SeekGE(key)
	if i.dbValid {
		i.dbKey = cloneBytes(i.dbIter.Key())
	}
	
	if i.batchIter != nil {
		i.batchValid = i.batchIter.SeekGE(key)
		if i.batchValid {
			i.batchKey = cloneBytes(i.batchIter.Key())
		}
	}
	
	return i.reposition()
}

func (i *MergingIterator) SeekLT(key []byte) bool {
	if i == nil {
		return false
	}
	
	i.atStart = false
	i.atEnd = false
	
	// Seek both iterators
	i.dbValid = i.dbIter.SeekLT(key)
	if i.dbValid {
		i.dbKey = cloneBytes(i.dbIter.Key())
	}
	
	if i.batchIter != nil {
		i.batchValid = i.batchIter.SeekLT(key)
		if i.batchValid {
			i.batchKey = cloneBytes(i.batchIter.Key())
		}
	}
	
	return i.reposition()
}

func (i *MergingIterator) First() bool {
	if i == nil {
		return false
	}
	
	i.atStart = false
	i.atEnd = false
	
	// Position both iterators at the beginning
	i.dbValid = i.dbIter.First()
	if i.dbValid {
		i.dbKey = cloneBytes(i.dbIter.Key())
	}
	
	if i.batchIter != nil {
		i.batchValid = i.batchIter.First()
		if i.batchValid {
			i.batchKey = cloneBytes(i.batchIter.Key())
		}
	}
	
	return i.reposition()
}

func (i *MergingIterator) Last() bool {
	if i == nil {
		return false
	}
	
	i.atStart = false
	i.atEnd = false
	
	// Position both iterators at the end
	i.dbValid = i.dbIter.Last()
	if i.dbValid {
		i.dbKey = cloneBytes(i.dbIter.Key())
	}
	
	if i.batchIter != nil {
		i.batchValid = i.batchIter.Last()
		if i.batchValid {
			i.batchKey = cloneBytes(i.batchIter.Key())
		}
	}
	
	return i.reposition()
}

func (i *MergingIterator) Close() error {
	if i == nil {
		return nil
	}
	
	var err error
	if i.dbIter != nil {
		err = i.dbIter.Close()
	}
	if i.batchIter != nil {
		if batchErr := i.batchIter.Close(); batchErr != nil && err == nil {
			err = batchErr
		}
	}
	return err
}

// reposition determines which iterator to use based on current positions
func (i *MergingIterator) reposition() bool {
	// Reset state
	i.useBatch = false
	
	for {
		// Handle cases where one iterator is invalid
		if !i.dbValid && !i.batchValid {
			return false
		}
		
		if !i.dbValid {
			// Only batch is valid, check if it's a deletion
			if i.batchIter.Value() == nil {
				// This is a deletion, skip it
				i.batchValid = i.batchIter.Next()
				if i.batchValid {
					i.batchKey = cloneBytes(i.batchIter.Key())
				}
				continue
			}
			i.useBatch = true
			return true
		}
		
		if !i.batchValid {
			// Only db is valid
			return true
		}
		
		// Both iterators are valid, compare keys
		cmp := bytes.Compare(i.dbKey, i.batchKey)
		
		if i.reverse {
			// For reverse iteration, prefer the larger key
			if cmp > 0 {
				// dbKey > batchKey, use db
				i.useBatch = false
				return true
			} else if cmp < 0 {
				// dbKey < batchKey, check if batch is a deletion
				if i.batchIter.Value() == nil {
					// This is a deletion, skip it
					i.batchValid = i.batchIter.Prev()
					if i.batchValid {
						i.batchKey = cloneBytes(i.batchIter.Key())
					}
					continue
				}
				// Use batch (it's more recent)
				i.useBatch = true
				return true
			} else {
				// Keys are equal, check if batch is a deletion
				if i.batchIter.Value() == nil {
					// This is a deletion, skip both and continue
					i.dbValid = i.dbIter.Prev()
					if i.dbValid {
						i.dbKey = cloneBytes(i.dbIter.Key())
					}
					i.batchValid = i.batchIter.Prev()
					if i.batchValid {
						i.batchKey = cloneBytes(i.batchIter.Key())
					}
					continue
				}
				// Keys are equal and not deleted, prefer batch (it's more recent)
				i.useBatch = true
				return true
			}
		} else {
			// For forward iteration, prefer the smaller key
			if cmp < 0 {
				// dbKey < batchKey, use db
				i.useBatch = false
				return true
			} else if cmp > 0 {
				// dbKey > batchKey, check if batch is a deletion
				if i.batchIter.Value() == nil {
					// This is a deletion, skip it
					i.batchValid = i.batchIter.Next()
					if i.batchValid {
						i.batchKey = cloneBytes(i.batchIter.Key())
					}
					continue
				}
				// Use batch (it's more recent)
				i.useBatch = true
				return true
			} else {
				// Keys are equal, check if batch is a deletion
				if i.batchIter.Value() == nil {
					// This is a deletion, skip both and continue
					i.dbValid = i.dbIter.Next()
					if i.dbValid {
						i.dbKey = cloneBytes(i.dbIter.Key())
					}
					i.batchValid = i.batchIter.Next()
					if i.batchValid {
						i.batchKey = cloneBytes(i.batchIter.Key())
					}
					continue
				}
				// Keys are equal and not deleted, prefer batch (it's more recent)
				i.useBatch = true
				return true
			}
		}
	}
}

// cloneBytes creates a copy of a byte slice
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	result := make([]byte, len(b))
	copy(result, b)
	return result
}