package kv

import (
	"github.com/cockroachdb/pebble"
	"github.com/guileen/pglitedb/storage/shared"
)

// SimpleIterator wraps a single pebble iterator to implement the shared.Iterator interface
type SimpleIterator struct {
	iter    *pebble.Iterator
	reverse bool
	err     error
}

// NewSimpleIterator creates a new simple iterator
func NewSimpleIterator(iter *pebble.Iterator, opts *shared.IteratorOptions) *SimpleIterator {
	return &SimpleIterator{
		iter:    iter,
		reverse: opts != nil && opts.Reverse,
	}
}

func (i *SimpleIterator) Valid() bool {
	if i == nil || i.iter == nil {
		return false
	}
	return i.iter.Valid()
}

func (i *SimpleIterator) Next() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Prev()
	}
	return i.iter.Next()
}

func (i *SimpleIterator) Prev() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Next()
	}
	return i.iter.Prev()
}

func (i *SimpleIterator) Key() []byte {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Key()
}

func (i *SimpleIterator) Value() []byte {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Value()
}

func (i *SimpleIterator) Error() error {
	if i == nil {
		return nil
	}
	if i.err != nil {
		return i.err
	}
	if i.iter == nil {
		return nil
	}
	return i.iter.Error()
}

func (i *SimpleIterator) SeekGE(key []byte) bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.SeekLT(key)
	}
	return i.iter.SeekGE(key)
}

func (i *SimpleIterator) SeekLT(key []byte) bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.SeekGE(key)
	}
	return i.iter.SeekLT(key)
}

func (i *SimpleIterator) First() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Last()
	}
	return i.iter.First()
}

func (i *SimpleIterator) Last() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.First()
	}
	return i.iter.Last()
}

func (i *SimpleIterator) Close() error {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Close()
}