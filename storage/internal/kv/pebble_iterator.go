package kv

import (
	"github.com/cockroachdb/pebble"
)

type PebbleIterator struct {
	iter    *pebble.Iterator
	reverse bool
	err     error
}

func (i *PebbleIterator) Valid() bool {
	if i == nil || i.iter == nil {
		return false
	}
	return i.iter.Valid()
}

func (i *PebbleIterator) Next() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Prev()
	}
	return i.iter.Next()
}

func (i *PebbleIterator) Prev() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Next()
	}
	return i.iter.Prev()
}

func (i *PebbleIterator) Key() []byte {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Key()
}

func (i *PebbleIterator) Value() []byte {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Value()
}

func (i *PebbleIterator) Error() error {
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

func (i *PebbleIterator) SeekGE(key []byte) bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.SeekLT(key)
	}
	return i.iter.SeekGE(key)
}

func (i *PebbleIterator) SeekLT(key []byte) bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.SeekGE(key)
	}
	return i.iter.SeekLT(key)
}

func (i *PebbleIterator) First() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.Last()
	}
	return i.iter.First()
}

func (i *PebbleIterator) Last() bool {
	if i == nil || i.iter == nil {
		return false
	}
	if i.reverse {
		return i.iter.First()
	}
	return i.iter.Last()
}

func (i *PebbleIterator) Close() error {
	if i == nil || i.iter == nil {
		return nil
	}
	return i.iter.Close()
}