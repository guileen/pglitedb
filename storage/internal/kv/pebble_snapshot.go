package kv

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/guileen/pglitedb/storage/shared"
)

type PebbleSnapshot struct {
	snapshot *pebble.Snapshot
}

func (s *PebbleSnapshot) Get(key []byte) ([]byte, error) {
	value, closer, err := s.snapshot.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, shared.ErrNotFound
		}
		return nil, fmt.Errorf("pebble snapshot get: %w", err)
	}
	defer closer.Close()

	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (s *PebbleSnapshot) NewIterator(opts *shared.IteratorOptions) shared.Iterator {
	var pebbleOpts *pebble.IterOptions
	if opts != nil {
		pebbleOpts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	iter, err := s.snapshot.NewIter(pebbleOpts)
	if err != nil {
		return nil
	}
	if iter == nil {
		return nil
	}

	return &PebbleIterator{
		iter:    iter,
		reverse: opts != nil && opts.Reverse,
		err:     err,
	}
}

func (s *PebbleSnapshot) Close() error {
	return s.snapshot.Close()
}