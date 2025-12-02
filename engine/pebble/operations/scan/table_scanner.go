package scan

import (
	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// TableScanner implements table scanning operations
type TableScanner struct {
	kv    storage.KV
	codec codec.Codec
}

// NewTableScanner creates a new table scanner
func NewTableScanner(kv storage.KV, codec codec.Codec) *TableScanner {
	return &TableScanner{
		kv:    kv,
		codec: codec,
	}
}

// ScanRows performs a table scan
func (ts *TableScanner) ScanRows(tenantID, tableID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	var startKey, endKey []byte

	if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
	} else {
		startKey = ts.codec.EncodeTableKey(tenantID, tableID, 0)
	}

	if opts != nil && opts.EndKey != nil {
		endKey = opts.EndKey
	} else {
		endKey = ts.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := ts.kv.NewIterator(iterOpts)
	return NewRowIterator(iter, ts.codec, schemaDef, opts, nil), nil
}

// ScanIndex performs an index scan - implemented to satisfy Scanner interface
// For TableScanner, this is a fallback that performs a full table scan and filters
func (ts *TableScanner) ScanIndex(tenantID, tableID, indexID int64, schemaDef *dbTypes.TableDefinition, opts *engineTypes.ScanOptions) (engineTypes.RowIterator, error) {
	// For TableScanner, we'll do a full table scan and apply index-based filtering
	// This is less efficient than using an actual index, but provides compatibility
	
	var startKey, endKey []byte

	if opts != nil && opts.StartKey != nil {
		startKey = opts.StartKey
	} else {
		startKey = ts.codec.EncodeTableKey(tenantID, tableID, 0)
	}

	if opts != nil && opts.EndKey != nil {
		endKey = opts.EndKey
	} else {
		endKey = ts.codec.EncodeTableKey(tenantID, tableID, int64(^uint64(0)>>1))
	}

	iterOpts := &storage.IteratorOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	}

	if opts != nil && opts.Reverse {
		iterOpts.Reverse = true
	}

	iter := ts.kv.NewIterator(iterOpts)
	return NewRowIterator(iter, ts.codec, schemaDef, opts, nil), nil
}