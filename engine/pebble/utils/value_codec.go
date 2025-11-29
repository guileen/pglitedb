package utils

import (
	"fmt"

	"github.com/guileen/pglitedb/codec"
	dbTypes "github.com/guileen/pglitedb/types"
)

// ValueCodec provides value encoding/decoding utilities
type ValueCodec struct{}

// NewValueCodec creates a new value codec
func NewValueCodec() *ValueCodec {
	return &ValueCodec{}
}

// EncodeRow encodes a row
func (vc *ValueCodec) EncodeRow(row *dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]byte, error) {
	// Use the proper codec implementation
	memCodec := codec.NewMemComparableCodec()
	return memCodec.EncodeRow(row, schemaDef)
}

// DecodeRow decodes a row
func (vc *ValueCodec) DecodeRow(data []byte, schemaDef *dbTypes.TableDefinition) (*dbTypes.Record, error) {
	// Use the proper codec implementation
	memCodec := codec.NewMemComparableCodec()
	return memCodec.DecodeRow(data, schemaDef)
}

// EncodeIndexValue encodes an index value
func (vc *ValueCodec) EncodeIndexValue(value interface{}) ([]byte, error) {
	// This is a placeholder implementation
	return []byte(fmt.Sprintf("encoded_index_%v", value)), nil
}

// DecodeIndexValue decodes an index value
func (vc *ValueCodec) DecodeIndexValue(data []byte) (interface{}, error) {
	// This is a placeholder implementation
	return nil, nil
}