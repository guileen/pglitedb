package pebble

import (
	"context"

	dbTypes "github.com/guileen/pglitedb/types"
)

// IDGenerator handles ID generation for the Pebble engine
type IDGenerator struct {
	idGenerator *dbTypes.SnowflakeIDGenerator
}

// NewIDGenerator creates a new IDGenerator
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{
		idGenerator: dbTypes.NewSnowflakeIDGenerator(0),
	}
}

// NextRowID generates a new row ID
func (ig *IDGenerator) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return ig.idGenerator.Next()
}