package idgen

import (
	"context"
)

// IDGeneratorInterface defines the interface for ID generation
type IDGeneratorInterface interface {
	// NextRowID generates a new row ID
	NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error)
	
	// NextTableID generates a new table ID
	NextTableID(ctx context.Context, tenantID int64) (int64, error)
	
	// NextIndexID generates a new index ID
	NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error)
}