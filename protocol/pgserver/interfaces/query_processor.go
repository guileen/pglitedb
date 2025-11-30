package interfaces

import (
	"context"
	
	"github.com/jackc/pgx/v5/pgproto3"
)

// QueryProcessorInterface defines the interface for query processing
type QueryProcessorInterface interface {
	ProcessQuery(ctx context.Context, backend *pgproto3.Backend, query string) (bool, error)
	ProcessDDL(ctx context.Context, backend *pgproto3.Backend, query string) (bool, error)
	ProcessDML(ctx context.Context, backend *pgproto3.Backend, query string) (bool, error)
	HealthCheck() error
}