package interfaces

import (
	"context"
	
	"github.com/jackc/pgx/v5/pgproto3"
)

// PreparedStatementManagerInterface defines the interface for prepared statement management
type PreparedStatementManagerInterface interface {
	Parse(ctx context.Context, backend *pgproto3.Backend, msg *pgproto3.Parse) (bool, error)
	Bind(ctx context.Context, backend *pgproto3.Backend, msg *pgproto3.Bind) (bool, error)
	Describe(ctx context.Context, backend *pgproto3.Backend, msg *pgproto3.Describe) (bool, error)
	Execute(ctx context.Context, backend *pgproto3.Backend, msg *pgproto3.Execute) (bool, error)
	HealthCheck() error
}