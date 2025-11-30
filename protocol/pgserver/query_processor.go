package pgserver

import (
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
)

// QueryProcessorInterface defines the interface for query processing
type QueryProcessorInterface interface {
	ProcessQuery(backend *pgproto3.Backend, query string) bool
}

// NewQueryProcessor creates a new query processor
func NewQueryProcessor(executor interface{}, parser interface{}, planner interface{}) QueryProcessorInterface {
	return components.NewQueryProcessor(executor, parser, planner)
}