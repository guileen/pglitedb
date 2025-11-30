package pgserver

import (
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// QueryProcessorInterface defines the interface for query processing
type QueryProcessorInterface interface {
	interfaces.QueryProcessorInterface
}

// NewQueryProcessor creates a new query processor
func NewQueryProcessor(executor *sql.Executor, parser sql.Parser, planner *sql.Planner) interfaces.QueryProcessorInterface {
	return components.NewQueryProcessor(executor, parser, planner)
}