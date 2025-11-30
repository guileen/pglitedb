package modules

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/types"
)

// FunctionExecutor handles function calls in SELECT statements
type FunctionExecutor struct {
}

// NewFunctionExecutor creates a new function executor
func NewFunctionExecutor() *FunctionExecutor {
	return &FunctionExecutor{}
}

// ExecuteFunctionCall handles function calls in SELECT statements
func (fe *FunctionExecutor) ExecuteFunctionCall(ctx context.Context, plan *sql.Plan) (*types.ResultSet, error) {
	if len(plan.Fields) == 0 {
		return nil, fmt.Errorf("no fields specified in function call")
	}

	// Extract function name (remove "func:" prefix)
	funcName := strings.TrimPrefix(plan.Fields[0], "func:")

	// Handle specific functions
	switch funcName {
	case "version":
		// Return version information
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{"PGLiteDB 0.1.0 (compatible with PostgreSQL 14.0)"},
		}
		result.Count = 1
		return result, nil

	case "current_database", "current_catalog":
		// Return current database name
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{"pglitedb"},
		}
		result.Count = 1
		return result, nil

	case "current_user", "user", "session_user", "current_role":
		// Return current user (placeholder)
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{"postgres"},
		}
		result.Count = 1
		return result, nil

	case "current_schema":
		// Return current schema (placeholder)
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{"public"},
		}
		result.Count = 1
		return result, nil

	case "current_timestamp", "now":
		// Return current timestamp
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{time.Now().Format("2006-01-02 15:04:05.999999-07")},
		}
		result.Count = 1
		return result, nil

	case "current_date":
		// Return current date
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{time.Now().Format("2006-01-02")},
		}
		result.Count = 1
		return result, nil

	case "current_time":
		// Return current time
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{time.Now().Format("15:04:05.999999-07")},
		}
		result.Count = 1
		return result, nil

	case "pg_backend_pid":
		// Return backend process ID (placeholder)
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{12345},
		}
		result.Count = 1
		return result, nil

	case "pg_postmaster_start_time":
		// Return postmaster start time (placeholder)
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{time.Now().Format("2006-01-02 15:04:05.999999-07")},
		}
		result.Count = 1
		return result, nil

	default:
		return nil, fmt.Errorf("function %s not implemented", funcName)
	}
}