package sql

import (
	"context"
	"testing"
	"time"
)

func TestExecutor_ExecuteFunctionCall(t *testing.T) {
	executor := &Executor{}
	ctx := context.Background()

	tests := []struct {
		name           string
		functionName   string
		expectedColumn string
	}{
		{
			name:           "version",
			functionName:   "version",
			expectedColumn: "version",
		},
		{
			name:           "current_database",
			functionName:   "current_database",
			expectedColumn: "current_database",
		},
		{
			name:           "current_catalog",
			functionName:   "current_catalog",
			expectedColumn: "current_catalog",
		},
		{
			name:           "current_user",
			functionName:   "current_user",
			expectedColumn: "current_user",
		},
		{
			name:           "user",
			functionName:   "user",
			expectedColumn: "user",
		},
		{
			name:           "session_user",
			functionName:   "session_user",
			expectedColumn: "session_user",
		},
		{
			name:           "current_role",
			functionName:   "current_role",
			expectedColumn: "current_role",
		},
		{
			name:           "current_schema",
			functionName:   "current_schema",
			expectedColumn: "current_schema",
		},
		{
			name:           "current_timestamp",
			functionName:   "current_timestamp",
			expectedColumn: "current_timestamp",
		},
		{
			name:           "now",
			functionName:   "now",
			expectedColumn: "now",
		},
		{
			name:           "current_date",
			functionName:   "current_date",
			expectedColumn: "current_date",
		},
		{
			name:           "current_time",
			functionName:   "current_time",
			expectedColumn: "current_time",
		},
		{
			name:           "pg_backend_pid",
			functionName:   "pg_backend_pid",
			expectedColumn: "pg_backend_pid",
		},
		{
			name:           "pg_postmaster_start_time",
			functionName:   "pg_postmaster_start_time",
			expectedColumn: "pg_postmaster_start_time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := &Plan{
				Fields: []string{"func:" + tt.functionName},
			}

			result, err := executor.executeFunctionCall(ctx, plan)
			if err != nil {
				t.Fatalf("executeFunctionCall failed: %v", err)
			}

			if result == nil {
				t.Fatal("Expected result, got nil")
			}

			if len(result.Columns) != 1 {
				t.Errorf("Expected 1 column, got %d", len(result.Columns))
			}

			if result.Columns[0] != tt.expectedColumn {
				t.Errorf("Expected column %s, got %s", tt.expectedColumn, result.Columns[0])
			}

			if len(result.Rows) != 1 {
				t.Errorf("Expected 1 row, got %d", len(result.Rows))
			}

			if result.Count != 1 {
				t.Errorf("Expected count 1, got %d", result.Count)
			}
		})
	}

	// Test unsupported function
	t.Run("unsupported function", func(t *testing.T) {
		plan := &Plan{
			Fields: []string{"func:unsupported_function"},
		}

		_, err := executor.executeFunctionCall(ctx, plan)
		if err == nil {
			t.Fatal("Expected error for unsupported function, got nil")
		}
	})
}

func TestExecutor_TimestampFunctions(t *testing.T) {
	executor := &Executor{}
	ctx := context.Background()

	// Test current_timestamp and now functions return valid timestamps
	timestampFuncs := []string{"current_timestamp", "now"}
	for _, funcName := range timestampFuncs {
		t.Run(funcName, func(t *testing.T) {
			plan := &Plan{
				Fields: []string{"func:" + funcName},
			}

			result, err := executor.executeFunctionCall(ctx, plan)
			if err != nil {
				t.Fatalf("executeFunctionCall failed: %v", err)
			}

			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
				t.Fatal("Expected one row with one column")
			}

			timestampStr, ok := result.Rows[0][0].(string)
			if !ok {
				t.Fatal("Expected string timestamp")
			}

			// Parse the timestamp to ensure it's valid
			_, err = time.Parse("2006-01-02 15:04:05.999999-07", timestampStr)
			if err != nil {
				t.Errorf("Invalid timestamp format: %v", err)
			}
		})
	}

	// Test current_date function
	t.Run("current_date", func(t *testing.T) {
		plan := &Plan{
			Fields: []string{"func:current_date"},
		}

		result, err := executor.executeFunctionCall(ctx, plan)
		if err != nil {
			t.Fatalf("executeFunctionCall failed: %v", err)
		}

		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			t.Fatal("Expected one row with one column")
		}

		dateStr, ok := result.Rows[0][0].(string)
		if !ok {
			t.Fatal("Expected string date")
		}

		// Parse the date to ensure it's valid
		_, err = time.Parse("2006-01-02", dateStr)
		if err != nil {
			t.Errorf("Invalid date format: %v", err)
		}
	})

	// Test current_time function
	t.Run("current_time", func(t *testing.T) {
		plan := &Plan{
			Fields: []string{"func:current_time"},
		}

		result, err := executor.executeFunctionCall(ctx, plan)
		if err != nil {
			t.Fatalf("executeFunctionCall failed: %v", err)
		}

		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			t.Fatal("Expected one row with one column")
		}

		timeStr, ok := result.Rows[0][0].(string)
		if !ok {
			t.Fatal("Expected string time")
		}

		// Parse the time to ensure it's valid
		_, err = time.Parse("15:04:05.999999-07", timeStr)
		if err != nil {
			t.Errorf("Invalid time format: %v", err)
		}
	})
}