#!/bin/bash

# Improved script to run regression tests with better error handling and timeout management

# Exit on any error
set -e

# Configuration
REGRESS_DIR="/Users/gl/agentwork/postgresql-18.1/src/test/regress"
SERVER_HOST="127.0.0.1"
SERVER_PORT="5670"  # Use dedicated test port
OUTPUT_DIR="/tmp/regress_test"
TIMEOUT_DURATION=300  # 5 minutes timeout

# Create timestamp for output files
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create output directory
mkdir -p regress

# Function to check if server is ready
wait_for_server() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting for PostgreSQL server to be ready..."
    for i in {1..30}; do
        if pg_isready -h $SERVER_HOST -p $SERVER_PORT > /dev/null 2>&1; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] PostgreSQL server is ready"
            return 0
        fi
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting for server... ($i/30)"
        sleep 2
    done
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: PostgreSQL server is not responding after 60 seconds"
    return 1
}

# Function to run regression tests with timeout
run_regression_tests() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running regression tests..."
    
    # Change to regress directory
    cd "$REGRESS_DIR"
    
    # Run tests with timeout and capture output
    OUTPUT_FILE="/tmp/regress_output_$TIMESTAMP.txt"
    
    # Use timeout to prevent hanging
    if command -v gtimeout > /dev/null; then
        # GNU timeout (Linux)
        TIMEOUT_CMD="gtimeout"
    elif command -v timeout > /dev/null; then
        # BSD timeout (macOS) or GNU timeout (Linux)
        TIMEOUT_CMD="timeout"
    else
        # No timeout command available
        TIMEOUT_CMD=""
    fi
    
    if [ -n "$TIMEOUT_CMD" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running tests with timeout (${TIMEOUT_DURATION}s)..."
        $TIMEOUT_CMD $TIMEOUT_DURATION \
            ./pg_regress --host=$SERVER_HOST --port=$SERVER_PORT \
            --outputdir=$OUTPUT_DIR --schedule=parallel_schedule \
            > "$OUTPUT_FILE" 2>&1 || TEST_EXIT_CODE=$?
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running tests without timeout (timeout command not available)..."
        ./pg_regress --host=$SERVER_HOST --port=$SERVER_PORT \
            --outputdir=$OUTPUT_DIR --schedule=parallel_schedule \
            > "$OUTPUT_FILE" 2>&1 || TEST_EXIT_CODE=$?
    fi
    
    # If TEST_EXIT_CODE is not set, assume success
    TEST_EXIT_CODE=${TEST_EXIT_CODE:-0}
    
    # Save human-readable output with timestamped filename
    HUMAN_READABLE_FILE="/Users/gl/agentwork/pglitedb/regress/regress_${TIMESTAMP}.out"
    cp "$OUTPUT_FILE" "$HUMAN_READABLE_FILE"
    
    # Also display the output to console
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Regression Test Output ==="
    cat "$OUTPUT_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] =============================="
    
    # Convert output to JSON format
    JSON_FILE="/Users/gl/agentwork/pglitedb/regress/regress_${TIMESTAMP}.json"
    echo "{" > "$JSON_FILE"
    echo "  \"timestamp\": \"$TIMESTAMP\"," >> "$JSON_FILE"
    echo "  \"exit_code\": $TEST_EXIT_CODE," >> "$JSON_FILE"
    echo "  \"output\": [" >> "$JSON_FILE"
    
    # Escape quotes and add output line by line
    while IFS= read -r line || [ -n "$line" ]; do
        # Escape special characters for JSON
        escaped_line=$(echo "$line" | sed 's/\\/\\\\/g' | sed 's/"/\\"/g' | sed 's/	/\\t/g' | sed 's/$/\\n/')
        echo "    \"$escaped_line\"" >> "$JSON_FILE"
    done < "$OUTPUT_FILE"
    
    # Remove the last comma and close JSON properly
    sed -i '' -e '$ d' "$JSON_FILE" 2>/dev/null || true
    echo "  ]" >> "$JSON_FILE"
    echo "}" >> "$JSON_FILE"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Regression test results saved to regress/regress_${TIMESTAMP}.json"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Human-readable output saved to regress/regress_${TIMESTAMP}.out"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Test exit code: $TEST_EXIT_CODE"
    
    return $TEST_EXIT_CODE
}

# Main execution
main() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting improved regression tests"
    
    # Wait for server to be ready
    if ! wait_for_server; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Server not ready, exiting"
        exit 1
    fi
    
    # Run regression tests
    if run_regression_tests; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Regression tests completed successfully"
        exit 0
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Regression tests failed with exit code: $?"
        exit 1
    fi
}

# Run main function
main