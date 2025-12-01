#!/bin/bash

# Complete CI test script that starts server, runs tests, and cleans up

# Exit on any error
set -e

# Configuration - Use dedicated test port
TEST_PORT="5670"  # Dedicated test port
SERVER_HOST="localhost"
SERVER_PORT="$TEST_PORT"
DB_PATH="/tmp/pglitedb-ci-$(date +%s)"
LOG_FILE="/tmp/pglitedb-ci.log"
TIMEOUT_DURATION=600  # 10 minutes timeout for entire process

# Function to cleanup on exit
cleanup() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cleaning up..."
    
    # Kill server process if running
    if [ -n "$SERVER_PID" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Killing server process $SERVER_PID"
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    
    # Remove temporary files
    rm -rf "$DB_PATH"
    rm -f "$LOG_FILE"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cleanup completed"
}

# Register cleanup function
trap cleanup EXIT

# Function to kill any existing processes on test port
kill_existing_processes() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Checking for existing processes on port $TEST_PORT..."
    
    # Kill any processes using the test port
    if lsof -i :$TEST_PORT > /dev/null 2>&1; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Killing existing processes on port $TEST_PORT"
        lsof -i :$TEST_PORT | grep LISTEN | awk '{print $2}' | xargs kill -9 2>/dev/null || true
        sleep 2  # Give time for processes to terminate
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] No existing processes found on port $TEST_PORT"
    fi
}

# Function to start server
start_server() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting PostgreSQL server on port $SERVER_PORT..."
    
    # Kill any existing processes on test port
    kill_existing_processes
    
    # Create database directory
    mkdir -p "$DB_PATH"
    
    # Start server in background with test port
    PG_PORT=$TEST_PORT go run ./cmd/server "$DB_PATH" pg > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Server started with PID $SERVER_PID"
    
    # Wait a bit for server to initialize
    sleep 3
    
    # Check if server is still running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Server failed to start"
        cat "$LOG_FILE"
        return 1
    fi
    
    # Wait for server to be ready
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting for server to be ready..."
    for i in {1..30}; do
        if pg_isready -h $SERVER_HOST -p $SERVER_PORT > /dev/null 2>&1; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Server is ready"
            return 0
        fi
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting... ($i/30)"
        sleep 2
    done
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Server did not become ready in time"
    cat "$LOG_FILE"
    return 1
}

# Function to run all tests
run_all_tests() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running all tests..."
    
    # Run regression tests
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running regression tests..."
    if ! scripts/run_regress_improved.sh; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Regression tests failed"
        return 1
    fi
    
    # Run pgbench tests
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running pgbench tests..."
    if ! scripts/run_pgbench_improved.sh; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: PGbench tests failed"
        return 1
    fi
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] All tests completed successfully"
    return 0
}

# Main execution
main() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting complete CI test"
    
    # Start server
    if ! start_server; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to start server"
        exit 1
    fi
    
    # Run tests
    if ! run_all_tests; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Tests failed"
        exit 1
    fi
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] All CI tests completed successfully"
    exit 0
}

# Run main function
main