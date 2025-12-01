#!/bin/bash

# Improved script to run pgbench tests with better error handling and timeout management

# Exit on any error
set -e

# Configuration
DB_NAME="pgbench_test"
DB_HOST="localhost"
DB_PORT="5670"  # Use dedicated test port
DB_USER="postgres"
TIMEOUT_DURATION=300  # 5 minutes timeout

# Create timestamp for output files
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create output directory
mkdir -p bench

# Function to check if server is ready
wait_for_server() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting for PostgreSQL server to be ready..."
    for i in {1..30}; do
        if pg_isready -h $DB_HOST -p $DB_PORT -U $DB_USER > /dev/null 2>&1; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] PostgreSQL server is ready"
            return 0
        fi
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting for server... ($i/30)"
        sleep 2
    done
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: PostgreSQL server is not responding after 60 seconds"
    return 1
}

# Function to run pgbench tests
run_pgbench_tests() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running pgbench tests..."
    
    # Wait for server to be ready
    if ! wait_for_server; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Server not ready, exiting"
        return 1
    fi
    
    # Create database if it doesn't exist
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Creating database $DB_NAME if it doesn't exist..."
    createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME 2>/dev/null || true
    
    # Initialize pgbench tables
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Initializing pgbench tables..."
    pgbench -h $DB_HOST -p $DB_PORT -U $DB_USER -i $DB_NAME
    
    # Run pgbench test with different configurations
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running pgbench test with different configurations..."
    
    # Test 1: Light load
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Test 1: Light load (1 client, 1 thread, 1000 transactions)"
    OUTPUT_FILE_1="/tmp/pgbench_output_1_${TIMESTAMP}.txt"
    pgbench -h $DB_HOST -p $DB_PORT -U $DB_USER -c 1 -j 1 -t 1000 $DB_NAME > "$OUTPUT_FILE_1" 2>&1
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Light load test completed"
    
    # Test 2: Medium load
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Test 2: Medium load (5 clients, 2 threads, 2000 transactions)"
    OUTPUT_FILE_2="/tmp/pgbench_output_2_${TIMESTAMP}.txt"
    pgbench -h $DB_HOST -p $DB_PORT -U $DB_USER -c 5 -j 2 -t 2000 $DB_NAME > "$OUTPUT_FILE_2" 2>&1
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Medium load test completed"
    
    # Test 3: Heavy load
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Test 3: Heavy load (10 clients, 4 threads, 5000 transactions)"
    OUTPUT_FILE_3="/tmp/pgbench_output_3_${TIMESTAMP}.txt"
    pgbench -h $DB_HOST -p $DB_PORT -U $DB_USER -c 10 -j 4 -t 5000 $DB_NAME > "$OUTPUT_FILE_3" 2>&1
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Heavy load test completed"
    
    # Combine all outputs
    COMBINED_OUTPUT="/tmp/pgbench_combined_${TIMESTAMP}.txt"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] === PGbench Test Results - $TIMESTAMP ===" > "$COMBINED_OUTPUT"
    echo "" >> "$COMBINED_OUTPUT"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Test 1: Light Load ===" >> "$COMBINED_OUTPUT"
    cat "$OUTPUT_FILE_1" >> "$COMBINED_OUTPUT"
    echo "" >> "$COMBINED_OUTPUT"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Test 2: Medium Load ===" >> "$COMBINED_OUTPUT"
    cat "$OUTPUT_FILE_2" >> "$COMBINED_OUTPUT"
    echo "" >> "$COMBINED_OUTPUT"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Test 3: Heavy Load ===" >> "$COMBINED_OUTPUT"
    cat "$OUTPUT_FILE_3" >> "$COMBINED_OUTPUT"
    
    # Save to final location
    FINAL_OUTPUT="/Users/gl/agentwork/pglitedb/bench/bench_${TIMESTAMP}.out"
    cp "$COMBINED_OUTPUT" "$FINAL_OUTPUT"
    
    # Also display the output to console
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] === PGbench Test Output ==="
    cat "$COMBINED_OUTPUT"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ==========================="
    
    # Convert output to JSON format
    JSON_FILE="/Users/gl/agentwork/pglitedb/bench/bench_${TIMESTAMP}.json"
    echo "{" > "$JSON_FILE"
    echo "  \"timestamp\": \"$TIMESTAMP\"," >> "$JSON_FILE"
    echo "  \"tests\": [" >> "$JSON_FILE"
    
    # Process each test output
    for i in {1..3}; do
        OUTPUT_FILE="/tmp/pgbench_output_${i}_${TIMESTAMP}.txt"
        echo "    {" >> "$JSON_FILE"
        echo "      \"test_number\": $i," >> "$JSON_FILE"
        echo "      \"output\": [" >> "$JSON_FILE"
        
        # Escape quotes and add output line by line
        while IFS= read -r line || [ -n "$line" ]; do
            # Escape special characters for JSON
            escaped_line=$(echo "$line" | sed 's/\\/\\\\/g' | sed 's/"/\\"/g' | sed 's/	/\\t/g' | sed 's/$/\\n/')
            echo "        \"$escaped_line\"" >> "$JSON_FILE"
        done < "$OUTPUT_FILE"
        
        # Remove the last comma and close JSON properly
        sed -i '' -e '$ d' "$JSON_FILE" 2>/dev/null || true
        echo "      ]" >> "$JSON_FILE"
        if [ $i -lt 3 ]; then
            echo "    }," >> "$JSON_FILE"
        else
            echo "    }" >> "$JSON_FILE"
        fi
    done
    
    echo "  ]" >> "$JSON_FILE"
    echo "}" >> "$JSON_FILE"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] PGbench test results saved to bench/bench_${TIMESTAMP}.json"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Human-readable output saved to bench/bench_${TIMESTAMP}.out"
    
    return 0
}

# Main execution
main() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting improved pgbench tests"
    
    # Run pgbench tests
    if run_pgbench_tests; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] PGbench tests completed successfully"
        exit 0
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] PGbench tests failed"
        exit 1
    fi
}

# Run main function
main