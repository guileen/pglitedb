#!/bin/bash

# Script to run regression tests and save output in both JSON and human-readable formats

# Create timestamp for output files
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create output directory
mkdir -p regress

# Run regression tests
echo "Running regression tests..."
cd /Users/gl/agentwork/postgresql-18.1/src/test/regress

# Run tests and capture output
OUTPUT_FILE="/tmp/regress_output_$TIMESTAMP.txt"
./pg_regress --host=localhost --port=5666 --outputdir=/tmp/regress_test --schedule=parallel_schedule > "$OUTPUT_FILE" 2>&1
TEST_EXIT_CODE=$?

# Save human-readable output with timestamped filename
HUMAN_READABLE_FILE="/Users/gl/agentwork/pglitedb/regress/regress_${TIMESTAMP}.out"
cp "$OUTPUT_FILE" "$HUMAN_READABLE_FILE"

# Also display the output to console
echo "=== Regression Test Output ==="
cat "$OUTPUT_FILE"
echo "=============================="

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
sed -i '' -e '$ d' "$JSON_FILE"
echo "  ]" >> "$JSON_FILE"
echo "}" >> "$JSON_FILE"

echo "Regression test results saved to regress/regress_${TIMESTAMP}.json"
echo "Human-readable output saved to regress/regress_${TIMESTAMP}.out"
echo "Test exit code: $TEST_EXIT_CODE"