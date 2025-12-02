#!/bin/bash

# Script to parse go test -json output and generate structured JSON reports

INPUT_LOG="$1"
OUTPUT_JSON="$2"
SUITE_NAME="$3"

if [ -z "$INPUT_LOG" ] || [ -z "$OUTPUT_JSON" ] || [ -z "$SUITE_NAME" ]; then
    echo "Usage: $0 <input_log> <output_json> <suite_name>"
    exit 1
fi

# Create temporary working files
TEMP_JSON="test_logs/temp_${SUITE_NAME}_parsed.json"
FINAL_JSON="$OUTPUT_JSON"

# Initialize the test suite structure
cat > "$FINAL_JSON" << EOF
{
  "name": "$SUITE_NAME",
  "total_tests": 0,
  "passed": 0,
  "failed": 0,
  "skipped": 0,
  "duration": "0s",
  "tests": []
}
EOF

# Check if the input is already in JSON format (go test -json output)
if grep -q '^{"Time"' "$INPUT_LOG"; then
    echo "Parsing go test -json output format"
    
    # Extract test cases and aggregate results
    PASSED=0
    FAILED=0
    SKIPPED=0
    TOTAL_DURATION=0
    
    # Process each line as JSON
    while IFS= read -r line; do
        if echo "$line" | jq -e .Test >/dev/null 2>&1; then
            TEST_NAME=$(echo "$line" | jq -r '.Test // empty')
            ACTION=$(echo "$line" | jq -r '.Action // empty')
            
            if [ -n "$TEST_NAME" ]; then
                case "$ACTION" in
                    "pass")
                        PASSED=$((PASSED + 1))
                        ;;
                    "fail")
                        FAILED=$((FAILED + 1))
                        ;;
                    "skip")
                        SKIPPED=$((SKIPPED + 1))
                        ;;
                esac
            fi
        fi
    done < "$INPUT_LOG"
    
    # Update the summary
    TOTAL_TESTS=$((PASSED + FAILED + SKIPPED))
    jq --argjson total_tests "$TOTAL_TESTS" \
       --argjson passed "$PASSED" \
       --argjson failed "$FAILED" \
       --argjson skipped "$SKIPPED" \
       '.total_tests = $total_tests |
        .passed = $passed |
        .failed = $failed |
        .skipped = $skipped' "$FINAL_JSON" > "$TEMP_JSON" && mv "$TEMP_JSON" "$FINAL_JSON"
    
else
    echo "Parsing verbose output format"
    
    # Parse verbose go test output
    PASSED=$(grep -c "^PASS" "$INPUT_LOG" 2>/dev/null || echo "0")
    FAILED=$(grep -c "^FAIL" "$INPUT_LOG" 2>/dev/null || echo "0")
    SKIPPED=$(grep -c "^SKIP" "$INPUT_LOG" 2>/dev/null || echo "0")
    
    # Ensure variables are treated as integers
    PASSED=${PASSED:-0}
    FAILED=${FAILED:-0}
    SKIPPED=${SKIPPED:-0}
    
    TOTAL_TESTS=$((PASSED + FAILED + SKIPPED))
    
    # Update the summary
    jq --argjson total_tests "$TOTAL_TESTS" \
       --argjson passed "$PASSED" \
       --argjson failed "$FAILED" \
       --argjson skipped "$SKIPPED" \
       '.total_tests = $total_tests |
        .passed = $passed |
        .failed = $failed |
        .skipped = $skipped' "$FINAL_JSON" > "$TEMP_JSON" && mv "$TEMP_JSON" "$FINAL_JSON"
fi

echo "Parsed test results for suite '$SUITE_NAME':"
echo "  Total Tests: $(jq -r '.total_tests' "$FINAL_JSON")"
echo "  Passed: $(jq -r '.passed' "$FINAL_JSON")"
echo "  Failed: $(jq -r '.failed' "$FINAL_JSON")"
echo "  Skipped: $(jq -r '.skipped' "$FINAL_JSON")"