#!/bin/bash

# Generate comprehensive test badges for the project README

echo "Generating comprehensive test badges..."

# Create badges directory if it doesn't exist
mkdir -p badges

# Function to generate SVG badge
generate_badge() {
    local label=$1
    local value=$2
    local color=$3
    local filename=$4
    
    # Determine color based on value if not specified
    if [ -z "$color" ]; then
        if (( $(echo "$value >= 90" | bc -l) )); then
            color="#4c1"  # Green
        elif (( $(echo "$value >= 75" | bc -l) )); then
            color="#dfb317"  # Yellow
        else
            color="#e05d44"  # Red
        fi
    fi
    
    # Create badge SVG
    cat > "badges/$filename" << EOF
<svg xmlns="http://www.w3.org/2000/svg" width="120" height="20">
  <rect width="120" height="20" fill="#555"/>
  <rect x="60" width="60" height="20" fill="$color"/>
  <text x="6" y="14" font-family="Verdana" font-size="11" fill="#fff">$label</text>
  <text x="70" y="14" font-family="Verdana" font-size="11" fill="#fff">${value}%</text>
</svg>
EOF
}

# Function to generate pass/fail badge
generate_status_badge() {
    local label=$1
    local status=$2
    local filename=$3
    
    local color
    if [ "$status" == "PASS" ] || [ "$status" == "100" ]; then
        color="#4c1"  # Green
        status="100"
    else
        color="#e05d44"  # Red
    fi
    
    # Create badge SVG
    cat > "badges/$filename" << EOF
<svg xmlns="http://www.w3.org/2000/svg" width="120" height="20">
  <rect width="120" height="20" fill="#555"/>
  <rect x="60" width="60" height="20" fill="$color"/>
  <text x="6" y="14" font-family="Verdana" font-size="11" fill="#fff">$label</text>
  <text x="70" y="14" font-family="Verdana" font-size="11" fill="#fff">$status%</text>
</svg>
EOF
}

# Get current coverage percentages
echo "Collecting coverage data..."
CATALOG_COVERAGE=$(go test ./catalog -covermode=atomic 2>/dev/null | grep -o '[0-9.]*% of statements' | cut -d'%' -f1 || echo "0")
ENGINE_COVERAGE=$(go test ./engine -covermode=atomic 2>/dev/null | grep -o '[0-9.]*% of statements' | cut -d'%' -f1 || echo "0")
STORAGE_COVERAGE=$(go test ./storage -covermode=atomic 2>/dev/null | grep -o '[0-9.]*% of statements' | cut -d'%' -f1 || echo "0")

# Get overall coverage
OVERALL_COVERAGE=$(go test ./... -covermode=atomic -coverprofile=coverage.out 2>/dev/null | grep -o '[0-9.]*% of statements' | tail -1 | cut -d'%' -f1 || echo "0")

# Get test results
echo "Running tests to determine pass rate..."
TEST_OUTPUT=$(go test ./... -short 2>&1)
TOTAL_TESTS=$(echo "$TEST_OUTPUT" | grep -c "RUN")
FAILED_TESTS=$(echo "$TEST_OUTPUT" | grep -c "FAIL")
if [ "$TOTAL_TESTS" -gt 0 ]; then
    PASSED_TESTS=$((TOTAL_TESTS - FAILED_TESTS))
    TEST_PASS_RATE=$((PASSED_TESTS * 100 / TOTAL_TESTS))
else
    TEST_PASS_RATE=100
fi

# Generate coverage badges
generate_badge "catalog" "$CATALOG_COVERAGE" "" "catalog_coverage.svg"
generate_badge "engine" "$ENGINE_COVERAGE" "" "engine_coverage.svg"
generate_badge "storage" "$STORAGE_COVERAGE" "" "storage_coverage.svg"
generate_badge "coverage" "$OVERALL_COVERAGE" "" "overall_coverage.svg"

# Generate test pass rate badge
generate_status_badge "tests" "$TEST_PASS_RATE" "tests_passing.svg"

echo "Comprehensive badges generated:"
echo "- Catalog Coverage: ${CATALOG_COVERAGE}%"
echo "- Engine Coverage: ${ENGINE_COVERAGE}%"
echo "- Storage Coverage: ${STORAGE_COVERAGE}%"
echo "- Overall Coverage: ${OVERALL_COVERAGE}%"
echo "- Tests Passing: ${TEST_PASS_RATE}%"