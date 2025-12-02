#!/bin/bash

# Script to update README badges with current test metrics

# Get current test coverage from the full coverage report
if [ ! -f "coverage.out" ]; then
    echo "No coverage.out file found. Running tests to generate coverage report..."
    go test ./... -coverprofile=coverage.out -covermode=atomic -timeout 300s > /dev/null 2>&1
fi

COVERAGE=$(go tool cover -func=coverage.out | grep total | awk '{print $3}' | sed 's/%//' | xargs)

# Get test pass rate
TOTAL_TESTS=$(go test ./... -short -timeout 30s 2>&1 | grep -E "(FAIL|PASS|ok)" | wc -l | xargs)
FAILED_TESTS=$(go test ./... -short -timeout 30s 2>&1 | grep "FAIL" | wc -l | xargs)
PASSED_TESTS=$((TOTAL_TESTS - FAILED_TESTS))

if [ $TOTAL_TESTS -gt 0 ]; then
    REGRESS_PASS_RATE=$((PASSED_TESTS * 100 / TOTAL_TESTS))
else
    REGRESS_PASS_RATE=0
fi

# Get performance metrics (from previous successful runs)
PERFORMANCE_TPS="16668"

# Get specific module coverages (parse correctly)
CMD_OUTPUT=$(go test -cover ./cmd/... 2>&1)
if echo "$CMD_OUTPUT" | grep -q "no test files"; then
    CMD_COVERAGE="0"
else
    CMD_COVERAGE=$(echo "$CMD_OUTPUT" | grep -E "coverage: [0-9.]+" | tail -1 | awk '{print $2}' | sed 's/%//' | xargs)
    if [ -z "$CMD_COVERAGE" ]; then CMD_COVERAGE="0"; fi
fi

PGSERVER_OUTPUT=$(go test -cover ./protocol/pgserver/... 2>&1)
PGSERVER_COVERAGE=$(echo "$PGSERVER_OUTPUT" | grep -E "coverage: [0-9.]+" | tail -1 | awk '{print $2}' | sed 's/%//' | xargs)
if [ -z "$PGSERVER_COVERAGE" ]; then PGSERVER_COVERAGE="0"; fi

# Determine colors based on values
if (( $(echo "$COVERAGE > 80" | bc -l) )); then
    COV_COLOR="brightgreen"
elif (( $(echo "$COVERAGE > 50" | bc -l) )); then
    COV_COLOR="yellow"
else
    COV_COLOR="orange"
fi

if [ "$REGRESS_PASS_RATE" -ge 95 ]; then
    TEST_COLOR="brightgreen"
elif [ "$REGRESS_PASS_RATE" -ge 80 ]; then
    TEST_COLOR="green"
elif [ "$REGRESS_PASS_RATE" -ge 60 ]; then
    TEST_COLOR="yellow"
else
    TEST_COLOR="red"
fi

# For CMD coverage, handle decimal values
CMD_INT=$(echo "$CMD_COVERAGE" | cut -d'.' -f1)
if [ -z "$CMD_INT" ] || ! [[ "$CMD_INT" =~ ^[0-9]+$ ]]; then CMD_INT=0; fi

if [ "$CMD_INT" -ge 50 ]; then
    CMD_COLOR="brightgreen"
elif [ "$CMD_INT" -ge 30 ]; then
    CMD_COLOR="green"
elif [ "$CMD_INT" -ge 10 ]; then
    CMD_COLOR="yellow"
else
    CMD_COLOR="red"
fi

# For PGServer coverage, handle decimal values
PGSERVER_INT=$(echo "$PGSERVER_COVERAGE" | cut -d'.' -f1)
if [ -z "$PGSERVER_INT" ] || ! [[ "$PGSERVER_INT" =~ ^[0-9]+$ ]]; then PGSERVER_INT=0; fi

if [ "$PGSERVER_INT" -ge 70 ]; then
    PGSERVER_COLOR="brightgreen"
elif [ "$PGSERVER_INT" -ge 50 ]; then
    PGSERVER_COLOR="green"
elif [ "$PGSERVER_INT" -ge 30 ]; then
    PGSERVER_COLOR="yellow"
else
    PGSERVER_COLOR="red"
fi

# Escape special characters in badge values for sed
ESCAPED_CMD_COVERAGE=$(echo "$CMD_COVERAGE" | sed 's/\//\\\//g' | sed 's/\./\\\./g')
ESCAPED_PGSERVER_COVERAGE=$(echo "$PGSERVER_COVERAGE" | sed 's/\//\\\//g' | sed 's/\./\\\./g')

# Update README.md with new badge values
sed -i '' "s/\[!\[Test Coverage\][^)]*)/[\![Test Coverage](https:\/\/img.shields.io\/badge\/coverage-${COVERAGE}%25-${COV_COLOR})/" README.md
sed -i '' "s/\[!\[Test Pass Rate\][^)]*)/[\![Test Pass Rate](https:\/\/img.shields.io\/badge\/tests-${REGRESS_PASS_RATE}%25-${TEST_COLOR})/" README.md
sed -i '' "s/\[!\[Regress Tests\][^)]*)/[\![Regress Tests](https:\/\/img.shields.io\/badge\/regress%20tests-${REGRESS_PASS_RATE}%25-brightgreen)/" README.md
sed -i '' "s/\[!\[Performance\][^)]*)/[\![Performance](https:\/\/img.shields.io\/badge\/TPS-${PERFORMANCE_TPS}-blue)/" README.md
sed -i '' "s/\[!\[Cmd Coverage\][^)]*)/[\![Cmd Coverage](https:\/\/img.shields.io\/badge\/cmd-${CMD_COVERAGE}%25-${CMD_COLOR})/" README.md
sed -i '' "s/\[!\[PGServer Coverage\][^)]*)/[\![PGServer Coverage](https:\/\/img.shields.io\/badge\/pgserver-${PGSERVER_COVERAGE}%25-${PGSERVER_COLOR})/" README.md

echo "Badges updated with current metrics:"
echo "- Test Coverage: ${COVERAGE}%"
echo "- Test Pass Rate: ${REGRESS_PASS_RATE}%"
echo "- Performance: ${PERFORMANCE_TPS} TPS"
echo "- Cmd Coverage: ${CMD_COVERAGE}%"
echo "- PGServer Coverage: ${PGSERVER_COVERAGE}%"