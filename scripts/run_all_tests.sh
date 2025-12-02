#!/bin/bash
set -e

echo "======================================"
echo "PGLiteDB Test Suite Runner"
echo "======================================"

# Create test logs directory
mkdir -p test_logs

# Find available port for testing
find_available_port() {
    local port=5433
    while lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; do
        port=$((port + 1))
    done
    echo $port
}

TEST_PORT=$(find_available_port)

# 1. Unit Tests
echo ""
echo "=== 1. Running Unit Tests ==="
go test -v ./... -timeout 30s 2>&1 | tee test_logs/unit.log
UNIT_EXIT=${PIPESTATUS[0]}

# 2. Integration Tests
echo ""
echo "=== 2. Running Integration Tests ==="
cd examples/integration_test
go test -v ./... -timeout 30s 2>&1 | tee ../../test_logs/integration.log
INTEGRATION_EXIT=${PIPESTATUS[0]}
cd ../..

# 3. Start PostgreSQL Server for client tests
echo ""
echo "=== 3. Starting PostgreSQL Server (port $TEST_PORT) ==="
PG_PORT=$TEST_PORT go run cmd/server/main.go pg > test_logs/server.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
sleep 3

# 4. GORM Test
echo ""
echo "=== 4. Running GORM Client Test ==="
cd examples/gorm_test
# Update the database connection URL to use the dynamic port
sed -i.bak 's/localhost:[0-9]\+/localhost:'"$TEST_PORT"'/g' main.go
timeout 30s go run main.go 2>&1 | tee ../../test_logs/gorm.log
GORM_EXIT=${PIPESTATUS[0]}
mv main.go.bak main.go 2>/dev/null || true
cd ../..

# 5. TypeScript Test
echo ""
echo "=== 5. Running TypeScript Client Test ==="
cd examples/typescript_test
PG_TEST_PORT=$TEST_PORT timeout 30s pnpm test 2>&1 | tee ../../test_logs/typescript.log
TS_EXIT=${PIPESTATUS[0]}
cd ../..

# 6. Stop PostgreSQL Server
echo ""
echo "=== 6. Stopping PostgreSQL Server ==="
kill $SERVER_PID 2>/dev/null || true
sleep 1

# 7. Test Summary
echo ""
echo "======================================"
echo "Test Summary"
echo "======================================"
echo "Unit Tests:        $([ $UNIT_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
echo "Integration Tests: $([ $INTEGRATION_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
echo "GORM Test:         $([ $GORM_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
echo "TypeScript Test:   $([ $TS_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
echo "======================================"

# Print detailed failures if any
if [ $UNIT_EXIT -ne 0 ] || [ $INTEGRATION_EXIT -ne 0 ] || [ $GORM_EXIT -ne 0 ] || [ $TS_EXIT -ne 0 ]; then
    echo ""
    echo "Check test_logs/ for detailed error messages"
    exit 1
fi

echo ""
echo "All tests passed!"
exit 0