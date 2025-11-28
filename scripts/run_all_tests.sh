#!/bin/bash
set -e

echo "======================================"
echo "PGLiteDB Test Suite Runner"
echo "======================================"

# Create test logs directory
mkdir -p test_logs

# 1. Unit Tests
echo ""
echo "=== 1. Running Unit Tests ==="
go test -v ./... 2>&1 | tee test_logs/unit.log
UNIT_EXIT=$?

# 2. Integration Tests
echo ""
echo "=== 2. Running Integration Tests ==="
cd examples/integration_test
go test -v ./... 2>&1 | tee ../../test_logs/integration.log
INTEGRATION_EXIT=$?
cd ../..

# 3. Start PostgreSQL Server for client tests
echo ""
echo "=== 3. Starting PostgreSQL Server (port 5433) ==="
PG_PORT=5433 go run cmd/server/main.go pg > test_logs/server.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
sleep 3

# 4. GORM Test
echo ""
echo "=== 4. Running GORM Client Test ==="
cd examples/gorm_test
go run main.go 2>&1 | tee ../../test_logs/gorm.log
GORM_EXIT=$?
cd ../..

# 5. TypeScript Test
echo ""
echo "=== 5. Running TypeScript Client Test ==="
cd examples/typescript_test
pnpm test 2>&1 | tee ../../test_logs/typescript.log
TS_EXIT=$?
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
