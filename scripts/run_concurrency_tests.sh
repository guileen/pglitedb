#!/bin/bash

# Test runner script for concurrency tests
echo "Running PGLiteDB Concurrency Tests"
echo "==================================="

# Run all concurrent tests with timeout
echo "Running concurrent transaction tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestConcurrentTransactions -timeout 30s

echo ""
echo "Running race condition tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestRaceConditions -timeout 30s

echo ""
echo "Running deadlock scenario tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestDeadlockScenarios -timeout 30s

echo ""
echo "Running edge case tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestEdgeCases -timeout 30s

echo ""
echo "Running error recovery tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestErrorRecovery -timeout 30s

echo ""
echo "Running all concurrent tests with race detection..."
go test -race -v ./engine/pebble/concurrent_tests/ -timeout 60s

echo ""
echo "Concurrency test suite completed!"