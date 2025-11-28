#!/bin/bash

# Test runner script for concurrency tests
echo "Running PGLiteDB Concurrency Tests"
echo "==================================="

# Run all concurrent tests
echo "Running concurrent transaction tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestConcurrentTransactions

echo ""
echo "Running race condition tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestRaceConditions

echo ""
echo "Running deadlock scenario tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestDeadlockScenarios

echo ""
echo "Running edge case tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestEdgeCases

echo ""
echo "Running error recovery tests..."
go test -v ./engine/pebble/concurrent_tests/ -run TestErrorRecovery

echo ""
echo "Running all concurrent tests with race detection..."
go test -race -v ./engine/pebble/concurrent_tests/

echo ""
echo "Concurrency test suite completed!"