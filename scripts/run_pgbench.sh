#!/bin/bash

# Script to run pgbench tests with proper table initialization

set -e

# Configuration
PG_HOST="localhost"
PG_PORT="${PG_PORT:-5666}"
PG_USER="${USER}"
DB_NAME="postgres"
TEST_DURATION=3

echo "Running pgbench tests..."

# Create database if it doesn't exist
echo "Creating database if not exists..."
createdb -h $PG_HOST -p $PG_PORT -U $PG_USER $DB_NAME 2>/dev/null || true

# Initialize pgbench tables
echo "Initializing pgbench tables..."
pgbench -i -h $PG_HOST -p $PG_PORT -U $PG_USER $DB_NAME

# Run benchmark
echo "Running benchmark for ${TEST_DURATION} seconds..."
TIMESTAMP=$(date +%s)
mkdir -p bench
pgbench -h $PG_HOST -p $PG_PORT -U $PG_USER $DB_NAME -T $TEST_DURATION > bench/$TIMESTAMP.json 2>&1

echo "Benchmark completed. Results saved to bench/$TIMESTAMP.json"