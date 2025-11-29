#!/bin/bash

# Script to run pgbench tests with proper table initialization

# Exit on any error
set -e

# Configuration
DB_NAME="pgbench_test"
DB_HOST="localhost"
DB_PORT="5666"
DB_USER="postgres"

# Wait for server to be ready
echo "Waiting for PostgreSQL server to be ready..."
for i in {1..30}; do
    if pg_isready -h $DB_HOST -p $DB_PORT -U $DB_USER > /dev/null 2>&1; then
        echo "PostgreSQL server is ready"
        break
    fi
    echo "Waiting for server... ($i/30)"
    sleep 2
done

if ! pg_isready -h $DB_HOST -p $DB_PORT -U $DB_USER > /dev/null 2>&1; then
    echo "ERROR: PostgreSQL server is not responding"
    exit 1
fi

# Create database if it doesn't exist
echo "Creating database $DB_NAME if it doesn't exist..."
createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME || true

# Initialize pgbench tables
echo "Initializing pgbench tables..."
pgbench -h $DB_HOST -p $DB_PORT -U $DB_USER -i $DB_NAME

# Run pgbench test
echo "Running pgbench test..."
pgbench -h $DB_HOST -p $DB_PORT -U $DB_USER -c 10 -j 2 -t 1000 $DB_NAME

echo "pgbench test completed successfully!"