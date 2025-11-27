#!/bin/bash

# Script to run pgbench tests with proper table initialization

# Exit on any error
set -e

# Configuration
DB_NAME="pgbench_test"
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="postgres"

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