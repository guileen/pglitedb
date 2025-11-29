#!/bin/bash
# Script to run the profiling benchmark and generate a report

set -e

echo "=== PGLiteDB Profiling Benchmark ==="
echo "This script runs a 5-minute benchmark with profiling enabled"
echo

# Create profiles directory
PROFILE_DIR="./profiles"
mkdir -p "$PROFILE_DIR"

# Clean up any previous database
rm -rf /tmp/pglitedb-profiling-benchmark

echo "Starting profiling benchmark..."
echo "Duration: 5 minutes"
echo "Workers: 10"
echo "Batch Size: 50"
echo "Profile Directory: $PROFILE_DIR"
echo

# Run the profiling benchmark
echo "Running benchmark..."
go run benchmark_profiling.go \
  -duration=5m \
  -workers=10 \
  -batch-size=50 \
  -db-path=/tmp/pglitedb-profiling-benchmark \
  -profile-dir="$PROFILE_DIR"

echo
echo "=== PROFILING COMPLETE ==="
echo "Profiles generated in: $PROFILE_DIR"
echo

# List generated profiles
echo "Generated profiles:"
ls -la "$PROFILE_DIR"
echo

# Show quick analysis
echo "=== QUICK ANALYSIS ==="
echo "Top functions by CPU usage:"
go tool pprof -top -cum "$PROFILE_DIR/cpu.prof" | head -20
echo

echo "Top memory allocations:"
go tool pprof -top -cum "$PROFILE_DIR/mem.prof" | head -20
echo

echo "=== ANALYSIS INSTRUCTIONS ==="
echo "To perform detailed analysis:"
echo "1. CPU profiling: go tool pprof -http=:8080 $PROFILE_DIR/cpu.prof"
echo "2. Memory profiling: go tool pprof -http=:8081 $PROFILE_DIR/mem.prof"
echo "3. Goroutine analysis: go tool pprof $PROFILE_DIR/goroutine.prof"
echo "4. Block analysis: go tool pprof $PROFILE_DIR/block.prof"
echo "5. Mutex analysis: go tool pprof $PROFILE_DIR/mutex.prof"
echo

echo "Report generation complete!"