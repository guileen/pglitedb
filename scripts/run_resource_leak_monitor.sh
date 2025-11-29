#!/bin/bash
# Script to run the resource leak monitoring for an extended period
# This script runs a 10-minute stress test with moderate load to detect resource leaks

set -e

echo "=== PGLiteDB Resource Leak Monitoring ==="
echo "This script runs a 10-minute stress test with resource leak detection"
echo

# Create profiles directory
PROFILE_DIR="./resource_profiles"
mkdir -p "$PROFILE_DIR"

# Clean up any previous database
rm -rf /tmp/pglitedb-resource-leak-monitor

echo "Starting resource leak monitoring..."
echo "Duration: 10 minutes"
echo "Workers: 15"
echo "Batch Size: 100"
echo "Profile Directory: $PROFILE_DIR"
echo

# Run the resource leak monitoring
echo "Running resource leak monitoring..."
go run scripts/resource_leak_monitor.go \
  -duration=10m \
  -workers=15 \
  -batch-size=100 \
  -db-path=/tmp/pglitedb-resource-leak-monitor \
  -profile-dir="$PROFILE_DIR"

echo
echo "=== MONITORING COMPLETE ==="
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

echo "Resource leak monitoring complete!"