#!/bin/bash
# Script to run benchmarks with profiling enabled

echo "Running benchmarks with profiling enabled..."

# Create profiles directory if it doesn't exist
mkdir -p profiles

# Run pebble engine benchmarks with all profiling enabled
echo "Running pebble engine benchmarks..."
go test -bench=BenchmarkStorageEngine_ -cpuprofile -memprofile -allocprofile -blockprofile -mutexprofile -profiledir=./profiles -profileprefix=pebble ./engine/...

# Run SQL planner benchmarks with all profiling enabled
echo "Running SQL planner benchmarks..."
go test -bench=BenchmarkPlanner_ -cpuprofile -memprofile -allocprofile -blockprofile -mutexprofile -profiledir=./profiles -profileprefix=sql ./protocol/sql/...

echo "Benchmarking complete. Profiles saved to ./profiles/"
echo "To analyze profiles, use:"
echo "  go tool pprof profiles/pebble_BenchmarkStorageEngine_*_cpu.prof"
echo "  go tool pprof -http=:8080 profiles/pebble_BenchmarkStorageEngine_*_cpu.prof"