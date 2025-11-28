#!/bin/bash

# Script to compare benchmark results and generate performance comparison reports

# Create timestamp for output files
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create bench directory if it doesn't exist
mkdir -p bench

# Find the two most recent benchmark files
LATEST_BENCH=$(ls -t bench/*.json | head -n 1)
PREVIOUS_BENCH=$(ls -t bench/*.json | head -n 2 | tail -n 1)

if [ -z "$LATEST_BENCH" ] || [ -z "$PREVIOUS_BENCH" ]; then
    echo "Error: Not enough benchmark files to compare"
    exit 1
fi

# Extract performance metrics from JSON files
LATEST_TPS=$(jq -r '.tps' "$LATEST_BENCH")
PREVIOUS_TPS=$(jq -r '.tps' "$PREVIOUS_BENCH")

LATEST_LATENCY=$(jq -r '.latency' "$LATEST_BENCH")
PREVIOUS_LATENCY=$(jq -r '.latency' "$PREVIOUS_BENCH")

LATEST_FAILED=$(jq -r '.failed_transactions' "$LATEST_BENCH")
PREVIOUS_FAILED=$(jq -r '.failed_transactions' "$PREVIOUS_BENCH")

# Calculate percentage changes
if (( $(echo "$PREVIOUS_TPS > 0" | bc -l) )); then
    TPS_CHANGE=$(echo "scale=2; (($LATEST_TPS - $PREVIOUS_TPS) / $PREVIOUS_TPS) * 100" | bc -l)
else
    TPS_CHANGE="N/A"
fi

if (( $(echo "$PREVIOUS_LATENCY > 0" | bc -l) )); then
    LATENCY_CHANGE=$(echo "scale=2; (($LATEST_LATENCY - $PREVIOUS_LATENCY) / $PREVIOUS_LATENCY) * 100" | bc -l)
else
    LATENCY_CHANGE="N/A"
fi

# Generate comparison report
REPORT_FILE="bench/performance_comparison_${TIMESTAMP}.md"
cat > "$REPORT_FILE" << EOF
# Performance Comparison Report (${TIMESTAMP})

## Overview
This report compares the performance of PGLiteDB between two versions to track improvements and identify any regressions.

## Performance Results

### Latest Version ($(basename "$LATEST_BENCH" .json))
- TPS: ${LATEST_TPS}
- Latency: ${LATEST_LATENCY} ms
- Failed Transactions: ${LATEST_FAILED}

### Previous Version ($(basename "$PREVIOUS_BENCH" .json))
- TPS: ${PREVIOUS_TPS}
- Latency: ${PREVIOUS_LATENCY} ms
- Failed Transactions: ${PREVIOUS_FAILED}

## Performance Analysis

Compared to the previous version:
- TPS change: ${TPS_CHANGE}%
- Latency change: ${LATENCY_CHANGE}%
- Failed transactions change: $((LATEST_FAILED - PREVIOUS_FAILED))

## Recommendations

1. Continue monitoring performance trends with each new commit
2. Maintain this comparison report as a living document, updating it with each significant release
3. Investigate any significant performance changes (greater than 5%)
4. Consider making performance-critical features configurable for different deployment scenarios
EOF

echo "Performance comparison report generated: $REPORT_FILE"