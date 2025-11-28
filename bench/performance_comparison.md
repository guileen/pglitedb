# PGLiteDB Performance Comparison Report

## Latest Performance Results

### Regression Tests
- Total Tests: 228
- Passed: 228
- Failed: 0
- Pass Rate: 100%

### Performance Benchmark (pgbench)
- Tool: pgbench
- Transaction Type: TPC-B (sort of)
- Scaling Factor: 1
- Number of Clients: 10
- Number of Threads: 2
- Transactions per Client: 1000
- Total Transactions Processed: 10000/10000
- Failed Transactions: 0 (0.000%)
- Latency Average: 4.028 ms
- Initial Connection Time: 26.779 ms
- TPS (Transactions Per Second): 2482.38

## Historical Performance Comparison

| Date | TPS | Latency (ms) | Regress Pass Rate |
|------|-----|--------------|-------------------|
| 2025-11-29 | 2482.38 | 4.028 | 100% (228/228) |
| 2025-11-28 | 2475.60 | 4.052 | 100% (228/228) |

## Performance Analysis

PGLiteDB continues to maintain excellent performance with a 100% PostgreSQL regress test pass rate. The latest benchmark shows:

- TPS: 2482.38 (slight improvement from previous 2475.60)
- Latency: 4.028 ms (slight improvement from previous 4.052 ms)

These results demonstrate the stability and performance of PGLiteDB as a PostgreSQL-compatible embedded database solution.

## Comparison with PostgreSQL

While formal comparative benchmarks with official PostgreSQL are still in progress, PGLiteDB's performance metrics show competitive results for an embedded database solution:

- TPS: ~2482 transactions per second
- Latency: ~4ms average response time
- 100% PostgreSQL regress test compatibility

## Conclusion

PGLiteDB maintains its position as a high-performance, PostgreSQL-compatible embedded database with continued improvements in both performance and stability.
