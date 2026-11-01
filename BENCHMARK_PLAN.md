# Rexodb Benchmark Framework — Task Spec

> **Status: implemented in `bench/`** (workspace-excluded crate, own
> lockfile). Reliability suite: 6/6 PASS. Crash-kill proof also lives in the
> unit suite (`crash_acknowledged_writes_survive` in `tests/db.rs`).
> Pipeline+async measured: 4.5M fill / 4.6M mixed×8. Full multi-DB 10-rep
> run still not executed.

Deferred task. Act as a database benchmarking specialist. Build a complete,
reproducible performance and reliability testing framework for Rexodb.

## Requirements

1. Benchmark Rexodb against RocksDB, SQLite, LMDB and redb.
2. Measure GET, PUT, DELETE, batch writes, range scans and mixed workloads.
3. Test 1, 2, 4, 8, 16 and 32 concurrent threads.
4. Test multiple key sizes, value sizes and database sizes.
5. Measure throughput, p50, p95, p99, p999 latency, CPU, memory and disk I/O.
6. Benchmark in-memory mode separately from persistent mode.
7. Compare equivalent durability guarantees. Never compare asynchronous writes
   against synchronous writes unfairly.
8. Implement repeatable workload generation with deterministic seeds.
9. Run at least 10 independent benchmark repetitions and report median,
   variance and outliers.
10. Add crash-recovery, WAL corruption, checkpoint interruption, disk-full and
    restart tests.
11. Detect performance regressions against a stored baseline.
12. Generate JSON and Markdown benchmark reports.
13. Record CPU, RAM, OS, kernel, Rust version, compiler flags, storage device
    and database configuration.
14. Profile hot paths and identify allocation overhead, lock contention and
    CPU bottlenecks.
15. Never fabricate performance numbers.

## Approach

- First inspect the repository. Follow existing architecture and conventions.
- Implement the benchmark framework incrementally.
- Run the benchmarks, report actual results and identify the five
  highest-impact performance improvements.
- Final output must distinguish measured results, estimates and untested
  assumptions.

## Notes / open questions

- External DB deps needed (dev-dependencies): `rocksdb`, `rusqlite`,
  `heed`/`liblmdb`, `redb` — confirm before adding.
- Decide where the harness lives (e.g. `benches/`, a `xtask`, or a separate
  crate in the workspace).
- Baseline storage format/location for regression detection — TBD.
