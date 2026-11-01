# Benchmarks

One measurement pass, one dev machine — re-measure before quoting externally.
Full per-cell tables (all thread counts, both key/value sizes) live in
`bench/out-{full,weak,strong,inmem,pipe}/report.md`; regenerate with the
commands at the bottom.

## Environment

- Apple M5, 24 GiB, arm64 — macOS 26.6, APFS (local SSD)
- rustc 1.99.1, release `opt-level=3 lto=thin codegen-units=1`
- 10 reps/scenario, median reported; deterministic seeds, same op streams
- default `Config` except where noted; 150k-key preload for read/scan/mixed

## Throughput — Direct write path, `EveryMs(1)`, 16B keys / 128B values

| workload | t1 | t8 | t32 |
|---|---|---|---|
| fill_seq | 1.35 M | 1.17 M | 857 k |
| fill_rand | 1.36 M | 980 k | 592 k |
| read_rand | 3.64 M | 3.52 M | 2.66 M |
| delete_rand | 1.63 M | 935 k | 590 k |
| mixed_50_50 | 664 k | 781 k | 439 k |
| batch_128 (batches/s) | 38.5 k | 43.8 k | 47.5 k |
| scan_1000 (scans/s) | 483 | 871 | 939 |

Latency (t1, same run): fill p50 250 ns / p99 875 ns / p999 6.5 µs;
read p50 250 ns / p99 459 ns; mixed p50 833 ns / p99 3.5 µs.

Large values (64B/4KB): `fill_rand` ~165 k ops/s — disk-bandwidth-bound.
`read_rand` 400 k/430 k/1.98 M (t1/t8/t32).

## Durability modes (`fill_rand`, Direct, 16B/128B)

| sync policy | t1 | t8 | t32 |
|---|---|---|---|
| `Never` (weak) | 1.92 M | 666 k | 425 k |
| `EveryMs(1)` (periodic) | 1.36 M | 980 k | 592 k |
| `Always` (strong) | 261 | 1.1 k | 2.6 k |

`Always` is fsync-bound — each `put` pays a real fsync; group commit only
helps with concurrent writers (~t32 ≈ one fsync per ~10 ops). Reads are
unaffected by durability mode.

## Write path: Direct vs Pipeline+async (`fill_rand`, periodic)

| threads | Direct (sync `put`) | Pipeline (`put_async`) |
|---|---|---|
| t1 | 1.36 M | 2.98 M |
| t8 | 980 k | 4.57 M |
| t32 | 592 k | 4.07 M |

`put_async` returns after ring publish (~250-300 ns p50) — it now beats
Direct at *every* thread count (TLS write overlay removed the old
single-thread round-trip penalty). Sync `put` on Pipeline still pays a
cross-thread wait — use `*_async` there.

## In-memory mode (ramdisk, `fill_rand`/`read_rand`/`mixed_50_50`)

| workload | t1 | t8 | t32 |
|---|---|---|---|
| fill_rand | 3.61 M | 1.08 M | 535 k |
| read_rand | 3.28 M | 4.47 M | 4.19 M |
| mixed_50_50 | 1.07 M | 946 k | 736 k |

Ramdisk fill t1 is ~2.7x the disk number — on APFS the `EveryMs(1)`
fsync pacing *is* a real cost for pure fills. Reads are memory-bound
either way (~4.4 M at t8).

## Reliability (6/6 PASS)

| test | result |
|---|---|
| restart | 5k keys survived clean restart, tombstone intact |
| crash_recovery | 492 fsync-acked writes verified after SIGKILL |
| wal_torn_tail | torn tail ignored, 100/100 keys intact |
| wal_corrupt_payload | open rejects post-CRC decode failure |
| checkpoint_interruption | partial state recovered, `.tmp` discarded |
| disk_full | graceful ENOSPC — writes refused, not corrupted |

## Reproduce

```sh
cd bench
cargo run --release -- run --dbs rexodb --threads 1,8,32 --out out-full   # periodic matrix
cargo run --release -- run --dbs rexodb --threads 1,8,32 --durability weak --out out-weak
cargo run --release -- run --dbs rexodb --threads 1,8,32 --durability strong --out out-strong
REXO_WRITE_PATH=pipeline REXO_ASYNC=1 cargo run --release -- run --dbs rexodb --threads 1,8,32 --out out-pipe
# in-mem: point workdir at a ramdisk (hdiutil attach -nomount ram://…, erase, mount)
cargo run --release -- run --dbs rexodb --threads 1,8,32 --quick --workdir /Volumes/ramdisk --out out-inmem
cargo run --release -- reliability --out out-rel
```

Env knobs on the rexodb adapter: `REXO_ASYNC=1` (use `*_async` writes),
`REXO_WRITE_PATH=pipeline`, `REXO_PIN=a,b` (pin consumer/fsync; hard pin
Linux, scheduling hint macOS), `REXO_MEMTABLE`/`REXO_COMPACT`/`REXO_FULLMERGE=1`
(shrink thresholds so short runs exercise flushes + merges).

Caveats: numbers swing several-x under machine load — a background VM
poisoned one full matrix here (~10x on write cells); re-measure on an
idle box and compare same-run A/B only.
