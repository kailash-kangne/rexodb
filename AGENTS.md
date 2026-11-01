# Project Rules

Embedded persistent key-value store in Rust, engineered for low latency and
predictable tail latency. Correctness and durability are non-negotiable;
every optimization must be proven with a benchmark, never assumed.

## Layout

```
crates/
├── wal/       # append-only WAL: CRC32 records, group-commit writer thread, replay
├── segment/   # immutable sorted files: sparse index, tombstones, pread readers
└── rexodb/    # public API + engine: memtable, flush, compaction, recovery
```

## Commands

- `cargo build` / `cargo test --all-targets` / `cargo clippy --all-targets` —
  workspace-wide, from the repo root.
- `cargo bench --bench rexodb_bench` — throughput suite (criterion).
- `cargo bench --bench latency` — p50/p95/p99/p999 latency suite.
- `cd bench && cargo run --release -- run` — comparative harness vs
  sqlite/lmdb/redb (workspace-excluded crate, own deps; `--quick` smoke,
  `--inmem` ramdisk, `--baseline/--save-baseline` regression checks).
- `cd bench && cargo run --release -- reliability` — crash/corruption/
  disk-full/restart tests (SIGKILLs child processes, mounts ramdisks).

## Invariants that must not be broken

- **Commit order:** WAL append always precedes memtable apply, both under
  `Inner::write_mu`. A record logged to WAL gen *g* is guaranteed to be in
  the memtable frozen for gen *g* — WAL rotation + memtable swap happen
  atomically inside `write_mu`.
- **Segment recency = filename order:** segments are written to `*.tmp`,
  fsync'd, then renamed to `seg/{gen:016}.sst` where `gen` is allocated at
  *commit* time. Never name a segment before its data placement is final.
  **Compaction outputs are the exception:** a merged run takes its *oldest
  input's* gen (the rename replaces that file atomically) — a fresh gen
  would sort it newest at reopen and resurrect shadowed data.
- **One live WAL:** a `wal/*.log` file exists iff its memtable hasn't been
  committed to a segment. `Db::open` replays all WALs (idempotent), folds
  them into a segment, then deletes them.
- **Durability is honest:** `SyncPolicy::Always` waits for a real fsync
  before acknowledging. Never ack a durable commit early.
- **Torn tails are normal:** replay stops at the first bad CRC/short read —
  that's a crash boundary, not corruption. A payload that passes CRC but
  won't decode *is* corruption.
- **Tombstones are entries:** `delete` writes a tombstone that must shadow
  older segments. A merge may only drop a tombstone when its window
  includes the oldest live segment — mid-window merges (the default
  incremental policy, smallest adjacent pair) write tombstones through.

## Conventions

- Little-endian encoding everywhere. `bytes::BufMut::put_u32` is BIG-endian —
  use `put_u32_le` (this caused a real corruption bug once).
- Deps stay minimal and each is justified: `bytes` (refcounted keys/values),
  `parking_lot` (locks), `thiserror` (errors), `crc32fast` (record CRCs),
  `xxhash-rust`/`xxh3` (segment bloom filters), `arc-swap` =`1.9.3` (wait-free
  segs/imm snapshots), `core_affinity` (optional `pin_cores`),
  `criterion`+`tempfile` (dev only).
- No tokio — the DB is embedded; the only threads are the WAL writer and
  on-demand flush/compaction workers.
- Length fields from disk are bounds-checked before allocation, always.
- Tests cover the critical combinations (recovery, torn writes, batch
  atomicity, tombstones, compaction) — not exhaustive coverage.

## Learned so far (edit/remove freely)

- First bench (Apple Silicon, 128B values, default `EveryMs(1)`):
  put p50 ≈ 208 ns, get memtable p50 ≈ 156 ns, get segment p50 ≈ 1.3 µs,
  put `Always` ≈ 4 ms (fsync-bound — group commit is the concurrency win).
- Bloom filter impact, measured A/B (env-hack disable vs enable, same run):
  multi-segment miss 13.6 µs → 0.365 µs ≈ **37x faster**; `segment_hit`
  unchanged (hits still pay their one pread by design).
- Benchmark caveat: absolute numbers vary ~2x under machine load
  (load avg >8 skews criterion runs); compare same-run A/B only.
- io_uring: tried for the WAL write+fsync path (Linux-only branch, linked
  SQEs, sync fallback), measured in Docker — 22% slower on `Always`, 3x
  slower on the write-only path (submit+wait per 1-2 SQEs can't amortize).
  Reverted; would need SQPOLL + deep batching to win — revisit only if a
  real benchmark demands it.
- Segment format v2 (`RXSSEG02`): `data | sparse index | bloom | 40B footer`.
- Segment format v3 (`RXSSEG03`): entries are `klen|vlen|key|value|crc32`
  (crc covers the rest); footer grew to 48B with `index_crc`/`bloom_crc`
  (checked at `open`). Read paths verify entry CRCs as bytes stream through
  the `Cursor` hasher — `get`, `iter`, compaction all detect corruption.
  `Reader::verify()` = full-file pass for the future verify tool. v2 files
  fail the magic check and are deleted on open, same as v1 was.
- `wal::crash::point(id)` — env-gated crash injection (`REXO_CRASH_AT`,
  `REXO_CRASH_AFTER`) at every on-disk transition; aborts like SIGKILL.
  `tests/crash_points.rs` walks 1..=crash::COUNT × {Direct,Pipeline} ×
  hit#{1,2}. Adding a transition = pick the next free id + bump COUNT.
- `SyncPolicy::EveryMs` is now timer-driven, not job-arrival-driven: the
  wal writer waits on `recv_timeout(deadline)` while unfsync'd data is
  outstanding (`dirty` flag), so idle periods can't stretch the loss
  window. Regression test: `wal::tests::every_ms_fsyncs_unflushed_data_when_idle`
  (observed via the `Writer::fsyncs` counter).
- `tests/model.rs` — proptest: generated op sequences vs `BTreeMap`
  oracle, run simultaneously on Direct + Pipeline rigs (equivalence proof)
  with flush/compact/sync/reopen interleaved. `proptest` is a dev-dep of
  rexodb only.
- `cache.rs`: 32-shard point cache keyed by (seg gen, key) — immutable
  segments + newest→oldest walk make stale hits impossible, no invalidation.
  `Config::cache_entries` (default 64k, 0=off); `Stats.cache_hits`.
- WAL `rotate()` is two-phase: caller unblocks after the new file is live;
  the old file's fsync runs after — append acks still wait for it (`Always`
  contract kept). Do NOT move the fsync back before the caller ack.
- `Db::get_many(&[keys])` — batched point reads sharing layer locks.
- vs RocksDB 0.22 (same machine, periodic dur, 16/128B, 150k preload):
  rexodb wins all cells — fill 3-7x, read_rand 2.7x/1.1x (1t/8t),
  mixed 1.8-4.9x ops/s. Bench: `cd bench && cargo run --release
  --features rocksdb -- run --dbs rexodb,rocksdb --quick`.
- Benchmark policy: only run the full multi-DB comparison for a new/final
  measurement; for iteration run `--dbs rexodb` alone against
  `--baseline` — other engines' numbers don't change between runs.
  Bloom = 10 bits/key, k=7 double-hash of `xxh3_64` (LevelDB scheme), no
  false negatives — `Reader::get` short-circuits absent keys with zero I/O.
  v1 segments are rejected by the magic check and deleted on open.
- `Bytes` is not `From<&[u8; N]>` — API takes `AsRef<[u8]>` so `b"lit"` works.
- `Db::range` takes `RangeBounds<K: AsRef<[u8]>>`; call with slices
  (`db.range(b"a".as_slice()..b"z".as_slice())`) or `db.range::<[u8],_>(..)`.
- `WritePath::Pipeline` (Disruptor-style ring → single consumer owns
  WAL-write+apply in claim order; separate fsync thread paces durability).
  Measured: sync `put` pays a cross-thread round-trip (~3.5µs floor — always
  loses to Direct ~0.5µs). The win is `put_async`/`delete_async`/`write_async`
  (Aeron offer()-style, returns after ring publish + the ring seq for
  journaling): fill ~4.5M ops/s t1 at ~83ns p50. Same-thread `get` reads own
  unapplied writes from a per-pipeline TLS overlay (`Pending{tip,map}` —
  entries trusted only while seq > consumer `applied`), NOT by waiting;
  barriers (`range`/`flush`/`sync`) still wait. Post-overlay: mixed 50/50
  1.37M t1 / 4.58M t8 vs Direct 1.31M/1.16M. `Always` async writes defer
  durability to `sync()`. Default stays `Direct` — pipeline is the opt-in
  throughput path.
- `Db::close` (and `Drop`) drains `Inner::workers` (settle/compact threads)
  before returning — a worker's seg-rename/wal-unlink racing the next
  `Db::open`'s directory scans can hide committed records (observed as
  silent data loss across close+reopen).
- `Inner::segs`/`imm` are `arc_swap::ArcSwap` snapshots — readers `load()`
  wait-free; writers `rcu`/`store` a whole new list on flush/compact. The
  memtable itself stays `RwLock` (live-mutated BTreeMap — snapshot swaps
  can't cover it cheaply).
- Per-call strict durability: `put_durable`/`delete_durable`/`write_durable`
  return only after the fsync covering the record — an explicit `Sync`
  barrier under `EveryMs`/`Never` (the `Always` ack already is one). Sync
  `put` under `Never`/`EveryMs` is OS-buffered (crash-loss window);
  `put_async` defers durability to `sync()`.
- Single-owner guard: `Db::open` creates `<dir>/LOCK` + `File::try_lock`
  (std, advisory); held for the handle's lifetime, released by `close()`/drop
  — close→reopen works while the old handle is still in scope.
- Directory fsyncs: `sync_parent(path)` (wal) / same helper in lib.rs fsyncs
  the parent dir after every file create/segment rename — file `sync_all`
  covers contents, NOT the directory entry. Unix-only (`cfg!(unix)` no-op).
- Crash-kill proof: `crash_acknowledged_writes_survive` spawns a child
  writer (`Always` + progress log), SIGKILLs it mid-run, reopens, asserts
  every fsync-acked key intact — Direct AND Pipeline.
- Pipeline hot-path details: `Padded<T>` cache-line padding on hot atomics;
  `wait()` = spin→yield→condvar; `notify_waiters`/`unpark` gated by waiter
  count + `consumer_idle` flag (flag→recheck→park, no missed wakeup); slot
  `SlotBody` pools `ops`/`frame` buffers via `wal::encode_record_into` —
  zero steady-state allocs; `Config.pin_cores` = `[consumer, fsync]` pins
  (best-effort, Linux hard / macOS hint).
- `Memtable::insert` subtracts the replaced entry's bytes on overwrite —
  `mem.bytes` is rotation input; inflating it forces early rotations.
