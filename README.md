<p align="center">
  <img src="logo.png" alt="rexodb" width="420">
</p>

**Ultra-low-latency embedded key-value store for Rust — disruptor-grade writes, crash-proven durability.**

[![License: BUSL-1.1](https://img.shields.io/badge/license-BUSL--1.1-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.89%2B-orange.svg)](Cargo.toml)

A WAL + LSM-inspired storage engine built for tail latency, not averages.
Single-writer ring buffer, wait-free reads, group commit, zero-copy `Bytes`
end to end — and a test suite that SIGKILLs real processes to prove the
durability claims.

---

## 🚀 Numbers

Measured on an M-series MacBook, `--release`, quiet machine. Yours will differ —
treat as ballpark, see `bench/` to reproduce.

| Operation | Throughput | p50 | p99 |
|---|---:|---:|---:|
| `put_async` (pipeline ring) | **4.5 M ops/s** | **83 ns** | 292 ns |
| `put` (direct, periodic sync) | 1.5 M ops/s | 208 ns | 875 ns |
| `get` (hot) | **3.1 M ops/s** | 292 ns | 625 ns |
| `get` × 8 threads | 4.7 M ops/s | 1.1 µs | 8.3 µs |
| mixed 50/50 × 8 threads (pipeline) | **4.6 M ops/s** | 958 ns | 3.6 µs |
| `put_durable` (strict, real fsync) | ~250 ops/s | ~4 ms | — |

> Honest footnote: strict durability costs a real fsync — ~4 ms on this
> machine. Group commit folds ~5 concurrent writers into each fsync at 8
> threads. No disk store escapes this; it's physics, not engineering.

## 🧠 Why it's fast

```
 producers ──claim──▶ ring of slots ──single consumer──▶ WAL write ──▶ memtable
      │                    │                               │
   atomic seq          bounded,                        fsync thread
   (no mutex)        backpressured                     paces durability
```

- **Disruptor write path** — producers claim a slot by atomic counter;
  one consumer drains in order. No lock on the hot path.
- **Cache-line-padded atomics** — `next_claim`/`applied`/`durable` each on
  their own line; no cross-core ping-pong.
- **Wait-free reads** — segment/imm lists are `arc-swap` snapshots; `get`
  never takes a lock except the live memtable's `RwLock::read`.
- **Read-your-writes without waiting** — per-thread overlay of your own
  pending writes, so `get` after `put_async` stays ~300 ns instead of
  paying a cross-thread round trip.
- **Zero steady-state allocs** — ring slots carry reusable buffers.
- **Group commit everywhere** — one `write` + at most one `fsync` per
  drained batch, on every write path.
- **Optional CPU pinning** — pin consumer/fsync threads to cores:
  `cfg.pin_cores = vec![4, 5]`.

## 🛡️ Durability — three honest modes

| Mode | Acknowledged write survives | Cost |
|---|---|---|
| `SyncPolicy::Always` / `put_durable` | process crash + power loss | ~one fsync per commit |
| `EveryMs(ms)` (default `1`) | process crash; power loss ≤ `ms`, timer-driven even when idle | none per write |
| `Never` | nothing — call `db.sync()` yourself | none |

- **CRC32 per WAL record** — a torn tail is detected and dropped on replay.
- **CRC32 per segment entry + per index/bloom block** — bit-rot in a segment
  surfaces as `Corrupt` on read (`Reader::verify` checks a whole file);
  it can never return wrong data.
- **Directory fsyncs** on every create/rename — a crash can't lose a file's
  *name* even though its contents were fsynced.
- **Lockfile** — two processes can't open the same dir concurrently.
- **Crash-kill tested** — `crash_acknowledged_writes_survive` SIGKILLs a
  writer mid-run, reopens, and asserts every fsync-acked write is intact.
- **Batches are atomic** — `Db::write` is one WAL record: all or nothing,
  across crashes.

## 📦 Usage

```rust
use rexodb::{Config, Db, SyncPolicy, WriteBatch, WritePath};

let mut cfg = Config::new("/path/to/db");
cfg.sync = SyncPolicy::EveryMs(1);   // default: ≤1 ms loss window
let db = Db::open(cfg)?;

db.put(b"k1", b"v1")?;
assert_eq!(db.get(b"k1")?.as_deref(), Some(b"v1".as_slice()));

// The HFT write path: ~83 ns ring publish, returns your sequence number.
cfg.write_path = WritePath::Pipeline;
let db = Db::open(cfg)?;
let seq = db.put_async(b"order:4521", b"filled")?;   // journal by seq

// Critical records: fsync before returning, whatever the policy.
db.put_durable(b"trade:1", b"...")?;

let mut b = WriteBatch::new();
b.put(b"a", b"1").delete(b"b");
db.write(&b)?;                     // atomic — one WAL record

for entry in db.range(b"a".as_slice()..b"z".as_slice())? {
    let (k, v) = entry?;
}

db.sync()?;   // fsync barrier
db.close()?;  // fsync + drain background work + release lock
```

## � API reference — every call

| Call | Does | Cost / detail |
|---|---|---|
| `Db::open(cfg)` | Create dirs, take `LOCK`, scan segs, replay WALs | startup bounded by `memtable_bytes` |
| `db.put(k, v)` | WAL append → memtable apply | ~208 ns (periodic); ~4 ms under `Always` |
| `db.delete(k)` | Append a tombstone | same as `put` |
| `db.write(&batch)` | Multi-op, **one WAL record** — atomic or absent, even across crashes | one append + one apply pass |
| `db.get(k)` | memtable → imm → cache → bloom → segment index | ~292 ns hot; ~0 I/O on absent keys (bloom) |
| `db.get_many(&[k])` | Batched point reads; shares snapshot loads across keys | cheaper/key than N×`get` |
| `db.range(a..z)` | Ordered merge iterator over memtable + imm + all segments | waits for own pending async writes |
| `db.put_async(k,v)` → `u64` | Pipeline only: publish to the ring, returns seq for journaling/replay | **~83 ns**; durability deferred to `sync()` |
| `db.delete_async(k)` → `u64` | Same, for a tombstone | ~83 ns |
| `db.write_async(&batch)` → `u64` | Same, atomic batch | ~83 ns + batch size |
| `db.put_durable(k,v)` | Returns only after the **fsync** covering this record | ~4 ms — for critical records only |
| `db.delete_durable(k)` | Same for a tombstone | ~4 ms |
| `db.write_durable(&batch)` | Same for an atomic batch | ~4 ms |
| `db.sync()` | fsync barrier: everything acked before it is durable, any policy | ~4 ms |
| `db.flush()` | Force memtable → segment now (normally automatic at `memtable_bytes`) | bounds WAL size / reopen time |
| `db.compact()` | Merge all live segments now (normally auto past `compact_at`) | drops tombstones, reclaims space |
| `db.close()` | Stop writes → fsync → drain flush/compact workers → release `LOCK` | idempotent; `Drop` calls it |
| `db.stats()` | Snapshot of counters (puts/gets/hits/flushes/compactions/…) | free |

`WriteBatch`: `new()` → `put(k,v)` / `delete(k)` (chainable) → `len()` / `is_empty()`.

## �🗂️ On disk

```
db/
├── LOCK                        exclusive flock — single owner
├── wal/0000000000000005.log    crc32 | len | payload frames, append-only
└── seg/0000000000000007.sst    crc'd entries + sparse index + XXH3 bloom + footer
```

- **Memtable** — `BTreeMap`; rotates to a segment at `memtable_bytes`.
- **Segments** — immutable, commit by atomic rename; a miss costs zero I/O
  (bloom), a cold hit ~1 `pread`.
- **Compaction** — full merge past `compact_at` segments; newest-wins,
  tombstones dropped.
- **Recovery** — replay WALs oldest→newest, fold to a segment, delete logs.
  Startup cost is bounded by `memtable_bytes`, not total WAL size.

## 🔧 Tuning knobs (`Config`)

| Field | Default | What it does |
|---|---|---|
| `sync` | `EveryMs(1)` | durability policy (see table above) |
| `write_path` | `Direct` | `Pipeline` = ring+consumer architecture |
| `memtable_bytes` | 64 MiB | memtable → segment rotation threshold |
| `cache_entries` | 64 k | sharded point-lookup cache |
| `ring_capacity` | 16 k | pipeline ring slots (pow2, ≥16) |
| `pin_cores` | `[]` | `[consumer, fsync]` CPU pins (best-effort) |
| `compact_at` | — | segments before auto-compaction |

## 🧪 Bench

```
cargo bench --bench rexodb_bench   # throughput (criterion)
cargo bench --bench latency        # p50/p95/p99/p999

cd bench && cargo run --release -- run --dbs rexodb --quick          # default path
REXO_WRITE_PATH=pipeline REXO_ASYNC=1 cargo run --release -- run --dbs rexodb --quick
REXO_PIN=4,5  # pin consumer→core 4, fsync→core 5
```

## License

[BUSL-1.1](LICENSE)
