//! Throughput benchmarks for rexodb. Run: `cargo bench -p rexodb`.
//!
//! All numbers are single-process, no network — the point is the engine's
//! raw syscall/lock/alloc overhead. `SyncPolicy::EveryMs(1)` is used except
//! where noted, matching the default durability guarantee.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rexodb::{Config, Db, SyncPolicy, WriteBatch};
use tempfile::TempDir;

const VALUE: &[u8] = &[b'x'; 128];

/// Tiny deterministic PRNG — no rand dep for a bench.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn key(i: u64) -> [u8; 16] {
    format!("key{i:013}").into_bytes().try_into().unwrap()
}

fn open(dir: &TempDir, memtable_bytes: usize) -> Db {
    let mut c = Config::new(dir.path().join("db"));
    c.sync = SyncPolicy::EveryMs(1);
    c.memtable_bytes = memtable_bytes;
    Db::open(c).unwrap()
}

fn puts(c: &mut Criterion) {
    let mut g = c.benchmark_group("put");
    g.throughput(Throughput::Elements(1));

    let dir = TempDir::new().unwrap();
    let db = open(&dir, 512 << 20);
    let mut i = 0u64;
    g.bench_function("sequential_128B", |b| {
        b.iter(|| {
            db.put(key(i), VALUE).unwrap();
            i += 1;
        })
    });

    let mut rng = Rng(0x9e3779b9);
    g.bench_function("random_128B", |b| {
        b.iter(|| {
            db.put(key(rng.next() % 1_000_000), VALUE).unwrap();
        })
    });
    g.finish();
}

fn batches(c: &mut Criterion) {
    let mut g = c.benchmark_group("batch");
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 512 << 20);
    for size in [1usize, 16, 128] {
        g.throughput(Throughput::Elements(size as u64));
        g.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let mut i = 0u64;
            b.iter(|| {
                let mut batch = WriteBatch::new();
                for _ in 0..size {
                    batch.put(key(i), VALUE);
                    i += 1;
                }
                db.write(&batch).unwrap();
            })
        });
    }
    g.finish();
}

fn gets(c: &mut Criterion) {
    let mut g = c.benchmark_group("get");
    g.throughput(Throughput::Elements(1));

    // Memtable-resident hits.
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 512 << 20);
    for i in 0..10_000u64 {
        db.put(key(i), VALUE).unwrap();
    }
    let mut rng = Rng(1);
    g.bench_function("memtable_hit", |b| b.iter(|| db.get(key(rng.next() % 10_000)).unwrap()));
    g.bench_function("miss", |b| b.iter(|| db.get(key(10_000 + 1 + rng.next() % 1_000_000)).unwrap()));

    // Segment hits: small memtable forces a flush.
    let dir2 = TempDir::new().unwrap();
    let db2 = open(&dir2, 1 << 16);
    for i in 0..10_000u64 {
        db2.put(key(i), VALUE).unwrap();
    }
    db2.flush().unwrap();
    let mut rng2 = Rng(2);
    g.bench_function("segment_hit", |b| b.iter(|| db2.get(key(rng2.next() % 10_000)).unwrap()));

    // Misses across multiple segments — exercises bloom short-circuiting
    // (without blooms this costs a sparse-index probe + pread per segment).
    let dir3 = TempDir::new().unwrap();
    let mut c3 = Config::new(dir3.path().join("db"));
    c3.sync = SyncPolicy::EveryMs(1);
    c3.memtable_bytes = 1 << 16;
    c3.compact_at = usize::MAX; // keep segments split for this measurement
    let db3 = Db::open(c3).unwrap();
    for i in 0..40_000u64 {
        db3.put(key(i), VALUE).unwrap();
        if i % 10_000 == 9_999 {
            db3.flush().unwrap();
        }
    }
    db3.flush().unwrap();
    assert!(db3.stats().live_segments >= 4);
    let mut rng3 = Rng(4);
    g.bench_function("miss_multi_segment", |b| {
        b.iter(|| db3.get(key(100_000 + rng3.next() % 1_000_000)).unwrap())
    });
    g.finish();
}

fn deletes(c: &mut Criterion) {
    let mut g = c.benchmark_group("delete");
    g.throughput(Throughput::Elements(1));
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 512 << 20);
    let mut i = 0u64;
    g.bench_function("tombstone", |b| {
        b.iter(|| {
            db.delete(key(i)).unwrap();
            i += 1;
        })
    });
    g.finish();
}

fn scans(c: &mut Criterion) {
    let mut g = c.benchmark_group("scan");
    let dir = TempDir::new().unwrap();
    let db = open(&dir, 512 << 20);
    for i in 0..100_000u64 {
        db.put(key(i), VALUE).unwrap();
    }
    for n in [10u64, 100, 1000] {
        g.throughput(Throughput::Elements(n));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut rng = Rng(3);
            b.iter(|| {
                let lo_i = rng.next() % (100_000 - n);
                let lo = key(lo_i);
                let hi = key(lo_i + n);
                let cnt =
                    db.range(lo.as_slice()..hi.as_slice()).unwrap().take_while(|r| r.is_ok()).count() as u64;
                assert_eq!(cnt, n);
            })
        });
    }
    g.finish();
}

/// Concurrent scaling: N reader threads hitting the memtable + 1 writer.
fn concurrent(c: &mut Criterion) {
    let mut g = c.benchmark_group("concurrent");
    for readers in [1usize, 4, 8] {
        g.bench_with_input(BenchmarkId::new("reads_1writer", readers), &readers, |b, &r| {
            b.iter_custom(|iters| {
                let dir = TempDir::new().unwrap();
                let db = Arc::new(open(&dir, 512 << 20));
                for i in 0..10_000u64 {
                    db.put(key(i), VALUE).unwrap();
                }
                let stop = Arc::new(AtomicU64::new(0));
                let start = Instant::now();
                std::thread::scope(|s| {
                    // Writer: `iters` puts, then signals readers to stop.
                    let dbw = db.clone();
                    let stopw = stop.clone();
                    s.spawn(move || {
                        for i in 0..iters {
                            dbw.put(key(10_000 + i), VALUE).unwrap();
                        }
                        stopw.store(1, Ordering::Relaxed);
                    });
                    for t in 0..r {
                        let db = db.clone();
                        let stop = stop.clone();
                        s.spawn(move || {
                            let mut rng = Rng(t as u64 + 1);
                            while stop.load(Ordering::Relaxed) == 0 {
                                db.get(key(rng.next() % 10_000)).unwrap();
                            }
                        });
                    }
                });
                start.elapsed()
            })
        });
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(3)).warm_up_time(Duration::from_secs(1));
    targets = puts, batches, gets, deletes, scans, concurrent
}
criterion_main!(benches);
