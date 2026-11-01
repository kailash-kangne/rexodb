//! Latency-percentile benchmark. `cargo bench --bench latency` prints
//! p50/p95/p99/p999 for each op — the numbers that actually describe tail
//! behavior, which throughput hides.

use std::time::{Duration, Instant};

use rexodb::{Config, Db, SyncPolicy};
use tempfile::TempDir;

const VALUE: &[u8] = &[b'x'; 128];
const OPS: usize = 20_000;

fn key(i: u64) -> [u8; 16] {
    format!("key{i:013}").into_bytes().try_into().unwrap()
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn percentiles(mut v: Vec<Duration>) -> (Duration, Duration, Duration, Duration) {
    v.sort_unstable();
    let pick = |p: f64| v[((v.len() - 1) as f64 * p) as usize];
    (pick(0.50), pick(0.95), pick(0.99), pick(0.999))
}

fn show(name: &str, v: Vec<Duration>) {
    let (p50, p95, p99, p999) = percentiles(v);
    println!("{name:<28} p50 {:>8.1?}  p95 {:>8.1?}  p99 {:>8.1?}  p999 {:>8.1?}", p50, p95, p99, p999);
}

fn main() {
    let dir = TempDir::new().unwrap();
    let mut c = Config::new(dir.path().join("db"));
    c.sync = SyncPolicy::EveryMs(1);
    c.memtable_bytes = 512 << 20;
    let db = Db::open(c).unwrap();

    let mut rng = Rng(1);

    let mut lat = Vec::with_capacity(OPS);
    for i in 0..OPS as u64 {
        let t = Instant::now();
        db.put(key(i), VALUE).unwrap();
        lat.push(t.elapsed());
    }
    show("put (EveryMs)", lat);

    let mut lat = Vec::with_capacity(OPS);
    for _ in 0..OPS {
        let k = key(rng.next() % OPS as u64);
        let t = Instant::now();
        db.get(k).unwrap();
        lat.push(t.elapsed());
    }
    show("get (memtable hit)", lat);

    let mut lat = Vec::with_capacity(OPS);
    for _ in 0..OPS {
        let k = key(OPS as u64 + 1 + rng.next() % 1_000_000);
        let t = Instant::now();
        db.get(k).unwrap();
        lat.push(t.elapsed());
    }
    show("get (miss)", lat);

    let mut lat = Vec::with_capacity(OPS);
    for i in 0..OPS as u64 {
        let t = Instant::now();
        db.delete(key(i)).unwrap();
        lat.push(t.elapsed());
    }
    show("delete", lat);

    // SyncPolicy::Always — the fsync-per-commit number.
    let dir2 = TempDir::new().unwrap();
    let mut c2 = Config::new(dir2.path().join("db"));
    c2.sync = SyncPolicy::Always;
    let db2 = Db::open(c2).unwrap();
    let mut lat = Vec::with_capacity(2_000);
    for i in 0..2_000u64 {
        let t = Instant::now();
        db2.put(key(i), VALUE).unwrap();
        lat.push(t.elapsed());
    }
    show("put (Always/fsync)", lat);

    // Segment-resident reads after a flush.
    let dir3 = TempDir::new().unwrap();
    let mut c3 = Config::new(dir3.path().join("db"));
    c3.memtable_bytes = 1 << 16;
    let db3 = Db::open(c3).unwrap();
    for i in 0..10_000u64 {
        db3.put(key(i), VALUE).unwrap();
    }
    db3.flush().unwrap();
    let mut rng3 = Rng(3);
    let mut lat = Vec::with_capacity(OPS);
    for _ in 0..OPS {
        let k = key(rng3.next() % 10_000);
        let t = Instant::now();
        db3.get(k).unwrap();
        lat.push(t.elapsed());
    }
    show("get (segment hit)", lat);
}
