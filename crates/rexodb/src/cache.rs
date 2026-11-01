//! Sharded point cache over segment lookups.
//!
//! Entries are keyed by `(segment id, key)`: segments are immutable and the
//! read path always walks them newest→oldest, so a cached result can never
//! shadow fresher data — entries for compacted-away segments just become
//! unreachable garbage the recency eviction drops. No invalidation needed.
//!
//! Lookup cost: one `xxh3` + one shard `Mutex`. `None` inside an entry is a
//! cached tombstone (deleted in that segment).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64_with_seed;

const SHARDS: usize = 32;

struct Entry {
    seg: u64,
    key: Bytes,
    val: Option<Bytes>,
    age: u64,
}

#[derive(Default)]
struct Shard {
    map: HashMap<u64, Entry>,
    tick: u64,
}

pub struct Cache {
    shards: Vec<Mutex<Shard>>,
    /// Per-shard entry cap; 0 → cache disabled.
    cap: usize,
    pub hits: AtomicU64,
}

fn key_hash(seg: u64, key: &[u8]) -> u64 {
    xxh3_64_with_seed(key, seg)
}

impl Cache {
    pub fn new(entries: usize) -> Self {
        let cap = entries / SHARDS;
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            cap,
            hits: AtomicU64::new(0),
        }
    }

    /// `Some(v)`/`Some(None)` = hit (value / tombstone); `None` = miss.
    pub fn get(&self, seg: u64, key: &[u8]) -> Option<Option<Bytes>> {
        if self.cap == 0 {
            return None;
        }
        let h = key_hash(seg, key);
        let mut s = self.shards[(h as usize) & (SHARDS - 1)].lock();
        let Shard { map, tick } = &mut *s;
        let e = map.get_mut(&h)?;
        if e.seg != seg || &e.key[..] != key {
            return None; // hash collision — verify key, treat as miss
        }
        *tick += 1;
        e.age = *tick;
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(e.val.clone())
    }

    pub fn insert(&self, seg: u64, key: &[u8], val: Option<Bytes>) {
        if self.cap == 0 {
            return;
        }
        let h = key_hash(seg, key);
        let mut s = self.shards[(h as usize) & (SHARDS - 1)].lock();
        s.tick += 1;
        if s.map.len() >= self.cap {
            // Keep roughly the newest 3/4 by age; if everything is fresh
            // (bulk insert), a clear still bounds memory.
            let keep = s.tick.saturating_sub((self.cap as u64) * 3 / 4);
            s.map.retain(|_, e| e.age > keep);
            if s.map.len() >= self.cap {
                s.map.clear();
            }
        }
        let tick = s.tick;
        s.map.insert(h, Entry { seg, key: Bytes::copy_from_slice(key), val, age: tick });
    }
}
