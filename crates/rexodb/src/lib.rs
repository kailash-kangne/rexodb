//! # rexodb
//!
//! Embedded persistent key-value store engineered for low latency.
//!
//! ```text
//! writes → WAL (group-commit writer thread) → memtable (BTreeMap)
//! reads  → memtable → imm (flushing memtable) → segments newest→oldest
//! memtable full → rotate WAL, freeze into imm, flush to sorted segment
//! segments > compact_at → incremental merge of the smallest adjacent pair
//! ```
//!
//! # Durability contract
//!
//! - [`SyncPolicy::Always`]: `put`/`write`/`delete` return only after the
//!   record is written *and* fsync'd — an acknowledged write survives
//!   power loss.
//! - [`SyncPolicy::EveryMs(ms)`]: acknowledged writes are guaranteed queued to
//!   the WAL in order; the writer fsyncs at least every `ms` while unfsync'd
//!   data is outstanding, load or no load — a crash loses at most ~`ms` of
//!   acknowledged writes.
//! - [`SyncPolicy::Never`]: acknowledged writes may live only in kernel/user
//!   buffers; loss window is OS-dependent. Call [`Db::sync`] for an explicit
//!   durability barrier with any policy.
//!
//! A batch ([`Db::write`]) is one WAL record — it applies atomically or not
//! at all, including across crashes.
//!
//! # Recovery invariant
//!
//! A WAL file exists iff its memtable has not been committed to a segment.
//! `Db::open` replays every WAL file (oldest→newest, idempotent) into a fresh
//! memtable, flushes it to a segment, then deletes the replayed files —
//! so startup work is bounded by `memtable_bytes`, not total WAL size.
//! Segment files are committed by atomic rename with the generation number
//! assigned at commit, so filename order always equals recency; a
//! crash-torn segment never has a valid footer and is discarded on open.

mod cache;
mod memtable;
mod merge;
mod pipeline;

use std::cell::RefCell;
use std::collections::HashMap;

use std::fs;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rexodb_segment as segment;
use rexodb_wal::{self as wal, Op};

pub use rexodb_wal::SyncPolicy;

/// Crash-injection points at every on-disk storage transition — armed via
/// `REXO_CRASH_AT`/`REXO_CRASH_AFTER`, used by `tests/crash_points.rs`.
/// No-ops when the env vars are unset.
#[doc(hidden)]
pub use rexodb_wal::crash;

use memtable::Memtable;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Wal(#[from] wal::WalError),
    #[error(transparent)]
    Segment(#[from] segment::SegmentError),
    #[error("key too large: {0} bytes > max_key_bytes {1}")]
    KeyTooLarge(usize, usize),
    #[error("value too large: {0} bytes > max_value_bytes {1}")]
    ValueTooLarge(usize, usize),
    #[error("database is closed")]
    Closed,
    #[error("background task failed, writes refused: {0}")]
    Background(String),
}

pub type Result<T> = std::result::Result<T, Error>;

// Per-thread overlay of this thread's own `*_async` writes, keyed per open
// pipeline. Same-thread reads consult it instead of waiting for the
// consumer — read-your-writes without a cross-thread round trip.
struct Pending {
    /// Highest seq this thread submitted — everything ≤ `applied` is in
    /// the committed view, so the map only matters while `tip` is ahead.
    tip: u64,
    /// key → (submit seq, value); `None` value = pending tombstone.
    map: HashMap<Bytes, (u64, Option<Bytes>)>,
}

thread_local! {
    static PIPELINE_PENDING: RefCell<HashMap<usize, Pending>> = RefCell::new(HashMap::new());
}

fn record_pipeline_ops(key: usize, seq: u64, ops: &[Op], applied: u64) {
    PIPELINE_PENDING.with(|p| {
        let mut map = p.borrow_mut();
        let pend = map.entry(key).or_insert_with(|| Pending { tip: 0, map: HashMap::new() });
        if applied >= pend.tip {
            pend.map.clear(); // consumer caught up — wholesale prune
        }
        pend.tip = seq;
        for op in ops {
            let (k, v) = match op {
                Op::Put(k, v) => (k, Some(v.clone())),
                Op::Delete(k) => (k, None),
            };
            pend.map.insert(k.clone(), (seq, v));
        }
    });
}

/// Own pending write for `k` if this thread submitted one newer than
/// `applied` on this pipeline. `Some(Some(v))` put, `Some(None)`
/// tombstone, `None` → take the normal path (everything ≤ applied is
/// already visible there, and pending ops from *other* threads are not
/// this thread's to observe).
fn pending_lookup(key: usize, applied: u64, k: &[u8]) -> Option<Option<Bytes>> {
    PIPELINE_PENDING.with(|p| {
        let mut p = p.borrow_mut();
        let pend = p.get_mut(&key)?;
        if applied >= pend.tip {
            pend.map.clear();
            return None;
        }
        match pend.map.get(k) {
            Some(&(es, ref v)) if es > applied => Some(v.clone()),
            _ => None,
        }
    })
}

fn pending_tip(key: usize) -> Option<u64> {
    PIPELINE_PENDING.with(|p| p.borrow().get(&key).map(|pend| pend.tip))
}

/// Where the WAL append + memtable apply happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePath {
    /// On the calling thread, serialized by one write mutex — lowest
    /// single-thread latency.
    Direct,
    /// Producers publish to a ring buffer; one consumer thread owns
    /// WAL-write + memtable apply in claim order — much higher throughput
    /// with concurrent writers, slightly higher single-thread latency.
    Pipeline,
}

pub struct Config {
    /// Database directory (created if missing).
    pub path: PathBuf,
    /// Durability guarantee for acknowledged writes — see module docs.
    /// Default `EveryMs(1)`: ≤1 ms loss window with no per-write fsync.
    pub sync: SyncPolicy,
    /// Approximate memtable byte budget before it flushes to a segment.
    pub memtable_bytes: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    /// Data entries per sparse-index slot in each segment (lower = faster
    /// cold lookups, more RAM per segment).
    pub index_every: usize,
    /// Merge once this many live segments accumulate. Default policy
    /// merges the smallest adjacent pair (bounded pause); `full_merge`
    /// reverts to merging everything at once.
    pub compact_at: usize,
    pub full_merge: bool,
    /// Point-cache entries across all shards; segment hits are served from
    /// here without a `pread`. 0 disables.
    pub cache_entries: usize,
    /// Write path — see [`WritePath`]. Default `Direct` (lowest `put`
    /// latency); choose `Pipeline` with the `*_async` API for max
    /// concurrent-write throughput.
    pub write_path: WritePath,
    /// (Pipeline only) ring slots; each holds one write or batch. Rounded
    /// up to a power of two, minimum 16.
    pub ring_capacity: usize,
    /// (Pipeline only) CPU ids to pin background threads to: `[0]` →
    /// consumer, `[1]` → fsync. Empty = let the scheduler decide (default).
    /// Hard pins on Linux, scheduling hints on macOS/Windows; best-effort.
    pub pin_cores: Vec<usize>,
}

impl Config {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            sync: SyncPolicy::EveryMs(1),
            memtable_bytes: 32 * 1024 * 1024,
            max_key_bytes: 64 * 1024,
            max_value_bytes: 64 * 1024 * 1024,
            index_every: 16,
            compact_at: 4,
            full_merge: false,
            cache_entries: 1 << 16,
            write_path: WritePath::Direct,
            ring_capacity: 1 << 14,
            pin_cores: Vec::new(),
        }
    }
}

/// Point-in-time counter snapshot — see [`Db::stats`].
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub puts: u64,
    pub deletes: u64,
    pub gets: u64,
    pub memtable_hits: u64,
    pub segment_reads: u64,
    pub cache_hits: u64,
    pub batches: u64,
    pub flushes: u64,
    pub compactions: u64,
    pub live_segments: usize,
    pub memtable_bytes: usize,
}

#[derive(Default)]
struct Counters {
    puts: AtomicU64,
    deletes: AtomicU64,
    gets: AtomicU64,
    memtable_hits: AtomicU64,
    segment_reads: AtomicU64,
    batches: AtomicU64,
    flushes: AtomicU64,
    compactions: AtomicU64,
}

struct Inner {
    cfg: Config,
    mem: RwLock<Memtable>,
    /// Memtable being flushed — checked after `mem`, before segments.
    /// Atomic snapshot: readers never wait on a commit's swap.
    imm: ArcSwap<Option<Arc<Memtable>>>,
    /// Live segments, newest first. Same snapshot pattern — a flush or
    /// compaction publishes a whole new list, readers load it wait-free.
    segs: ArcSwap<Vec<Arc<segment::Reader>>>,
    /// `Direct` write path only — `Pipeline` owns its WAL file instead.
    wal: Option<wal::Writer>,
    /// `Pipeline` write path — started after `Inner` is in an `Arc`.
    pipeline: OnceLock<pipeline::Pipeline>,
    /// WAL file currently receiving appends.
    wal_gen: AtomicU64,
    /// Next file-generation number (wal files + committed segment renames).
    next_file: AtomicU64,
    /// Serializes wal-append + memtable-apply so every logged op lands in the
    /// memtable generation its WAL file belongs to — the invariant WAL
    /// rotation + recovery rely on (`Direct` path; `Pipeline` gets the same
    /// ordering from its single consumer).
    write_mu: Mutex<()>,
    compacting: AtomicBool,
    closed: AtomicBool,
    /// In-flight background workers (settle/compact threads). Drained on
    /// close so no worker's file ops can race a subsequent `Db::open` —
    /// a settle renaming/deleting files mid-scan can hide committed data.
    workers: (Mutex<usize>, parking_lot::Condvar),
    /// Set by a failed flush/compaction; refuses later writes instead of
    /// acknowledging data that can't persist.
    bg_error: Mutex<Option<String>>,
    cache: cache::Cache,
    counters: Counters,
    /// Held for the Db's lifetime — released by `close()` or on drop.
    _lock: fs::File,
}

impl Inner {
    fn wal_path(&self, gen: u64) -> PathBuf {
        self.cfg.path.join("wal").join(format!("{gen:016}.log"))
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Atomic multi-op write — one WAL record, applied or not.
#[derive(Default)]
pub struct WriteBatch {
    ops: Vec<Op>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> &mut Self {
        self.ops.push(Op::Put(Bytes::copy_from_slice(key.as_ref()), Bytes::copy_from_slice(value.as_ref())));
        self
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> &mut Self {
        self.ops.push(Op::Delete(Bytes::copy_from_slice(key.as_ref())));
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

pub struct Db {
    inner: Arc<Inner>,
}

impl Db {
    pub fn open(cfg: Config) -> Result<Self> {
        let seg_dir = cfg.path.join("seg");
        let wal_dir = cfg.path.join("wal");
        fs::create_dir_all(&seg_dir)?;
        fs::create_dir_all(&wal_dir)?;

        // Single-owner guard: a second live Db on this dir would interleave
        // WAL writes and race segment commits — corruption. Advisory lock,
        // held for the handle's lifetime; released on close/drop.
        let lock =
            fs::OpenOptions::new().create(true).write(true).truncate(false).open(cfg.path.join("LOCK"))?;
        if let Err(e) = lock.try_lock() {
            return Err(io::Error::other(format!("{} is already open: {e}", cfg.path.display())).into());
        }
        sync_parent(&seg_dir)?; // covers the new seg/, wal/, LOCK entries
        if cfg!(unix) {
            // and this dir's own entry, in case create_dir_all made it
            if let Some(parent) = cfg.path.parent() {
                fs::File::open(parent)?.sync_all()?;
            }
        }

        // Live segments, newest first (filename gen = commit order).
        let mut segs: Vec<Arc<segment::Reader>> = Vec::new();
        let mut max_gen = 0u64;
        for (gen, path) in numbered_files(&seg_dir, "sst")?.into_iter().rev() {
            match segment::Reader::open(&path) {
                Ok(r) => segs.push(Arc::new(r)),
                Err(_) => {
                    // Crash-torn segment (commit is rename, so this can only
                    // be a partial tmp write renamed by an unclean shutdown —
                    // or fs corruption). Either way it can't be trusted.
                    let _ = fs::remove_file(&path);
                }
            }
            max_gen = max_gen.max(gen);
        }
        // Leftover uncommitted segment writes from a crashed flush/compact.
        for (gen, path) in numbered_files(&seg_dir, "tmp")? {
            let _ = fs::remove_file(&path);
            max_gen = max_gen.max(gen);
        }

        // Replay every WAL file oldest→newest into a fresh memtable. Order
        // preserves last-writer-wins; replay is idempotent, so files already
        // covered by a segment (crash between commit and unlink) are harmless.
        let mut mem = Memtable::default();
        for (gen, path) in numbered_files(&wal_dir, "log")? {
            max_gen = max_gen.max(gen);
            let file_len = fs::metadata(&path)?.len();
            let valid = wal::replay(&path, |ops| {
                for op in &ops {
                    mem.apply(op);
                }
            })?;
            if valid < file_len {
                wal::truncate(&path, valid)?; // drop torn tail
            }
        }

        let mut next_file = max_gen + 1;

        // Fold replayed data into a segment so the replayed WAL files can be
        // deleted — restores the single-live-WAL invariant and bounds reopen
        // time by memtable_bytes.
        if !mem.is_empty() {
            let reader = write_segment(
                &cfg,
                &seg_dir,
                &mut || {
                    let g = next_file;
                    next_file += 1;
                    g
                },
                mem_entries(&mem),
            )?;
            if let Some(r) = reader {
                segs.insert(0, Arc::new(r));
            }
            mem = Memtable::default();
            // Replayed data is committed as a segment but its WAL files
            // still exist — a crash here must replay idempotently.
            crash::point(crash::RECOVERY_FOLDED);
        }
        // Replayed WALs are now covered by segments (or held nothing valid) —
        // delete them all to restore the single-live-WAL invariant.
        for (_, path) in numbered_files(&wal_dir, "log")? {
            let _ = fs::remove_file(&path);
        }

        let wal_gen = next_file;
        next_file += 1;
        let wal_path = wal_dir.join(format!("{wal_gen:016}.log"));
        let wal = match cfg.write_path {
            WritePath::Direct => Some(wal::Writer::open(&wal_path, cfg.sync)?),
            WritePath::Pipeline => None,
        };

        let inner = Arc::new(Inner {
            cache: cache::Cache::new(cfg.cache_entries),
            cfg,
            mem: RwLock::new(mem),
            imm: ArcSwap::from_pointee(None),
            segs: ArcSwap::from_pointee(segs),
            wal,
            pipeline: OnceLock::new(),
            wal_gen: AtomicU64::new(wal_gen),
            next_file: AtomicU64::new(next_file),
            write_mu: Mutex::new(()),
            compacting: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            workers: (Mutex::new(0), parking_lot::Condvar::new()),
            bg_error: Mutex::new(None),
            counters: Counters::default(),
            _lock: lock,
        });
        if inner.cfg.write_path == WritePath::Pipeline {
            let p = pipeline::Pipeline::start(&inner, &wal_path)?;
            let _ = inner.pipeline.set(p);
        }
        Ok(Self { inner })
    }

    /// `Some(value)`, `None` if absent or deleted.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let key = key.as_ref();
        self.check_key(key)?;
        self.check_open()?;
        self.inner.counters.gets.fetch_add(1, Ordering::Relaxed);

        // Read-your-writes without the wait: own pending `*_async` writes
        // overlay the committed view — anything older is already visible.
        if let Some(v) = self.pending_own(key) {
            return Ok(v);
        }
        if let Some(v) = self.inner.mem.read().lookup(key) {
            self.inner.counters.memtable_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(v);
        }
        if let Some(imm) = self.inner.imm.load().as_ref() {
            if let Some(v) = imm.lookup(key) {
                self.inner.counters.memtable_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(v);
            }
        }
        let segs = self.inner.segs.load();
        for seg in segs.iter() {
            if let Some(c) = self.inner.cache.get(seg.uid(), key) {
                match c {
                    Some(v) => {
                        self.inner.counters.segment_reads.fetch_add(1, Ordering::Relaxed);
                        return Ok(Some(v));
                    }
                    None => return Ok(None),
                }
            }
            match seg.get(key)? {
                Some(segment::Found::Value(v)) => {
                    self.inner.cache.insert(seg.uid(), key, Some(v.clone()));
                    self.inner.counters.segment_reads.fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(v));
                }
                Some(segment::Found::Tombstone) => {
                    self.inner.cache.insert(seg.uid(), key, None);
                    return Ok(None);
                }
                None => {}
            }
        }
        Ok(None)
    }

    /// Point lookups for a batch of keys — shares the memtable/segment read
    /// locks across the whole batch instead of taking them per key.
    /// Results align with `keys`; `None` = absent or deleted.
    pub fn get_many(&self, keys: &[impl AsRef<[u8]>]) -> Result<Vec<Option<Bytes>>> {
        self.check_open()?;
        let mut out: Vec<Option<Bytes>> = vec![None; keys.len()];
        // Own pending `*_async` writes overlay the committed view — one
        // watermark snapshot covers the whole batch (see `pending_own`).
        let (pkey, applied) = match self.inner.cfg.write_path {
            WritePath::Pipeline => {
                self.inner.pipeline.get().map(|p| (p.key(), p.applied())).unwrap_or((0, 0))
            }
            WritePath::Direct => (0, 0),
        };
        let mut pending: Vec<(usize, &[u8])> = Vec::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            self.check_key(k.as_ref())?;
            let k = k.as_ref();
            if self.inner.cfg.write_path == WritePath::Pipeline {
                if let Some(v) = pending_lookup(pkey, applied, k) {
                    out[i] = v;
                    continue;
                }
            }
            pending.push((i, k));
        }
        self.inner.counters.gets.fetch_add(keys.len() as u64, Ordering::Relaxed);
        let mut hits = 0u64;

        pending = filter_mem(&self.inner.mem.read(), pending, &mut out, &mut hits);
        if !pending.is_empty() {
            if let Some(imm) = self.inner.imm.load().as_ref() {
                pending = filter_mem(imm, pending, &mut out, &mut hits);
            }
        }
        self.inner.counters.memtable_hits.fetch_add(hits, Ordering::Relaxed);

        let segs = self.inner.segs.load();
        for seg in segs.iter() {
            if pending.is_empty() {
                break;
            }
            let mut still = Vec::with_capacity(pending.len());
            for (i, k) in pending {
                match self.inner.cache.get(seg.uid(), k) {
                    Some(Some(v)) => {
                        self.inner.counters.segment_reads.fetch_add(1, Ordering::Relaxed);
                        out[i] = Some(v);
                        continue;
                    }
                    Some(None) => continue, // cached tombstone — final
                    None => {}
                }
                match seg.get(k)? {
                    Some(segment::Found::Value(v)) => {
                        self.inner.cache.insert(seg.uid(), k, Some(v.clone()));
                        self.inner.counters.segment_reads.fetch_add(1, Ordering::Relaxed);
                        out[i] = Some(v);
                    }
                    Some(segment::Found::Tombstone) => {
                        self.inner.cache.insert(seg.uid(), k, None);
                    }
                    None => still.push((i, k)),
                }
            }
            pending = still;
        }
        Ok(out)
    }

    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        self.apply(&[Op::Put(Bytes::copy_from_slice(key.as_ref()), Bytes::copy_from_slice(value.as_ref()))])
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.apply(&[Op::Delete(Bytes::copy_from_slice(key.as_ref()))])
    }

    /// Apply `batch` atomically — a single WAL record.
    pub fn write(&self, batch: &WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.inner.counters.batches.fetch_add(1, Ordering::Relaxed);
        self.apply(&batch.ops)
    }

    /// Strict durability for this call alone: returns only after the fsync
    /// covering the record. Under `EveryMs`/`Never` that's a forced `Sync`
    /// barrier (not the policy cadence); under `Always` it's the normal
    /// ack. For critical records (e.g. trade writes) inside a weaker-mode
    /// workload — costs a real fsync (~ms).
    pub fn put_durable(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        self.apply_durable(
            &[Op::Put(Bytes::copy_from_slice(key.as_ref()), Bytes::copy_from_slice(value.as_ref()))],
            true,
        )
    }

    /// [`Db::put_durable`] for deletes.
    pub fn delete_durable(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.apply_durable(&[Op::Delete(Bytes::copy_from_slice(key.as_ref()))], true)
    }

    /// [`Db::put_durable`] for atomic batches.
    pub fn write_durable(&self, batch: &WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.inner.counters.batches.fetch_add(1, Ordering::Relaxed);
        self.apply_durable(&batch.ops, true)
    }

    /// Like [`Db::put`], but returns once the record is queued to the write
    /// pipeline instead of applied to the memtable — the fast path for
    /// throughput-bound writers (`Pipeline` write path only; on `Direct`
    /// this just calls `put`).
    ///
    /// Same-thread `get`/`get_many`/`range` still observe the write — they
    /// wait for your own outstanding submissions. Other threads may not
    /// observe it until the consumer applies it (~µs). Durability follows
    /// the normal rules: with `SyncPolicy::Always`, an async write is *not*
    /// durable on return — call [`Db::sync`] for the barrier.
    ///
    /// A pipeline failure surfaces on a later call instead of this one.
    ///
    /// Returns the ring sequence number — a durable total order over all
    /// writes to this open instance, usable for journaling/replay. `0` on
    /// the `Direct` write path (no sequencer exists there).
    pub fn put_async(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<u64> {
        self.apply_async(&[Op::Put(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        )])
    }

    /// [`Db::put_async`] for deletes.
    pub fn delete_async(&self, key: impl AsRef<[u8]>) -> Result<u64> {
        self.apply_async(&[Op::Delete(Bytes::copy_from_slice(key.as_ref()))])
    }

    /// [`Db::put_async`] for atomic batches.
    pub fn write_async(&self, batch: &WriteBatch) -> Result<u64> {
        if batch.is_empty() {
            return Ok(0);
        }
        self.inner.counters.batches.fetch_add(1, Ordering::Relaxed);
        self.apply_async(&batch.ops)
    }

    /// Ordered iterator over a key range — newest data wins, deletes are
    /// filtered. `db.range::<[u8], _>(..)` scans everything.
    pub fn range<K, R>(&self, bounds: R) -> Result<RangeIter>
    where
        K: AsRef<[u8]> + ?Sized,
        R: RangeBounds<K>,
    {
        self.check_open()?;
        self.wait_own_writes()?;
        let start = bounds.start_bound().map(|k| Bytes::copy_from_slice(k.as_ref()));
        let end = bounds.end_bound().map(|k| Bytes::copy_from_slice(k.as_ref()));
        // Reversed/degenerate bounds (e.g. `Excluded(x)..=Included(x)`) would
        // panic inside BTreeMap::range — return an empty iterator instead.
        if range_empty(&start, &end) {
            return Ok(RangeIter { merge: merge::MergeIter::new(Vec::new())?, start, end });
        }
        let mem_bounds = (bound_ref(&start), bound_ref(&end));

        let mut sources: Vec<merge::Source> = Vec::new();
        {
            let snap = self.inner.mem.read().snapshot(mem_bounds);
            sources.push(merge::Source::Mem(snap.into_iter()));
        }
        if let Some(imm) = self.inner.imm.load().as_ref() {
            sources.push(merge::Source::Mem(imm.snapshot(mem_bounds).into_iter()));
        }
        {
            let segs = self.inner.segs.load();
            for seg in segs.iter() {
                let it = match &start {
                    Bound::Included(k) | Bound::Excluded(k) => seg.iter_from(k.as_ref())?,
                    Bound::Unbounded => seg.iter()?,
                };
                sources.push(merge::Source::Seg(it));
            }
        }
        Ok(RangeIter { merge: merge::MergeIter::new(sources)?, start, end })
    }

    /// fsync barrier: everything acknowledged before this call is durable,
    /// regardless of the configured policy.
    pub fn sync(&self) -> Result<()> {
        self.check_open()?;
        match self.inner.cfg.write_path {
            WritePath::Pipeline => self.pipeline()?.sync(),
            WritePath::Direct => {
                self.inner.wal.as_ref().expect("direct mode has a wal").sync()?;
                Ok(())
            }
        }
    }

    /// Forces a memtable flush: waits for any in-flight flush, rotates the
    /// WAL and writes the current memtable to a segment synchronously.
    pub fn flush(&self) -> Result<()> {
        self.check_open()?;
        while self.inner.imm.load().is_some() {
            self.check_bg()?;
            thread::sleep(Duration::from_millis(1));
        }
        if self.inner.cfg.write_path == WritePath::Pipeline {
            let frozen = self.pipeline()?.freeze()?;
            let Some(f) = frozen else { return Ok(()) };
            return pipeline::settle_frozen(self.inner.clone(), Arc::new(f));
        }
        let g = self.lock_write()?;
        let frozen = self.freeze()?;
        drop(g);
        let Some((gen, imm)) = frozen else { return Ok(()) };
        commit_flush(&self.inner, gen, &imm)
    }

    /// Merges all live segments into one synchronously — reclaims space from
    /// overwritten keys and tombstones.
    pub fn compact(&self) -> Result<()> {
        self.check_open()?;
        compact_all(&self.inner)
    }

    /// Stops accepting writes, fsyncs everything queued, and drains any
    /// in-flight flush/compaction so files are stable when it returns.
    pub fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let res = match self.inner.cfg.write_path {
            WritePath::Pipeline => match self.inner.pipeline.get() {
                Some(p) => p.close(),
                None => Ok(()),
            },
            WritePath::Direct => {
                self.inner.wal.as_ref().expect("direct mode has a wal").sync()?;
                Ok(())
            }
        };
        self.wait_workers();
        let _ = self.inner._lock.unlock(); // allow close→reopen on the same handle
        res
    }

    /// Wait until no background worker holds this `Inner` — a settle's
    /// seg-rename + wal-unlink racing a reopened `Db::open`'s scans can
    /// hide committed records (data in neither seen segs nor replayed wal).
    /// `imm` covers the freeze→spawn window (set, not yet worker-counted);
    /// a `bg_error` means the flush failed permanently — bail, don't hang.
    fn wait_workers(&self) {
        let mut w = self.inner.workers.0.lock();
        while *w > 0 || (self.inner.imm.load().is_some() && self.inner.bg_error.lock().is_none()) {
            self.inner.workers.1.wait(&mut w);
        }
    }

    pub fn stats(&self) -> Stats {
        let c = &self.inner.counters;
        Stats {
            puts: c.puts.load(Ordering::Relaxed),
            deletes: c.deletes.load(Ordering::Relaxed),
            gets: c.gets.load(Ordering::Relaxed),
            memtable_hits: c.memtable_hits.load(Ordering::Relaxed),
            segment_reads: c.segment_reads.load(Ordering::Relaxed),
            cache_hits: self.inner.cache.hits.load(Ordering::Relaxed),
            batches: c.batches.load(Ordering::Relaxed),
            flushes: c.flushes.load(Ordering::Relaxed),
            compactions: c.compactions.load(Ordering::Relaxed),
            live_segments: self.inner.segs.load().len(),
            memtable_bytes: self.inner.mem.read().bytes,
        }
    }

    /// Shared write path for put/delete/batch.
    fn apply(&self, ops: &[Op]) -> Result<()> {
        self.apply_durable(ops, false)
    }

    /// `durable` = strict per-call durability: after the normal write path,
    /// force a `Sync` barrier unless the policy ack already covered fsync.
    fn apply_durable(&self, ops: &[Op], durable: bool) -> Result<()> {
        self.check_open()?;
        self.validate_ops(ops)?;
        self.check_bg()?;

        if self.inner.cfg.write_path == WritePath::Pipeline {
            return self.apply_pipeline(ops, durable);
        }

        let need_ack = self.inner.cfg.sync == SyncPolicy::Always;
        let g = self.lock_write()?;
        let wal = self.inner.wal.as_ref().expect("direct mode has a wal");
        let append = wal.append(ops, need_ack)?;
        {
            let mut mem = self.inner.mem.write();
            let mut puts = 0u64;
            let mut deletes = 0u64;
            for op in ops {
                mem.apply(op);
                match op {
                    Op::Put(..) => puts += 1,
                    Op::Delete(..) => deletes += 1,
                }
            }
            self.inner.counters.puts.fetch_add(puts, Ordering::Relaxed);
            self.inner.counters.deletes.fetch_add(deletes, Ordering::Relaxed);
        }
        // Memtable full → rotate WAL + freeze. Inside `write_mu`: no writer
        // can be mid-append/apply, so every op logged to the old WAL file is
        // already in the frozen memtable.
        let frozen = if self.inner.mem.read().bytes > self.inner.cfg.memtable_bytes
            && self.inner.imm.load().is_none()
        {
            self.freeze()?
        } else {
            None
        };
        drop(g);
        if let Some((gen, imm)) = frozen {
            spawn_worker(&self.inner, "rexodb-flush", move |inner| {
                let _ = commit_flush(&inner, gen, &imm);
            });
        }
        // Wait for fsync outside the write mutex so other writers keep
        // coalescing into the same group commit.
        if let Err(e) = append.wait() {
            return Err(Error::Background(format!("wal sync failed: {e}")));
        }
        // `need_ack` already covered fsync under `Always`; weaker policies
        // need an explicit barrier for the strict guarantee.
        if durable && !need_ack {
            wal.sync()?;
        }
        Ok(())
    }

    /// Shared write path for `*_async` — `Direct` mode just applies
    /// normally (already at least as strong as the async contract).
    /// Returns the ring seq, `0` on `Direct`.
    fn apply_async(&self, ops: &[Op]) -> Result<u64> {
        if self.inner.cfg.write_path == WritePath::Direct {
            self.apply(ops)?;
            return Ok(0);
        }
        self.check_open()?;
        self.validate_ops(ops)?;
        self.check_bg()?;
        let p = self.pipeline()?;
        let seq = p.submit_async(ops)?;
        record_pipeline_ops(p.key(), seq, ops, p.applied());
        self.count_ops(ops);
        Ok(seq)
    }

    /// Read-your-writes fast path: this thread's own `*_async` ops not yet
    /// applied by the consumer. `None` = no pending write for the key —
    /// the normal path already shows everything ≤ `applied`.
    fn pending_own(&self, key: &[u8]) -> Option<Option<Bytes>> {
        if self.inner.cfg.write_path != WritePath::Pipeline {
            return None;
        }
        let p = self.inner.pipeline.get()?;
        pending_lookup(p.key(), p.applied(), key)
    }

    /// Pipeline mode: wait for this thread's own outstanding `*_async`
    /// submissions to be applied — for barriers (`range`, `flush`, `sync`)
    /// the overlay can't serve. No-op for `Direct` or threads that never
    /// wrote async.
    fn wait_own_writes(&self) -> Result<()> {
        if self.inner.cfg.write_path != WritePath::Pipeline {
            return Ok(());
        }
        let Some(p) = self.inner.pipeline.get() else { return Ok(()) };
        let Some(seq) = pending_tip(p.key()) else { return Ok(()) };
        p.wait_applied(seq)
    }

    fn validate_ops(&self, ops: &[Op]) -> Result<()> {
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    self.check_key(k)?;
                    if v.len() > self.inner.cfg.max_value_bytes {
                        return Err(Error::ValueTooLarge(v.len(), self.inner.cfg.max_value_bytes));
                    }
                }
                Op::Delete(k) => self.check_key(k)?,
            }
        }
        Ok(())
    }

    fn count_ops(&self, ops: &[Op]) {
        let mut puts = 0u64;
        let mut deletes = 0u64;
        for op in ops {
            match op {
                Op::Put(..) => puts += 1,
                Op::Delete(..) => deletes += 1,
            }
        }
        self.inner.counters.puts.fetch_add(puts, Ordering::Relaxed);
        self.inner.counters.deletes.fetch_add(deletes, Ordering::Relaxed);
    }

    /// Pipeline write path: publish `ops` to the ring and wait for the
    /// consumer to apply them (+ fsync for `Always` or `durable`).
    /// Counters bump after the submit succeeds, matching the direct path.
    fn apply_pipeline(&self, ops: &[Op], durable: bool) -> Result<()> {
        let need_durable = durable || self.inner.cfg.sync == SyncPolicy::Always;
        self.pipeline()?.submit(ops, need_durable)?;
        self.count_ops(ops);
        Ok(())
    }

    fn pipeline(&self) -> Result<&pipeline::Pipeline> {
        self.inner.pipeline.get().ok_or(Error::Closed)
    }

    /// Rotates the WAL and freezes the current memtable into `imm`. Caller
    /// must hold `write_mu`. Returns the replaced WAL's gen + the frozen
    /// table, or `None` when the memtable was empty. `Direct` path only —
    /// `Pipeline` freezes on its consumer.
    fn freeze(&self) -> Result<Option<(u64, Arc<Memtable>)>> {
        if self.inner.mem.read().is_empty() {
            return Ok(None);
        }
        let gen = self.inner.wal_gen.load(Ordering::SeqCst);
        let new_gen = self.inner.next_file.fetch_add(1, Ordering::SeqCst);
        let wal_path = self.inner.wal_path(new_gen);
        self.inner.wal.as_ref().expect("direct mode has a wal").rotate(&wal_path)?;
        self.inner.wal_gen.store(new_gen, Ordering::SeqCst);
        let old = Arc::new(std::mem::take(&mut *self.inner.mem.write()));
        self.inner.imm.store(Arc::new(Some(old.clone())));
        crash::point(crash::FREEZE_SWAPPED); // imm published; flush not yet spawned
        Ok(Some((gen, old)))
    }

    fn lock_write(&self) -> Result<parking_lot::MutexGuard<'_, ()>> {
        self.check_open()?;
        self.check_bg()?;
        if self.inner.wal.as_ref().is_some_and(|w| w.io_failed()) {
            return Err(Error::Background("wal writer failed (disk error)".into()));
        }
        Ok(self.inner.write_mu.lock())
    }

    fn check_open(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        Ok(())
    }

    fn check_bg(&self) -> Result<()> {
        match &*self.inner.bg_error.lock() {
            Some(m) => Err(Error::Background(m.clone())),
            None => Ok(()),
        }
    }

    fn check_key(&self, key: &[u8]) -> Result<()> {
        if key.len() > self.inner.cfg.max_key_bytes {
            return Err(Error::KeyTooLarge(key.len(), self.inner.cfg.max_key_bytes));
        }
        Ok(())
    }
}

/// Writes `mem` to a segment, commits it (assigning a fresh gen via atomic
/// rename so filename order == commit order), registers it as newest, and
/// unlinks the WAL file it replaced. On failure the caller's `bg_error` is
/// set and the WAL file is kept so restart recovers the data.
fn commit_flush(inner: &Arc<Inner>, wal_gen: u64, mem: &Arc<Memtable>) -> Result<()> {
    let seg_dir = inner.cfg.path.join("seg");
    let mut next = || inner.next_file.fetch_add(1, Ordering::SeqCst);
    let res = write_segment(&inner.cfg, &seg_dir, &mut next, mem_entries(mem));
    match res {
        Ok(reader) => {
            if let Some(r) = reader {
                crash::point(crash::SEG_RENAMED); // committed on disk; list update pending
                let r = Arc::new(r);
                inner.segs.rcu(|segs| {
                    let mut next = segs.as_ref().clone();
                    next.insert(0, r.clone());
                    next
                });
            }
            crash::point(crash::FLUSH_PUBLISHED); // readers see it; wal unlink pending
            let _ = fs::remove_file(inner.wal_path(wal_gen));
            inner.counters.flushes.fetch_add(1, Ordering::Relaxed);
            inner.imm.store(Arc::new(None));
            maybe_compact(inner);
            Ok(())
        }
        Err(e) => {
            // Keep `imm` so reads still see the data; WAL file kept so a
            // restart retries the flush from the log.
            *inner.bg_error.lock() = Some(e.to_string());
            Err(e)
        }
    }
}

/// Merge all live segments into one. Tombstones are dropped — a full merge
/// leaves no older layer they could still shadow.
fn compact_all(inner: &Arc<Inner>) -> Result<()> {
    let inputs = inner.segs.load().as_ref().clone();
    if inputs.len() < 2 {
        return Ok(());
    }
    if inner.compacting.swap(true, Ordering::SeqCst) {
        return Ok(()); // another compaction already running
    }
    let res = merge_segments(inner, inputs, true);
    inner.compacting.store(false, Ordering::SeqCst);
    res
}

/// Merge `inputs` (a contiguous newest-first window of `segs`) into one
/// segment written back at the window's slot. `drop_tombstones` may only be
/// true when the window includes the oldest live segment.
fn merge_segments(
    inner: &Arc<Inner>,
    inputs: Vec<Arc<segment::Reader>>,
    drop_tombstones: bool,
) -> Result<()> {
    let mut sources: Vec<merge::Source> = Vec::with_capacity(inputs.len());
    for r in &inputs {
        sources.push(merge::Source::Seg(r.iter()?));
    }
    let mut mi = merge::MergeIter::new(sources)?;
    let tmp = inner
        .cfg
        .path
        .join("seg")
        .join(format!("{:016}.tmp", inner.next_file.fetch_add(1, Ordering::SeqCst)));
    let mut b = segment::Builder::create(&tmp, inner.cfg.index_every)?;
    while let Some((k, v)) = mi.next_entry()? {
        if v.is_some() || !drop_tombstones {
            b.add(k.as_ref(), v.as_deref())?;
        }
    }
    let (count, _) = b.finish()?;
    crash::point(crash::MERGE_TMP); // merged tmp fsynced; rename pending
                                    // The merged run takes the OLDEST input's slot, so it reuses that
                                    // input's gen — a fresh gen would sort the output *newest* at reopen
                                    // (filename order = recency) and resurrect data shadowed by younger
                                    // segs that committed during the merge. Renaming over the input's path
                                    // is the atomic commit; the other inputs are unlinked after.
    let merged = if count == 0 {
        let _ = fs::remove_file(&tmp);
        None
    } else {
        let path = inputs.last().expect("non-empty inputs").path.clone();
        fs::rename(&tmp, &path)?;
        sync_parent(&path)?;
        crash::point(crash::MERGE_RENAMED); // merged file live; inputs pending unlink
        Some(Arc::new(segment::Reader::open(&path)?))
    };
    inner.segs.rcu(|segs| {
        // The merged run takes the inputs' window slot — older than every
        // seg committed since the snapshot (prepended in front), newer than
        // every seg the window sat above.
        let mut next = Vec::with_capacity(segs.len());
        let mut placed = false;
        for r in segs.iter() {
            if inputs.iter().any(|i| Arc::ptr_eq(i, r)) {
                if !placed {
                    if let Some(m) = &merged {
                        next.push(m.clone());
                    }
                    placed = true;
                }
            } else {
                next.push(r.clone());
            }
        }
        next
    });
    // The oldest input's path now holds the merged file — unlink the rest.
    let keep = merged.is_some() as usize;
    for r in &inputs[..inputs.len() - keep] {
        let _ = fs::remove_file(&r.path);
    }
    inner.counters.compactions.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn maybe_compact(inner: &Arc<Inner>) {
    if inner.segs.load().len() <= inner.cfg.compact_at {
        return;
    }
    spawn_worker(inner, "rexodb-compact", |inner| {
        let _ = if inner.cfg.full_merge { compact_all(&inner) } else { compact_some(&inner) };
    });
}

/// Incremental compaction: merge the single adjacent pair of segments with
/// the smallest combined on-disk size. Unlike `compact_all` the pause is
/// bounded by the pair — the big tail is only rewritten once a similar
/// sized neighbour exists, so merges self-organize into size tiers.
fn compact_some(inner: &Arc<Inner>) -> Result<()> {
    let segs = inner.segs.load().as_ref().clone();
    if segs.len() < 2 {
        return Ok(());
    }
    if inner.compacting.swap(true, Ordering::SeqCst) {
        return Ok(()); // another compaction already running
    }
    let size = |r: &Arc<segment::Reader>| fs::metadata(&r.path).map(|m| m.len()).unwrap_or(u64::MAX);
    let mut best = 0usize;
    let mut best_size = u64::MAX;
    for i in 0..segs.len() - 1 {
        let s = size(&segs[i]).saturating_add(size(&segs[i + 1]));
        if s <= best_size {
            best = i;
            best_size = s;
        }
    }
    let inputs = vec![segs[best].clone(), segs[best + 1].clone()];
    // Tombstones may only be dropped when the merge includes the oldest
    // live segment — otherwise they must be written through to shadow the
    // data below.
    let res = merge_segments(inner, inputs, best + 1 == segs.len() - 1);
    inner.compacting.store(false, Ordering::SeqCst);
    res
}

/// Runs `f` on a named thread counted in `inner.workers` — `Db::close`
/// drains the count so no background file op outlives the `Db` handle.
fn spawn_worker<F>(inner: &Arc<Inner>, name: &str, f: F)
where
    F: FnOnce(Arc<Inner>) + Send + Clone + 'static,
{
    *inner.workers.0.lock() += 1;
    let inner2 = inner.clone();
    let f2 = f.clone();
    if thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            f2(inner2.clone());
            *inner2.workers.0.lock() -= 1;
            inner2.workers.1.notify_all();
        })
        .is_err()
    {
        // Can't offload — run inline so the work is never skipped.
        *inner.workers.0.lock() -= 1;
        inner.workers.1.notify_all();
        f(inner.clone());
    }
}

/// Streams `mem`'s entries as owned pairs — `Bytes` clones are refcount bumps.
fn mem_entries(mem: &Memtable) -> impl Iterator<Item = (Bytes, Option<Bytes>)> + '_ {
    mem.iter().map(|(k, v)| (k.clone(), v.clone()))
}

/// Writes sorted `entries` to a `.tmp` file and atomically renames it into
/// place with the next generation — the rename is the commit point, and the
/// gen assigned at commit keeps filename order == recency order.
/// Returns `None` when there was nothing to write.
fn write_segment(
    cfg: &Config,
    seg_dir: &Path,
    next_file: &mut dyn FnMut() -> u64,
    entries: impl Iterator<Item = (Bytes, Option<Bytes>)>,
) -> Result<Option<segment::Reader>> {
    let tmp = seg_dir.join(format!("{:016}.tmp", next_file()));
    let mut b = segment::Builder::create(&tmp, cfg.index_every)?;
    for (k, v) in entries {
        b.add(k.as_ref(), v.as_deref())?;
    }
    let (count, _) = b.finish()?;
    if count == 0 {
        let _ = fs::remove_file(&tmp);
        return Ok(None);
    }
    let path = seg_dir.join(format!("{:016}.sst", next_file()));
    crash::point(crash::SEG_TMP); // segment fsynced; rename is the commit
    fs::rename(&tmp, &path)?;
    sync_parent(&path)?;
    Ok(Some(segment::Reader::open(&path)?))
}

/// fsyncs `path`'s parent dir — `sync_all` on a file covers its contents
/// but not the directory entry, so a crash can still lose the new name
/// after a create/rename. No-op off Unix.
fn sync_parent(path: &Path) -> io::Result<()> {
    if cfg!(unix) {
        if let Some(dir) = path.parent() {
            fs::File::open(dir)?.sync_all()?;
        }
    }
    Ok(())
}

/// `(file_number, path)` sorted ascending for every `*.ext` file in `dir`.
fn numbered_files(dir: &Path, ext: &str) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for e in fs::read_dir(dir)? {
        let p = e?.path();
        if p.extension().is_some_and(|x| x == ext) {
            if let Some(n) = p.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse::<u64>().ok()) {
                out.push((n, p));
            }
        }
    }
    out.sort_by_key(|(n, _)| *n);
    Ok(out)
}

fn filter_mem<'a>(
    mem: &Memtable,
    pending: Vec<(usize, &'a [u8])>,
    out: &mut [Option<Bytes>],
    hits: &mut u64,
) -> Vec<(usize, &'a [u8])> {
    let mut still = Vec::with_capacity(pending.len());
    for (i, k) in pending {
        match mem.lookup(k) {
            Some(v) => {
                *hits += 1;
                out[i] = v;
            }
            None => still.push((i, k)),
        }
    }
    still
}

fn bound_ref(b: &Bound<Bytes>) -> Bound<&[u8]> {
    b.as_ref().map(|k| k.as_ref())
}

/// `true` when `start`/`end` can't contain any key — mirrors what
/// `BTreeMap::range` would panic on.
fn range_empty(start: &Bound<Bytes>, end: &Bound<Bytes>) -> bool {
    match (start, end) {
        (Bound::Included(s), Bound::Included(e)) => s > e,
        (Bound::Included(s), Bound::Excluded(e))
        | (Bound::Excluded(s), Bound::Included(e))
        | (Bound::Excluded(s), Bound::Excluded(e)) => s >= e,
        _ => false,
    }
}

/// Ordered range iterator — owns all its sources (`Bytes` snapshot refs +
/// dup'd segment fds), so it outlives flushes and compactions mid-scan.
pub struct RangeIter {
    merge: merge::MergeIter,
    start: Bound<Bytes>,
    end: Bound<Bytes>,
}

impl Iterator for RangeIter {
    type Item = Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.merge.next_entry() {
                Err(e) => return Some(Err(e)),
                Ok(None) => return None,
                Ok(Some((k, v))) => {
                    // Segments' iter_from only lands near the start key —
                    // filter the real bound here (also handles `Excluded`).
                    let below = match &self.start {
                        Bound::Included(s) => k.as_ref() < s.as_ref(),
                        Bound::Excluded(s) => k.as_ref() <= s.as_ref(),
                        Bound::Unbounded => false,
                    };
                    if below {
                        continue;
                    }
                    let past = match &self.end {
                        Bound::Included(e) => k.as_ref() > e.as_ref(),
                        Bound::Excluded(e) => k.as_ref() >= e.as_ref(),
                        Bound::Unbounded => false,
                    };
                    if past {
                        return None;
                    }
                    if let Some(val) = v {
                        return Some(Ok((k, val)));
                    }
                    // tombstone — suppressed, but it already shadowed older
                    // copies inside the merge
                }
            }
        }
    }
}
