//! Disruptor-style write pipeline (`WritePath::Pipeline`).
//!
//! Producers encode+frame their WAL record, claim a ring slot with one
//! `fetch_add`, publish, and unpark the consumer. The consumer drains slots
//! *in claim order*: one `write` per batch, one memtable write-lock per
//! batch. Claim order = WAL order = memtable-apply order, so the
//! commit-order invariant holds without a global write mutex — and WAL
//! rotation happens on the consumer instead of a random producer.
//!
//! fsync never runs on the consumer's critical path: a second thread owns
//! durability. The consumer flags how far it has written (`need`); the
//! fsync thread paces `sync_all` per [`SyncPolicy`], fsyncs rotated-out
//! files, and resolves `Sync`/`Close` barrier acks. A `put` returns once
//! its slot is *applied* (visible to readers); with `SyncPolicy::Always`
//! it additionally waits for `durable` — every writer covered by one fsync
//! shares it, the same group commit as `Direct`.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use rexodb_wal::{self as wal, Op};

use crate::memtable::Memtable;
use crate::{commit_flush, Error, Inner, Result};

enum Event {
    Ops {
        frame: Vec<u8>,
        ops: Vec<Op>,
    },
    /// fsync barrier: durable-cover everything before this slot, then ack.
    Sync(SyncSender<io::Result<()>>),
    /// Freeze the memtable + rotate the WAL file; ack gets the pieces the
    /// caller needs to finish the flush. `None` = memtable was empty.
    Flush(SyncSender<io::Result<Option<Frozen>>>),
    /// Same barrier as `Sync`, then the consumer exits.
    Close(SyncSender<io::Result<()>>),
}

struct Slot {
    /// == this slot's seq once `event` is filled (release/acquire pair with
    /// the consumer's seq wait — no separate ready flag needed).
    stamp: AtomicU64,
    event: Mutex<SlotBody>,
}

struct SlotBody {
    ev: Option<Event>,
    /// Spare capacity for the next submit claiming this slot — the consumer
    /// refills both in `finish`, after apply. A producer may legally claim
    /// the slot before that refill lands (it only needs `consumed`), in
    /// which case it just gets an empty Vec and re-grows — the consumer's
    /// refill still arrives via `recycle` and restores the capacity.
    ops: Vec<Op>,
    frame: Vec<u8>,
}

/// Keeps one hot atomic on its own cache line — `next_claim` (all producers),
/// `applied`/`need` (consumer), `durable` (fsync thread) are written on
/// different cores and would otherwise ping-pong the same line.
#[repr(align(128))]
struct Padded<T>(T);

impl<T> std::ops::Deref for Padded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

struct WalState {
    /// The file currently receiving appends.
    file: Arc<File>,
    /// Rotated-out files with their covered-seq boundary, fsync'd in order.
    rotations: VecDeque<(u64, Arc<File>)>,
}

/// Everything shared between producers, the consumer, and the fsync thread.
struct Shared {
    slots: Box<[Slot]>,
    mask: usize,
    /// Hands out seqs; the next claim is `fetch_add(1) + 1` (seqs are 1-based
    /// so `applied`/`durable`/`need` can start at 0).
    next_claim: Padded<AtomicU64>,
    /// Highest seq whose ops are in the memtable (and WAL-written).
    applied: Padded<AtomicU64>,
    /// Highest seq whose slot the consumer has *taken* — the slot-reuse
    /// frontier for producer backpressure. Strictly behind `applied`'s
    /// publish point but, crucially, published per-slot inside `drain`
    /// rather than in `finish`: a producer waiting on `applied` before
    /// stamping would deadlock — the consumer needs the stamp to reach
    /// `finish` in the first place.
    consumed: Padded<AtomicU64>,
    /// Highest seq the consumer has written to the WAL file — the fsync
    /// thread's work frontier.
    need: Padded<AtomicU64>,
    /// Highest seq known fsync'd (rotation boundaries included).
    durable: Padded<AtomicU64>,
    /// Active WAL file + rotated-out files awaiting fsync, under one lock:
    /// the consumer pushes a rotation and swaps the live file atomically,
    /// so the fsync thread can never credit a seq boundary to the wrong
    /// file (a `durable` claim without the old file's fsync would lie).
    wal_state: Mutex<WalState>,
    /// `Sync`/`Close` barriers (seq, ack) resolved once durable reaches them.
    sync_acks: Mutex<VecDeque<(u64, SyncSender<io::Result<()>>)>>,
    /// Thread handles for cheap unparks — each set once at start.
    consumer: OnceLock<thread::Thread>,
    fsyncer: OnceLock<thread::Thread>,
    wait_mu: Mutex<()>,
    wait_cv: Condvar,
    /// Producers blocked on `wait_cv` — `publish` skips the lock+notify
    /// when nobody is listening (the common case under `*_async`).
    waiters: Padded<AtomicU64>,
    /// True while the consumer is (about to be) parked — producers only
    /// `unpark` then; its claim poll precedes every park, so a seq can't
    /// be missed.
    consumer_idle: Padded<AtomicBool>,
    /// First I/O failure — every later op/barrier gets it instead of
    /// hanging on progress that will never come. `io_failed` is the cheap
    /// check; the mutex is only taken once it's set.
    io_failed: AtomicBool,
    failed: Mutex<Option<io::Error>>,
    /// False once the consumer thread exits — waiters bail out instead of
    /// hanging.
    alive: AtomicBool,
    /// Set by `Pipeline::drop` — both threads exit once queues drain.
    closing: AtomicBool,
}

/// Frozen memtable + the WAL gen it replaces — handed to a flush so the
/// caller can commit it as a segment. The old WAL file's fsync is the
/// fsync thread's job, not the flush's.
pub struct Frozen {
    wal_gen: u64,
    mem: Arc<Memtable>,
}

fn err_copy(e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), e.to_string())
}

/// Best-effort CPU affinity — a hard pin on Linux, a scheduling hint on
/// macOS/Windows. Failures (restricted cpuset, bad id) are ignored: the
/// feature is opt-in tuning, never a correctness requirement.
fn pin(core: Option<usize>) {
    if let Some(id) = core {
        core_affinity::set_for_current(core_affinity::CoreId { id });
    }
}

impl Shared {
    fn notify_waiters(&self) {
        if self.waiters.load(Ordering::Acquire) == 0 {
            return;
        }
        let _g = self.wait_mu.lock();
        self.wait_cv.notify_all();
    }

    /// Watermark publish — `fetch_max` keeps it monotonic across the MAX
    /// publish `fail` uses to release waiters.
    fn publish(&self, atomic: &AtomicU64, seq: u64) {
        atomic.fetch_max(seq, Ordering::AcqRel);
        self.notify_waiters();
    }

    fn unpark_fsyncer(&self) {
        if let Some(t) = self.fsyncer.get() {
            t.unpark();
        }
    }

    /// Record the first I/O failure, fail every pending barrier, and wake
    /// all waiters (MAX publish makes their seq checks pass so they take
    /// the failure branch).
    fn fail(&self, e: io::Error) {
        {
            let mut f = self.failed.lock();
            if f.is_none() {
                *f = Some(io::Error::new(e.kind(), e.to_string()));
            }
        }
        self.io_failed.store(true, Ordering::Release);
        let mut acks = self.sync_acks.lock();
        while let Some((_, a)) = acks.pop_front() {
            let _ = a.send(Err(err_copy(&e)));
        }
        drop(acks);
        self.applied.store(u64::MAX, Ordering::Release);
        self.durable.store(u64::MAX, Ordering::Release);
        self.notify_waiters();
        self.unpark_fsyncer();
    }

    fn io_failure(&self) -> Option<io::Error> {
        if !self.io_failed.load(Ordering::Acquire) {
            return None;
        }
        self.failed.lock().as_ref().map(err_copy)
    }

    fn failure(&self) -> Option<Error> {
        if !self.io_failed.load(Ordering::Acquire) {
            return None;
        }
        self.failed.lock().as_ref().map(|e| Error::Background(format!("wal writer failed: {e}")))
    }

    fn stopped(&self) -> Error {
        Error::Background("write pipeline stopped".into())
    }

    /// Wait for `atomic` to reach `seq`. Errors once the pipeline has failed
    /// or the consumer is gone — even when the seq itself looks satisfied
    /// (a MAX publish marks failure, not progress).
    fn wait(&self, atomic: &AtomicU64, seq: u64) -> Result<()> {
        for _ in 0..4096 {
            if atomic.load(Ordering::Acquire) >= seq {
                return self.status();
            }
            std::hint::spin_loop();
        }
        // Second gear: yield a bounded window — the consumer's drain is
        // ~µs, so most sync writes land here instead of paying a futex
        // sleep + notify round-trip.
        for _ in 0..256 {
            if atomic.load(Ordering::Acquire) >= seq {
                return self.status();
            }
            std::thread::yield_now();
        }
        self.waiters.fetch_add(1, Ordering::AcqRel);
        let result = {
            let mut g = self.wait_mu.lock();
            loop {
                if atomic.load(Ordering::Acquire) >= seq {
                    break self.status();
                }
                if let Some(e) = self.failure() {
                    break Err(e);
                }
                if !self.alive.load(Ordering::Acquire) {
                    break Err(self.stopped());
                }
                let _ = self.wait_cv.wait_for(&mut g, Duration::from_millis(10));
            }
        };
        self.waiters.fetch_sub(1, Ordering::AcqRel);
        result
    }

    fn status(&self) -> Result<()> {
        match self.failure() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Producer-side handle. All shared state is in `Arc<Shared>`.
pub struct Pipeline {
    shared: Arc<Shared>,
}

impl Pipeline {
    /// Opens the WAL file and spawns the consumer + fsync threads. `inner`
    /// is held weakly — the consumer exits once the `Db` is dropped rather
    /// than keeping it alive through a reference cycle.
    pub fn start(inner: &Arc<Inner>, wal_path: &std::path::Path) -> io::Result<Self> {
        let fw = wal::FileWriter::open(wal_path)?;
        let cap = inner.cfg.ring_capacity.next_power_of_two().max(16);
        let mut slots = Vec::with_capacity(cap);
        for _ in 0..cap {
            slots.push(Slot {
                stamp: AtomicU64::new(0),
                event: Mutex::new(SlotBody { ev: None, ops: Vec::new(), frame: Vec::new() }),
            });
        }
        let shared = Arc::new(Shared {
            slots: slots.into_boxed_slice(),
            mask: cap - 1,
            next_claim: Padded(AtomicU64::new(0)),
            applied: Padded(AtomicU64::new(0)),
            consumed: Padded(AtomicU64::new(0)),
            need: Padded(AtomicU64::new(0)),
            durable: Padded(AtomicU64::new(0)),
            wal_state: Mutex::new(WalState { file: fw.file().clone(), rotations: VecDeque::new() }),
            sync_acks: Mutex::new(VecDeque::new()),
            consumer: OnceLock::new(),
            fsyncer: OnceLock::new(),
            wait_mu: Mutex::new(()),
            wait_cv: Condvar::new(),
            waiters: Padded(AtomicU64::new(0)),
            consumer_idle: Padded(AtomicBool::new(false)),
            io_failed: AtomicBool::new(false),
            failed: Mutex::new(None),
            alive: AtomicBool::new(true),
            closing: AtomicBool::new(false),
        });
        let s2 = shared.clone();
        let weak = Arc::downgrade(inner);
        let consumer_core = inner.cfg.pin_cores.first().copied();
        thread::Builder::new()
            .name("rexodb-pipeline".into())
            .spawn(move || {
                pin(consumer_core);
                run(s2, weak, fw)
            })
            .map_err(|e| io::Error::other(format!("failed to spawn pipeline consumer: {e}")))?;
        let s3 = shared.clone();
        let policy = inner.cfg.sync;
        let fsync_core = inner.cfg.pin_cores.get(1).copied();
        thread::Builder::new()
            .name("rexodb-fsync".into())
            .spawn(move || {
                pin(fsync_core);
                fsync_loop(s3, policy)
            })
            .map_err(|e| io::Error::other(format!("failed to spawn fsync thread: {e}")))?;
        Ok(Self { shared })
    }

    /// Claim a seq. Blocks (backpressure) when the producer laps the ring by
    /// a full capacity. A claimed seq MUST be stamped — a gap makes the
    /// consumer spin forever in `wait_stamp`, so on a wrap-wait error we
    /// sacrifice the slot (empty ops the consumer skips) rather than
    /// abandon it.
    fn claim(&self) -> Result<u64> {
        if let Some(e) = self.shared.failure() {
            return Err(e);
        }
        if !self.shared.alive.load(Ordering::Acquire) {
            return Err(self.shared.stopped());
        }
        let seq = self.shared.next_claim.fetch_add(1, Ordering::AcqRel) + 1;
        let cap = self.shared.slots.len() as u64;
        if seq > cap {
            // Wait for the consumer to have taken seq - cap (slot free),
            // NOT applied: a claimed-but-unstamped seq blocks the consumer
            // before `finish`, so `applied` may never reach the target —
            // circular wait. `consumed` is published per slot in `drain`.
            if let Err(e) = self.shared.wait(&self.shared.consumed, seq - cap + 1) {
                self.stamp_event(seq, Event::Ops { frame: Vec::new(), ops: Vec::new() });
                return Err(e);
            }
        }
        Ok(seq)
    }

    fn stamp_event(&self, seq: u64, ev: Event) {
        let slot = &self.shared.slots[(seq as usize) & self.shared.mask];
        slot.event.lock().ev = Some(ev);
        self.publish_stamp(slot, seq);
    }

    fn publish_stamp(&self, slot: &Slot, seq: u64) {
        slot.stamp.store(seq, Ordering::Release);
        if self.shared.consumer_idle.load(Ordering::Acquire) {
            if let Some(t) = self.shared.consumer.get() {
                t.unpark();
            }
        }
    }

    /// Claim a slot and publish `ops` at it, filling the frame + ops into
    /// the slot's spare buffers (zero steady-state allocations).
    fn send_ops(&self, ops: &[Op]) -> Result<u64> {
        let seq = self.claim()?;
        let slot = &self.shared.slots[(seq as usize) & self.shared.mask];
        {
            let mut body = slot.event.lock();
            let mut ops_buf = std::mem::take(&mut body.ops);
            let mut frame_buf = std::mem::take(&mut body.frame);
            ops_buf.clear();
            frame_buf.clear();
            ops_buf.extend_from_slice(ops);
            wal::encode_record_into(ops, &mut frame_buf);
            body.ev = Some(Event::Ops { frame: frame_buf, ops: ops_buf });
        }
        self.publish_stamp(slot, seq);
        Ok(seq)
    }

    /// One atomic record (a batch's worth of ops). Returns once applied to
    /// the memtable; `need_durable` additionally waits for the fsync
    /// covering it — a barrier ack forces the fsync under any policy.
    pub fn submit(&self, ops: &[Op], need_durable: bool) -> Result<()> {
        let seq = self.send_ops(ops)?;
        self.shared.wait(&self.shared.applied, seq)?;
        if need_durable {
            let (tx, rx) = mpsc::sync_channel(1);
            self.shared.sync_acks.lock().push_back((seq, tx));
            self.shared.unpark_fsyncer();
            ack(rx)?;
        }
        Ok(())
    }

    /// Fire-and-forget record: claim + publish, return the seq without
    /// waiting. The caller is responsible for read-your-writes tracking —
    /// see `Db::put_async`.
    pub fn submit_async(&self, ops: &[Op]) -> Result<u64> {
        self.send_ops(ops)
    }

    /// Identity of this pipeline instance for thread-local seq tracking —
    /// the `Shared` allocation address, unique while it lives.
    pub fn key(&self) -> usize {
        Arc::as_ptr(&self.shared) as usize
    }

    /// Current apply watermark — highest seq whose ops are in the memtable.
    pub fn applied(&self) -> u64 {
        self.shared.applied.load(Ordering::Acquire)
    }

    /// Wait until `seq` is applied, clamped to this instance's claim tip —
    /// safe even if `seq` was claimed on a different pipeline instance
    /// (a stale TLS entry at a recycled address can't wait past the tip).
    pub fn wait_applied(&self, seq: u64) -> Result<()> {
        let tip = self.shared.next_claim.load(Ordering::Acquire);
        self.shared.wait(&self.shared.applied, seq.min(tip))
    }

    /// fsync barrier: everything claimed before this call's slot is durable
    /// when it returns.
    pub fn sync(&self) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        let seq = self.claim()?;
        self.stamp_event(seq, Event::Sync(tx));
        ack(rx)
    }

    /// Freeze the memtable + rotate the WAL at this queue position. The
    /// caller finishes the flush via [`settle_frozen`].
    pub fn freeze(&self) -> Result<Option<Frozen>> {
        let (tx, rx) = mpsc::sync_channel(1);
        let seq = self.claim()?;
        self.stamp_event(seq, Event::Flush(tx));
        rx.recv().map_err(|_| Error::Background("write pipeline stopped".into()))?.map_err(Error::Io)
    }

    /// fsync everything queued, then stop the consumer.
    pub fn close(&self) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        match self.claim() {
            Ok(seq) => {
                self.stamp_event(seq, Event::Close(tx));
                ack(rx)
            }
            // Already dead/failed — close still reports the failure honestly.
            Err(e) => Err(e),
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        self.shared.closing.store(true, Ordering::Release);
        if let Some(t) = self.shared.consumer.get() {
            t.unpark();
        }
        self.shared.unpark_fsyncer();
    }
}

fn ack(rx: Receiver<io::Result<()>>) -> Result<()> {
    rx.recv().map_err(|_| Error::Background("write pipeline stopped".into()))?.map_err(Error::Io)
}

/// Commit the frozen memtable as a segment — on the caller's thread for
/// `Db::flush`, on a spawned `rexodb-flush` thread for auto-rotation. The
/// rotated WAL's fsync is already handled by the fsync thread.
pub fn settle_frozen(inner: Arc<Inner>, f: Arc<Frozen>) -> Result<()> {
    commit_flush(&inner, f.wal_gen, &f.mem)
}

/// Sets `alive = false` and wakes waiters on consumer exit — including
/// unwinding from a panic — so producers error out instead of hanging.
struct AliveGuard(Arc<Shared>);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.alive.store(false, Ordering::Release);
        self.0.notify_waiters();
        self.0.unpark_fsyncer();
    }
}

struct Consumer {
    shared: Arc<Shared>,
    fw: wal::FileWriter,
    /// Next seq to process.
    next: u64,
    /// (slot idx, ops) taken this drain — applied to the memtable in one
    /// go, then the Vec goes back to its slot as the next submit's buffer.
    pending: Vec<(usize, Vec<Op>)>,
}

impl Consumer {
    /// Process every published slot up to `claim`. Returns false on `Close`.
    fn drain(&mut self, inner: &Arc<Inner>, claim: u64) -> bool {
        let mut batch = 0usize;
        let mut bytes = 0usize;
        while self.next <= claim && batch < wal::MAX_BATCH && bytes < wal::MAX_BATCH_BYTES {
            let seq = self.next;
            let slot = &self.shared.slots[(seq as usize) & self.shared.mask];
            if !wait_stamp(&self.shared, slot, seq) {
                return false;
            }
            let ev = slot.event.lock().ev.take().expect("stamped slot has an event");
            match ev {
                Event::Ops { frame, ops } => {
                    if self.shared.failed.lock().is_none() {
                        self.fw.queue(&frame);
                        self.pending.push(((seq as usize) & self.shared.mask, ops));
                        bytes += frame.len();
                        batch += 1;
                        // `queue` already copied the frame into the
                        // writer's batch buffer — recycle its capacity.
                        Self::recycle(slot, Vec::new(), frame);
                    } else {
                        Self::recycle(slot, ops, frame);
                    }
                }
                Event::Sync(ack) => {
                    self.finish(inner, seq - 1);
                    self.barrier(seq, ack);
                    self.shared.publish(&self.shared.applied, seq);
                }
                Event::Flush(ack) => {
                    self.finish(inner, seq - 1);
                    let res = self.freeze(inner);
                    if let Err(e) = &res {
                        self.shared.fail(err_copy(e));
                    }
                    self.shared.publish(&self.shared.applied, seq);
                    let _ = ack.send(res);
                }
                Event::Close(ack) => {
                    self.finish(inner, seq - 1);
                    self.barrier(seq, ack);
                    self.shared.publish(&self.shared.applied, seq);
                    return false;
                }
            }
            self.next += 1;
            // Slot is now reclaimable — wakes producers waiting in `claim`.
            // Per slot, not per batch: waiting on `applied`/`finish` would
            // circular-block against a producer holding this slot's seq.
            self.shared.publish(&self.shared.consumed, seq);
        }
        self.finish(inner, self.next - 1);
        self.maybe_rotate(inner);
        true
    }

    /// Resolve a `Sync`/`Close` barrier at `seq`: flag it as needing
    /// durability and hand the ack to the fsync thread.
    fn barrier(&self, seq: u64, ack: SyncSender<io::Result<()>>) {
        match self.shared.io_failure() {
            Some(e) => {
                let _ = ack.send(Err(e));
            }
            None => {
                self.shared.need.fetch_max(seq, Ordering::AcqRel);
                self.shared.sync_acks.lock().push_back((seq, ack));
                self.shared.unpark_fsyncer();
            }
        }
    }

    /// Returns spare buffers to `slot` — keeps whichever side has more
    /// capacity so a dead event's empty Vecs never shrink the pool.
    fn recycle(slot: &Slot, mut ops: Vec<Op>, mut frame: Vec<u8>) {
        ops.clear();
        frame.clear();
        let mut b = slot.event.lock();
        if ops.capacity() > b.ops.capacity() {
            b.ops = ops;
        }
        if frame.capacity() > b.frame.capacity() {
            b.frame = frame;
        }
    }

    /// Write the buffered frames, flag the write frontier for the fsync
    /// thread, apply pending ops to the memtable, publish `applied = upto`.
    /// WAL-write before memtable apply keeps the commit order; slot buffers
    /// are recycled before the `applied` publish — a producer's wrap-wait
    /// guarantees it can't claim the slot earlier.
    fn finish(&mut self, inner: &Arc<Inner>, upto: u64) {
        if self.shared.failed.lock().is_none() {
            match self.fw.flush() {
                Ok(()) => {
                    if upto > 0 {
                        self.shared.need.fetch_max(upto, Ordering::AcqRel);
                        self.shared.unpark_fsyncer();
                    }
                    let mut mem = inner.mem.write();
                    for (idx, ops) in self.pending.drain(..) {
                        for op in &ops {
                            mem.apply(op);
                        }
                        Self::recycle(&self.shared.slots[idx], ops, Vec::new());
                    }
                }
                Err(e) => self.shared.fail(e),
            }
        }
        self.pending.clear();
        self.shared.publish(&self.shared.applied, upto);
    }

    /// Memtable budget exceeded and no flush in flight → rotate the WAL and
    /// hand the frozen table to a flush thread.
    fn maybe_rotate(&mut self, inner: &Arc<Inner>) {
        if inner.mem.read().bytes <= inner.cfg.memtable_bytes || inner.imm.load().is_some() {
            return;
        }
        match self.freeze(inner) {
            Ok(Some(f)) => {
                let f = Arc::new(f);
                crate::spawn_worker(inner, "rexodb-flush", move |inner| {
                    let _ = settle_frozen(inner, f);
                });
            }
            Ok(None) => {}
            Err(e) => self.shared.fail(e),
        }
    }

    /// Rotate the WAL and freeze the memtable into `imm`. Consumer-side
    /// analogue of `Db::freeze` — no write mutex needed since the consumer
    /// is the only writer and processes seqs in order. The old file's fsync
    /// is queued for the fsync thread, covering all seqs written so far.
    fn freeze(&mut self, inner: &Arc<Inner>) -> io::Result<Option<Frozen>> {
        if inner.mem.read().is_empty() {
            return Ok(None);
        }
        if let Some(e) = self.shared.io_failure() {
            return Err(e);
        }
        // A flush is already in flight — wait it out like `Db::flush` does,
        // then fold everything since into a second frozen table.
        while inner.imm.load().is_some() {
            if inner.bg_error.lock().is_some() {
                return Err(io::Error::other("background flush failed"));
            }
            thread::sleep(Duration::from_millis(1));
        }
        let old_gen = inner.wal_gen.load(Ordering::SeqCst);
        let new_gen = inner.next_file.fetch_add(1, Ordering::SeqCst);
        self.fw.flush()?; // ensure everything before the barrier is in the old file
        let covered = self.next - 1;
        let old_file = self.fw.rotate(&inner.wal_path(new_gen))?;
        inner.wal_gen.store(new_gen, Ordering::SeqCst);
        {
            let mut w = self.shared.wal_state.lock();
            w.rotations.push_back((covered, old_file));
            w.file = self.fw.file().clone();
        }
        self.shared.unpark_fsyncer();
        let old = Arc::new(std::mem::take(&mut *inner.mem.write()));
        inner.imm.store(Arc::new(Some(old.clone())));
        Ok(Some(Frozen { wal_gen: old_gen, mem: old }))
    }
}

/// Wait for `slot`'s producer to publish seq `seq`. The producer is only a
/// store away once claimed, so spin→yield — never sleep. Bails when the
/// pipeline is dead: `send` always stamps a claimed seq, so a gap only
/// exists if a producer died mid-publish — spinning forever helps nobody.
/// `false` = stop draining.
fn wait_stamp(shared: &Shared, slot: &Slot, seq: u64) -> bool {
    let mut i = 0u32;
    while slot.stamp.load(Ordering::Acquire) != seq {
        if i.is_multiple_of(1 << 16)
            && (!shared.alive.load(Ordering::Acquire) || shared.failed.lock().is_some())
        {
            return false;
        }
        i += 1;
        if i < 1024 {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }
    true
}

/// Idle polls before the consumer parks — the Disruptor trade: a short
/// spin keeps the consumer hot between bursts (a park/unpark round-trip
/// costs ~10-50µs, dwarfing a batch's work) while bounded enough to not
/// burn a core on a quiet database.
const IDLE_SPINS: u32 = 20_000;

fn run(shared: Arc<Shared>, inner: Weak<Inner>, fw: wal::FileWriter) {
    let _alive = AliveGuard(shared.clone());
    let _ = shared.consumer.set(thread::current());
    let mut c = Consumer { shared, fw, next: 1, pending: Vec::with_capacity(64) };
    'outer: loop {
        // Wait for work: spin a bounded window, then park.
        let mut idle = 0u32;
        let claim = loop {
            let claim = c.shared.next_claim.load(Ordering::Acquire);
            if c.next <= claim {
                break claim;
            }
            if c.shared.closing.load(Ordering::Acquire) || inner.upgrade().is_none() {
                break 'outer;
            }
            idle += 1;
            if idle < IDLE_SPINS {
                std::hint::spin_loop();
            } else {
                // Flag first, then one last claim check: a producer that
                // saw "running" between the loop-top poll and this store
                // skipped our wakeup — the re-check catches its seq.
                c.shared.consumer_idle.store(true, Ordering::Release);
                let claim = c.shared.next_claim.load(Ordering::Acquire);
                if c.next <= claim {
                    break claim;
                }
                // Producer unparks between our check and park land in the
                // permit, so the park below can't miss a wakeup.
                thread::park();
            }
        };
        c.shared.consumer_idle.store(false, Ordering::Relaxed);
        let Some(inner) = inner.upgrade() else { break };
        if !c.drain(&inner, claim) {
            break;
        }
    }
}

/// Second pipeline stage: paces `fsync` per policy without ever blocking
/// the consumer. fsyncs rotated files at their covered boundary first
/// (older data before newer), then the live file whenever `need` runs
/// ahead and the policy point is due — or a barrier ack is pending, which
/// forces an fsync under any policy. `durable` only moves forward.
fn fsync_loop(shared: Arc<Shared>, policy: wal::SyncPolicy) {
    let _ = shared.fsyncer.set(thread::current());
    let mut done = 0u64;
    let mut last = Instant::now();
    loop {
        if shared.failed.lock().is_some() {
            // I/O is dead: `fail` already errored acks + woke waiters.
            if shared.closing.load(Ordering::Acquire) || !shared.alive.load(Ordering::Acquire) {
                break;
            }
            thread::park();
            continue;
        }
        // Rotated files first, in order — their boundary covers older seqs.
        let rot = shared.wal_state.lock().rotations.pop_front();
        if let Some((covered, f)) = rot {
            match f.sync_all() {
                Ok(()) => {
                    done = done.max(covered);
                    last = Instant::now();
                }
                Err(e) => shared.fail(e),
            }
            shared.publish(&shared.durable, done);
            continue;
        }
        let acks_pending = !shared.sync_acks.lock().is_empty();
        // Shutdown forces one last fsync so `close`/drop is never dropped
        // on the floor — and is what lets a `Never`-policy loop reach quiet.
        let shutting_down = shared.closing.load(Ordering::Acquire) || !shared.alive.load(Ordering::Acquire);
        let due = shutting_down
            || acks_pending
            || match policy {
                wal::SyncPolicy::Always => true,
                wal::SyncPolicy::EveryMs(ms) => last.elapsed() >= Duration::from_millis(ms),
                wal::SyncPolicy::Never => false,
            };
        // File + need under one lock: `need` can't outrun what the file we
        // sync actually contains — a rotation pushed between the two reads
        // would let `done` claim seqs living in an unsynced old file.
        let (f, target) = {
            let w = shared.wal_state.lock();
            (w.file.clone(), shared.need.load(Ordering::Acquire))
        };
        if target > done && due {
            match f.sync_all() {
                Ok(()) => {
                    done = done.max(target);
                    last = Instant::now();
                }
                Err(e) => shared.fail(e),
            }
            shared.publish(&shared.durable, done);
            continue;
        }
        // Resolve barriers whose seq is now durable.
        {
            let mut acks = shared.sync_acks.lock();
            while let Some((s, _)) = acks.front() {
                if *s > done {
                    break;
                }
                let (_, a) = acks.pop_front().expect("front checked");
                let _ = a.send(Ok(()));
            }
        }
        let quiet = shared.sync_acks.lock().is_empty()
            && shared.wal_state.lock().rotations.is_empty()
            && shared.need.load(Ordering::Acquire) <= done;
        if quiet && shutting_down {
            break;
        }
        // Everything queued is durable and acks are resolved — on shutdown
        // (close/drop) or consumer death there is nothing left to pace, so
        // exit instead of parking forever on a detached thread.
        if shutting_down {
            break;
        }
        // Pending periodic work: wake at the next due point. Otherwise
        // sleep until the consumer unparks (new need/rotation/ack/drop).
        match policy {
            wal::SyncPolicy::EveryMs(ms) if shared.need.load(Ordering::Acquire) > done => {
                let rem = Duration::from_millis(ms).saturating_sub(last.elapsed());
                // rem == 0 means the window already lapsed — don't park,
                // loop back and fsync now, else the gap overshoots `ms`.
                if !rem.is_zero() {
                    thread::park_timeout(rem);
                }
            }
            _ => thread::park(),
        }
    }
}
