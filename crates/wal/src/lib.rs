//! Append-only write-ahead log.
//!
//! One record on disk is one atomic unit (a single op or a whole batch):
//!
//! ```text
//! frame   = crc32 u32 | len u32 | payload[len]
//! payload = op*
//! op      = tag u8 | klen u32 | key[klen]                (tag = 2, delete)
//!         | tag u8 | klen u32 | vlen u32 | key | value   (tag = 1, put)
//! ```
//!
//! `crc32` covers `len || payload` — a torn tail write fails the checksum or
//! the length bound and replay simply stops there, treating everything after
//! `valid_len` as garbage from the crash.
//!
//! # Why a dedicated writer thread
//!
//! Same trick as reflexdb's AOF: a single background thread owns the file and
//! is the only thing calling `write(2)`/`fsync(2)`. `append()` hands it
//! pre-encoded bytes over a channel and returns immediately; the writer drains
//! everything already queued and coalesces it into one `write` plus at most
//! one `fsync` — group commit, so N concurrent writers cost O(1) syscalls.
//!
//! `append()` is fire-and-forget by itself: with [`SyncPolicy::EveryMs`] /
//! [`SyncPolicy::Never`] an acknowledged write can still be lost if the process
//! dies inside the fsync window. [`SyncPolicy::Always`] callers wait on the
//! returned [`Append::wait`] — an acknowledged write is on disk before the
//! caller is told. [`Writer::sync`] is the explicit "make everything so far
//! durable" barrier regardless of policy.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};

const TAG_PUT: u8 = 1;
const TAG_DEL: u8 = 2;
const HEADER_LEN: usize = 8;
/// Upper bound on one record's payload — rejects length fields smashed by a
/// torn write before we try to allocate against them.
const MAX_PAYLOAD: u64 = 256 * 1024 * 1024;
/// Caps on one coalesced write batch — shared by the channel writer and
/// the pipeline consumer.
pub const MAX_BATCH: usize = 1024;
/// Byte cap on one coalesced write batch.
pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// One logged operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put(Bytes, Bytes),
    Delete(Bytes),
}

/// How aggressively the writer thread fsyncs.
///
/// | Policy | fsync frequency | Loss bound on crash |
/// |--------|-----------------|---------------------|
/// | `Always` | every batch | none for writes that waited on `Append::wait` |
/// | `EveryMs(ms)` | at most `ms` stale | up to ~`ms` of writes |
/// | `Never` | only via `sync()`/`rotate()` | OS-dependent |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    Always,
    EveryMs(u64),
    Never,
}

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("corrupt record at offset {offset}: {reason}")]
    Corrupt { offset: u64, reason: &'static str },
}

/// Encodes `ops` into a payload `Bytes` (no frame header — the writer thread
/// frames it). Cheap: one allocation for the payload.
pub fn encode_ops(ops: &[Op]) -> Bytes {
    let mut len = 0;
    for op in ops {
        len += match op {
            Op::Put(k, v) => 9 + k.len() + v.len(),
            Op::Delete(k) => 5 + k.len(),
        };
    }
    let mut buf = BytesMut::with_capacity(len);
    for op in ops {
        match op {
            Op::Put(k, v) => {
                buf.put_u8(TAG_PUT);
                buf.put_u32_le(k.len() as u32);
                buf.put_u32_le(v.len() as u32);
                buf.extend_from_slice(k);
                buf.extend_from_slice(v);
            }
            Op::Delete(k) => {
                buf.put_u8(TAG_DEL);
                buf.put_u32_le(k.len() as u32);
                buf.extend_from_slice(k);
            }
        }
    }
    buf.freeze()
}

/// Decodes one record payload back into ops. Errors on any malformed field —
/// used by replay where the frame CRC already passed, so a failure here means
/// real corruption, not a torn tail.
pub fn decode_ops(payload: &[u8]) -> Result<Vec<Op>, WalError> {
    let mut ops = Vec::new();
    let mut pos = 0usize;
    while pos < payload.len() {
        let take = |pos: &mut usize, n: usize| -> Result<&[u8], WalError> {
            let end = pos.saturating_add(n);
            if end > payload.len() {
                return Err(WalError::Corrupt { offset: 0, reason: "truncated op" });
            }
            let s = &payload[*pos..end];
            *pos = end;
            Ok(s)
        };
        let tag = take(&mut pos, 1)?[0];
        let klen = u32::from_le_bytes(take(&mut pos, 4)?.try_into().unwrap()) as usize;
        match tag {
            TAG_PUT => {
                let vlen = u32::from_le_bytes(take(&mut pos, 4)?.try_into().unwrap()) as usize;
                let key = Bytes::copy_from_slice(take(&mut pos, klen)?);
                let val = Bytes::copy_from_slice(take(&mut pos, vlen)?);
                ops.push(Op::Put(key, val));
            }
            TAG_DEL => {
                let key = Bytes::copy_from_slice(take(&mut pos, klen)?);
                ops.push(Op::Delete(key));
            }
            _ => return Err(WalError::Corrupt { offset: 0, reason: "bad tag" }),
        }
    }
    Ok(ops)
}

enum Job {
    Append {
        payload: Bytes,
        ack: Option<SyncSender<io::Result<()>>>,
    },
    /// Barrier: write + fsync everything queued before it, then ack.
    Sync {
        ack: SyncSender<io::Result<()>>,
    },
    /// fsync the current file, then open `path` as the new active file.
    Rotate {
        path: std::path::PathBuf,
        ack: SyncSender<io::Result<()>>,
    },
}

/// Handle to the writer thread's durable-write ack. Resolves once the record
/// has been written, and fsync'd when the batch's policy point required it —
/// for [`SyncPolicy::Always`] that means really on disk; for the weaker
/// policies it means at least in the kernel's buffers (use [`Writer::sync`]
/// for an fsync barrier).
pub struct Append {
    rx: Option<Receiver<io::Result<()>>>,
}

impl Append {
    /// Blocks until the writer thread reports durability for this record.
    /// No-op resolve for fire-and-forget appends (returns `Ok` immediately).
    pub fn wait(self) -> io::Result<()> {
        match self.rx {
            Some(rx) => rx.recv().unwrap_or_else(|_| Err(writer_gone())),
            None => Ok(()),
        }
    }
}

/// The WAL. Clone-free handle; internally all shared state lives on the writer
/// thread side of the channel.
pub struct Writer {
    tx: Mutex<SyncSender<Job>>,
    io_failed: Arc<AtomicBool>,
    _worker: thread::JoinHandle<()>,
}

impl Writer {
    /// Opens (or creates) `path` for appending and spawns the writer thread.
    /// The caller is expected to have already truncated any torn tail via
    /// [`replay`]'s reported `valid_len`.
    pub fn open(path: &Path, policy: SyncPolicy) -> io::Result<Self> {
        let file = open_append(path)?;
        let (tx, rx) = mpsc::sync_channel(4096);
        let io_failed = Arc::new(AtomicBool::new(false));
        let flag = io_failed.clone();
        let worker = thread::Builder::new()
            .name("rexodb-wal-writer".into())
            .spawn(move || writer_loop(file, rx, policy, flag))
            .map_err(|e| io::Error::other(format!("failed to spawn wal writer: {e}")))?;
        Ok(Self { tx: Mutex::new(tx), io_failed, _worker: worker })
    }

    /// Queues `ops` as one atomic record. With `need_ack`, the returned
    /// [`Append`] resolves after the write reaches the durability level the
    /// policy promises; without it the handle resolves immediately.
    pub fn append(&self, ops: &[Op], need_ack: bool) -> io::Result<Append> {
        let (ack, rx) = if need_ack {
            let (tx, rx) = mpsc::sync_channel(1);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        self.send(Job::Append { payload: encode_ops(ops), ack })?;
        Ok(Append { rx })
    }

    /// fsync barrier: resolves after every record queued before this call is
    /// written and fsync'd, whatever the policy.
    pub fn sync(&self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Job::Sync { ack: tx })?;
        rx.recv().unwrap_or_else(|_| Err(writer_gone()))
    }

    /// fsync the current file's pending data and swap `path` in as the new
    /// active file. Returns once the new file is live — the caller is
    /// unblocked before the replaced file's fsync finishes, but append acks
    /// queued before this call still resolve only after that fsync, so the
    /// `Always` durability contract is preserved.
    pub fn rotate(&self, path: &Path) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Job::Rotate { path: path.to_path_buf(), ack: tx })?;
        rx.recv().unwrap_or_else(|_| Err(writer_gone()))
    }

    /// `true` once the writer thread has seen a `write`/`fsync` failure —
    /// subsequent writes should be refused rather than acked.
    pub fn io_failed(&self) -> bool {
        self.io_failed.load(Ordering::Relaxed)
    }

    fn send(&self, job: Job) -> io::Result<()> {
        let res = self.tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).send(job);
        if res.is_err() {
            self.io_failed.store(true, Ordering::Relaxed);
        }
        res.map_err(|_| writer_gone())
    }
}

fn writer_gone() -> io::Error {
    io::Error::other("wal writer thread is no longer running")
}

fn open_append(path: &Path) -> io::Result<File> {
    let f = OpenOptions::new().create(true).append(true).open(path)?;
    sync_parent(path)?;
    Ok(f)
}

/// fsyncs `path`'s parent dir — a file's `sync_all` covers its contents,
/// not the directory entry, so a crash right after create can still lose
/// the filename. No-op off Unix.
fn sync_parent(path: &Path) -> io::Result<()> {
    if cfg!(unix) {
        if let Some(dir) = path.parent() {
            File::open(dir)?.sync_all()?;
        }
    }
    Ok(())
}

fn fsync(file: &File) -> io::Result<()> {
    file.sync_all()
}

fn err_copy(e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), e.to_string())
}

fn resolve(acks: &mut Vec<SyncSender<io::Result<()>>>, res: &io::Result<()>) {
    for a in acks.drain(..) {
        let _ = a.send(match res {
            Ok(()) => Ok(()),
            Err(e) => Err(err_copy(e)),
        });
    }
}

/// Writer-thread main loop: block for one job, drain everything else already
/// queued, coalesce appends into a single `write`, fsync per policy point,
/// then resolve acks. `Sync`/`Rotate` force a flush + fsync at their queue
/// position so their barrier semantics hold regardless of policy. After a
/// write/fsync failure the loop keeps draining (so callers' acks never hang)
/// but every subsequent job just gets the stored error back — `io_failed`
/// tells the Db layer to refuse new writes.
fn writer_loop(mut file: File, rx: Receiver<Job>, policy: SyncPolicy, io_failed: Arc<AtomicBool>) {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut acks: Vec<SyncSender<io::Result<()>>> = Vec::with_capacity(64);
    let mut last_fsync = Instant::now();
    let mut pending: Option<Job> = None;
    let mut failed: Option<io::Error> = None;

    loop {
        let first = match pending.take() {
            Some(j) => j,
            None => match rx.recv() {
                Ok(j) => j,
                Err(_) => break,
            },
        };
        let mut jobs = Vec::with_capacity(64);
        let mut bytes = job_size(&first);
        jobs.push(first);
        while jobs.len() < MAX_BATCH {
            let Ok(job) = rx.try_recv() else { break };
            let n = job_size(&job);
            if bytes.saturating_add(n) > MAX_BATCH_BYTES {
                pending = Some(job);
                break;
            }
            bytes += n;
            jobs.push(job);
        }

        for job in jobs {
            match job {
                Job::Append { payload, ack } => {
                    if let Some(e) = &failed {
                        // Poisoned: ack the write as failed instead of framing.
                        if let Some(a) = ack {
                            let _ = a.send(Err(err_copy(e)));
                        }
                    } else {
                        frame(&payload, &mut buf);
                        if let Some(a) = ack {
                            acks.push(a);
                        }
                    }
                }
                Job::Sync { ack } => {
                    if failed.is_none() && !buf.is_empty() {
                        if let Err(e) = file.write_all(&buf).and_then(|_| fsync(&file)) {
                            failed = Some(e);
                        }
                    } else if failed.is_none() {
                        if let Err(e) = fsync(&file) {
                            failed = Some(e);
                        }
                    }
                    buf.clear();
                    last_fsync = Instant::now();
                    let res = match &failed {
                        None => Ok(()),
                        Some(e) => Err(err_copy(e)),
                    };
                    resolve(&mut acks, &res);
                    let _ = ack.send(res);
                }
                Job::Rotate { path, ack } => {
                    if let Some(e) = &failed {
                        let _ = ack.send(Err(err_copy(e)));
                    } else {
                        match file.write_all(&buf).and_then(|_| open_append(&path)) {
                            Ok(f) => {
                                // Swap first, unblock the caller immediately —
                                // it only needs the new file live. The replaced
                                // file's fsync happens right after; append acks
                                // resolve only once it lands.
                                let old = std::mem::replace(&mut file, f);
                                let _ = ack.send(Ok(()));
                                if let Err(e) = fsync(&old) {
                                    failed = Some(e);
                                }
                            }
                            Err(e) => {
                                let _ = ack.send(Err(err_copy(&e)));
                                failed = Some(e);
                            }
                        }
                    }
                    buf.clear();
                    last_fsync = Instant::now();
                    if let Some(e) = &failed {
                        resolve(&mut acks, &Err(err_copy(e)));
                    } else {
                        resolve(&mut acks, &Ok(()));
                    }
                }
            }
        }

        if !buf.is_empty() {
            if failed.is_none() {
                if let Err(e) = file.write_all(&buf) {
                    failed = Some(e);
                } else {
                    let need_fsync = match policy {
                        SyncPolicy::Always => true,
                        SyncPolicy::EveryMs(ms) => last_fsync.elapsed() >= Duration::from_millis(ms),
                        SyncPolicy::Never => false,
                    };
                    if need_fsync {
                        if let Err(e) = fsync(&file) {
                            failed = Some(e);
                        }
                        last_fsync = Instant::now();
                    }
                }
            }
            buf.clear();
        }
        if let Some(e) = &failed {
            io_failed.store(true, Ordering::Relaxed);
            resolve(&mut acks, &Err(err_copy(e)));
        } else {
            resolve(&mut acks, &Ok(()));
        }
    }
}

fn job_size(job: &Job) -> usize {
    match job {
        Job::Append { payload, .. } => payload.len() + HEADER_LEN,
        _ => 0,
    }
}

fn frame(payload: &[u8], buf: &mut Vec<u8>) {
    let mut hasher = crc32fast::Hasher::new();
    let len = (payload.len() as u32).to_le_bytes();
    hasher.update(&len);
    hasher.update(payload);
    buf.extend_from_slice(&hasher.finalize().to_le_bytes());
    buf.extend_from_slice(&len);
    buf.extend_from_slice(payload);
}

/// Replays `path`, invoking `f` with each record's ops in write order.
/// Returns the byte length of the valid prefix — anything past it is a torn
/// or corrupt tail; callers may `set_len(valid_len)` to repair the file.
/// A record whose framing is intact but whose payload can't decode is real
/// corruption and is reported, not silently swallowed.
pub fn replay(path: &Path, mut f: impl FnMut(Vec<Op>)) -> Result<u64, WalError> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut r = BufReader::with_capacity(256 * 1024, file);
    let mut valid_len = 0u64;
    let mut header = [0u8; HEADER_LEN];
    loop {
        match r.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64;
        let record_end = valid_len + HEADER_LEN as u64 + len;
        if len > MAX_PAYLOAD || record_end > file_len {
            break; // torn tail: declared length runs past real data
        }
        let mut payload = vec![0u8; len as usize];
        if let Err(e) = r.read_exact(&mut payload) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[4..8]);
        hasher.update(&payload);
        if hasher.finalize() != u32::from_le_bytes(header[0..4].try_into().unwrap()) {
            break; // torn tail or flipped bits — everything valid is before this
        }
        f(decode_ops(&payload).map_err(|_| WalError::Corrupt {
            offset: valid_len,
            reason: "payload failed to decode after passing crc",
        })?);
        valid_len = record_end;
    }
    Ok(valid_len)
}

/// Truncates `path` to `len` — used after [`replay`] to drop a torn tail so
/// future appends don't pile onto garbage.
pub fn truncate(path: &Path, len: u64) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.seek(SeekFrom::Start(len))?;
    Ok(())
}

/// Encodes `ops` as one complete framed record (`crc|len|payload`) in a
/// single allocation. Producer-side work for the pipeline write path — the
/// consumer's hot loop is then just a memcpy into the write buffer.
pub fn encode_record(ops: &[Op]) -> Bytes {
    let mut buf = Vec::new();
    encode_record_into(ops, &mut buf);
    buf.into()
}

/// Appends one framed record to `buf`, reusing its capacity — the pipeline
/// keeps a buffer per ring slot so steady-state submits allocate nothing.
pub fn encode_record_into(ops: &[Op], buf: &mut Vec<u8>) {
    let mut payload_len = 0;
    for op in ops {
        payload_len += match op {
            Op::Put(k, v) => 9 + k.len() + v.len(),
            Op::Delete(k) => 5 + k.len(),
        };
    }
    buf.reserve(HEADER_LEN + payload_len);
    let start = buf.len();
    buf.extend_from_slice(&[0; 4]);
    buf.put_u32_le(payload_len as u32);
    for op in ops {
        match op {
            Op::Put(k, v) => {
                buf.put_u8(TAG_PUT);
                buf.put_u32_le(k.len() as u32);
                buf.put_u32_le(v.len() as u32);
                buf.extend_from_slice(k);
                buf.extend_from_slice(v);
            }
            Op::Delete(k) => {
                buf.put_u8(TAG_DEL);
                buf.put_u32_le(k.len() as u32);
                buf.extend_from_slice(k);
            }
        }
    }
    let crc = crc32fast::hash(&buf[start + 4..]); // crc covers len || payload
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Synchronous WAL file writer — same on-disk framing as [`Writer`], but the
/// caller's thread does the writes. Owned by rexodb's pipeline consumer; the
/// file is shared so a separate thread can `fsync` it concurrently with
/// appends (`write`/`fsync` on the same fd is safe).
pub struct FileWriter {
    file: std::sync::Arc<File>,
    buf: Vec<u8>,
}

impl FileWriter {
    /// Opens (or creates) `path` for appending.
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self { file: std::sync::Arc::new(open_append(path)?), buf: Vec::with_capacity(64 * 1024) })
    }

    /// The active file, for sharing with an fsync thread.
    pub fn file(&self) -> &std::sync::Arc<File> {
        &self.file
    }

    /// Queues one [`encode_record`]-framed record; nothing reaches the file
    /// until [`FileWriter::flush`].
    pub fn queue(&mut self, framed: &[u8]) {
        self.buf.extend_from_slice(framed);
    }

    /// One `write` for everything queued since the last flush.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    /// Swaps `path` in as the active file and returns the replaced one — the
    /// caller fsyncs the old file (possibly on another thread so callers
    /// unblocked by the swap don't wait on rotation).
    pub fn rotate(&mut self, path: &Path) -> io::Result<std::sync::Arc<File>> {
        let f = std::sync::Arc::new(open_append(path)?);
        Ok(std::mem::replace(&mut self.file, f))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops() -> Vec<Op> {
        vec![
            Op::Put(Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            Op::Delete(Bytes::from_static(b"k2")),
            Op::Put(Bytes::from_static(b""), Bytes::from_static(b"")),
        ]
    }

    #[test]
    fn encode_decode_roundtrip() {
        assert_eq!(decode_ops(&encode_ops(&ops())).unwrap(), ops());
    }

    #[test]
    fn append_replays_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.log");
        {
            let w = Writer::open(&path, SyncPolicy::Always).unwrap();
            for op in ops() {
                w.append(&[op], true).unwrap().wait().unwrap();
            }
            w.sync().unwrap();
        }
        let mut got = Vec::new();
        replay(&path, |rec| got.extend(rec)).unwrap();
        assert_eq!(got, ops());
    }

    #[test]
    fn torn_tail_stops_at_valid_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("2.log");
        let w = Writer::open(&path, SyncPolicy::Always).unwrap();
        w.append(&[Op::Put(Bytes::from_static(b"a"), Bytes::from_static(b"1"))], true)
            .unwrap()
            .wait()
            .unwrap();
        drop(w);
        let good_len = std::fs::metadata(&path).unwrap().len();

        // Simulate a torn write: half a record + garbage.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x00]).unwrap();
        drop(f);

        let mut got = Vec::new();
        let valid = replay(&path, |rec| got.extend(rec)).unwrap();
        assert_eq!(valid, good_len);
        assert_eq!(got, vec![Op::Put(Bytes::from_static(b"a"), Bytes::from_static(b"1"))]);

        truncate(&path, valid).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);
    }

    #[test]
    fn encode_record_matches_writer_framing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("4.log");
        let mut w = FileWriter::open(&path).unwrap();
        w.queue(&encode_record(&ops()));
        w.flush().unwrap();
        drop(w);
        let mut got = Vec::new();
        let valid = replay(&path, |rec| got.extend(rec)).unwrap();
        assert_eq!(valid, std::fs::metadata(&path).unwrap().len());
        assert_eq!(got, ops());
    }

    #[test]
    fn file_writer_rotate_replays_both_files() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("5a.log");
        let p2 = dir.path().join("5b.log");
        let mut w = FileWriter::open(&p1).unwrap();
        w.queue(&encode_record(&[Op::Put(Bytes::from_static(b"a"), Bytes::from_static(b"1"))]));
        w.flush().unwrap();
        let old = w.rotate(&p2).unwrap();
        old.sync_all().unwrap();
        w.queue(&encode_record(&[Op::Put(Bytes::from_static(b"b"), Bytes::from_static(b"2"))]));
        w.flush().unwrap();
        drop(w);
        let mut got = Vec::new();
        replay(&p1, |rec| got.extend(rec)).unwrap();
        replay(&p2, |rec| got.extend(rec)).unwrap();
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn batch_is_one_atomic_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("3.log");
        let w = Writer::open(&path, SyncPolicy::Always).unwrap();
        w.append(&ops(), true).unwrap().wait().unwrap();
        drop(w);
        let mut records = 0;
        replay(&path, |rec| {
            records += 1;
            assert_eq!(rec, ops());
        })
        .unwrap();
        assert_eq!(records, 1);
    }
}
