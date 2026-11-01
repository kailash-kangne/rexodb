//! Immutable sorted segment files — what a flushed memtable becomes.
//!
//! ```text
//! data    = entry*                       (sorted by key, ascending)
//! entry   = klen u32 | vlen u32 | key | value | crc u32
//!           vlen == u32::MAX marks a tombstone (delete), no value bytes
//!           crc32 covers klen|vlen|key|value — checked on every read
//! index   = (klen u32 | offset u64 | key)*   sparse: every INDEX_EVERY-th key
//! bloom   = bloom bit array (10 bits/entry, k=7 probes, XXH3 + double-hash)
//! footer  = index_off u64 | index_len u32 | bloom_off u64 | bloom_len u32
//!         | count u64 | index_crc u32 | bloom_crc u32 | magic u64  (48 B, EOF)
//! ```
//!
//! Every entry read (point `get`, iterators, compaction) verifies its CRC
//! as the bytes stream through, so bit-rot surfaces as
//! [`SegmentError::Corrupt`] instead of wrong data. The index and bloom
//! CRCs are checked at open — both blocks are read there anyway, so the
//! check costs one extra hash and no I/O. [`Reader::verify`] re-checks the
//! whole data region on demand.
//!
//! A point `get` first asks the bloom filter — "definitely absent" answers
//! skip the disk entirely (~1% false-positive rate, zero false negatives).
//! Maybe-present keys binary-search the in-RAM sparse index for the floor
//! key, then scan that one index gap (`index_every` entries max) — ~1
//! syscall. Entry headers are bounds-checked against the gap's end offset
//! before any allocation, so a corrupt length field can't turn into a huge
//! `Vec`.
//!
//! Range/compaction iteration streams entries via [`Reader::iter`]; the merge
//! (newest-wins, tombstone suppression) lives in the `rexodb` crate.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

const MAGIC: u64 = u64::from_le_bytes(*b"RXSSEG03");
/// Process-unique id per opened file — unlike `id` (the filename gen,
/// which a merged segment inherits from its oldest input), a `uid` is
/// never shared between two different files.
static NEXT_UID: AtomicU64 = AtomicU64::new(1);
const TOMBSTONE: u32 = u32::MAX;
const FOOTER_LEN: u64 = 48;
const CRC_LEN: usize = 4;
const CHUNK: usize = 64 * 1024;

// Bloom parameters — fixed by the format, not stored: bit count is
// `bloom_len * 8` and probe count is `BLOOM_K`.
const BLOOM_BITS_PER_KEY: u64 = 10;
const BLOOM_K: u32 = 7;

/// Builds the bloom bit array for `hashes` (one `xxh3_64(key)` per entry).
/// `bits.len()` is rounded up to a whole byte, and the effective bit count
/// used for probing is `bits.len() * 8` so readers can derive it from the
/// stored length alone.
fn bloom_build(count: u64, hashes: &[u64]) -> Vec<u8> {
    let bits_len = (count.saturating_mul(BLOOM_BITS_PER_KEY).max(64) / 8) as usize;
    let m = bits_len as u64 * 8;
    let mut bits = vec![0u8; bits_len.max(1)];
    for &h0 in hashes {
        let mut h = h0;
        let delta = h.rotate_left(17);
        for _ in 0..BLOOM_K {
            let bit = (h % m) as usize;
            bits[bit / 8] |= 1 << (bit % 8);
            h = h.wrapping_add(delta);
        }
    }
    bits
}

/// `false` = key is definitely not in the segment (no false negatives);
/// `true` = maybe — caller must do the real lookup.
fn bloom_maybe(bits: &[u8], key: &[u8]) -> bool {
    if bits.is_empty() {
        return true; // no bloom info → don't skip the lookup
    }
    let m = bits.len() as u64 * 8;
    let mut h = xxh3_64(key);
    let delta = h.rotate_left(17);
    for _ in 0..BLOOM_K {
        let bit = (h % m) as usize;
        if bits[bit / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
        h = h.wrapping_add(delta);
    }
    true
}

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("corrupt segment: {0}")]
    Corrupt(&'static str),
    #[error("keys added out of order")]
    Unsorted,
}

/// Result of a point lookup — three states, because "deleted here" differs
/// from "not in this segment" when newer tombstones must shadow older data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found {
    Value(Bytes),
    Tombstone,
}

/// Builds one segment file. Keys must arrive in strictly increasing order —
/// the caller (memtable flush / compaction merge) guarantees this.
pub struct Builder {
    w: BufWriter<File>,
    index: Vec<(Bytes, u64)>,
    /// One `xxh3_64(key)` per entry — bloom is built at `finish`, when the
    /// final count (and therefore bit count) is known.
    hashes: Vec<u64>,
    last_key: Option<Bytes>,
    index_every: usize,
    count: u64,
    off: u64,
}

impl Builder {
    pub fn create(path: &Path, index_every: usize) -> Result<Self, SegmentError> {
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
        Ok(Self {
            w: BufWriter::with_capacity(256 * 1024, file),
            index: Vec::new(),
            hashes: Vec::new(),
            last_key: None,
            index_every: index_every.max(1),
            count: 0,
            off: 0,
        })
    }

    /// `value == None` writes a tombstone. Returns [`SegmentError::Unsorted`]
    /// if `key` doesn't exceed the previous key.
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), SegmentError> {
        if let Some(last) = &self.last_key {
            if key <= last.as_ref() {
                return Err(SegmentError::Unsorted);
            }
        }
        if self.count.is_multiple_of(self.index_every as u64) {
            self.index.push((Bytes::copy_from_slice(key), self.off));
        }
        self.hashes.push(xxh3_64(key));
        let mut h = crc32fast::Hasher::new();
        let klen = (key.len() as u32).to_le_bytes();
        h.update(&klen);
        self.w.write_all(&klen)?;
        match value {
            Some(v) => {
                let vlen = (v.len() as u32).to_le_bytes();
                h.update(&vlen);
                h.update(key);
                h.update(v);
                self.w.write_all(&vlen)?;
                self.w.write_all(key)?;
                self.w.write_all(v)?;
                self.off += 12 + key.len() as u64 + v.len() as u64;
            }
            None => {
                let vlen = TOMBSTONE.to_le_bytes();
                h.update(&vlen);
                h.update(key);
                self.w.write_all(&vlen)?;
                self.w.write_all(key)?;
                self.off += 12 + key.len() as u64;
            }
        }
        self.w.write_all(&h.finalize().to_le_bytes())?;
        self.last_key = Some(Bytes::copy_from_slice(key));
        self.count += 1;
        Ok(())
    }

    /// Writes index + bloom + footer, flushes and fsyncs. Returns entry
    /// count and total file size.
    pub fn finish(mut self) -> Result<(u64, u64), SegmentError> {
        let index_off = self.off;
        let mut index_len = 0u64;
        let mut index_crc = crc32fast::Hasher::new();
        for (key, off) in &self.index {
            let klen = (key.len() as u32).to_le_bytes();
            let off = off.to_le_bytes();
            index_crc.update(&klen);
            index_crc.update(&off);
            index_crc.update(key);
            self.w.write_all(&klen)?;
            self.w.write_all(&off)?;
            self.w.write_all(key)?;
            index_len += 12 + key.len() as u64;
        }
        let bloom_off = index_off + index_len;
        let bloom = bloom_build(self.count, &self.hashes);
        let bloom_crc = crc32fast::hash(&bloom);
        self.w.write_all(&bloom)?;
        let bloom_len = bloom.len() as u64;
        self.w.write_all(&index_off.to_le_bytes())?;
        self.w.write_all(&(index_len as u32).to_le_bytes())?;
        self.w.write_all(&bloom_off.to_le_bytes())?;
        self.w.write_all(&(bloom_len as u32).to_le_bytes())?;
        self.w.write_all(&self.count.to_le_bytes())?;
        self.w.write_all(&index_crc.finalize().to_le_bytes())?;
        self.w.write_all(&bloom_crc.to_le_bytes())?;
        self.w.write_all(&MAGIC.to_le_bytes())?;
        self.w.flush()?;
        let file = self.w.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        let size = bloom_off + bloom_len + FOOTER_LEN;
        Ok((self.count, size))
    }
}

/// Anything the cursor can `pread` from — `&File` for point gets (no fd dup)
/// and `File` for owned iterators.
trait ReadAt {
    fn pread_at(&self, buf: &mut [u8], off: u64) -> io::Result<()>;
}

impl ReadAt for File {
    fn pread_at(&self, buf: &mut [u8], off: u64) -> io::Result<()> {
        #[cfg(unix)]
        {
            FileExt::read_exact_at(self, buf, off)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = self;
            f.seek(SeekFrom::Start(off))?;
            f.read_exact(buf)
        }
    }
}

impl<T: ReadAt + ?Sized> ReadAt for &T {
    fn pread_at(&self, buf: &mut [u8], off: u64) -> io::Result<()> {
        (**self).pread_at(buf, off)
    }
}

/// A read handle over one segment file. Holds the sparse index in RAM; every
/// lookup is `pread`-based, so readers never take locks on the file.
pub struct Reader {
    file: File,
    pub path: std::path::PathBuf,
    /// Segment identity = filename generation; unique among live segments.
    id: u64,
    /// Instance identity — unique even across files that share a gen
    /// (merged outputs inherit their oldest input's filename).
    uid: u64,
    index: Vec<(Bytes, u64)>, // (key, data offset), sorted
    index_off: u64,
    bloom: Vec<u8>,
    pub count: u64,
    pub bytes: u64,
}

impl Reader {
    pub fn open(path: &Path) -> Result<Self, SegmentError> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_LEN {
            return Err(SegmentError::Corrupt("file smaller than footer"));
        }
        let mut footer = [0u8; FOOTER_LEN as usize];
        file.pread_at(&mut footer, len - FOOTER_LEN)?;
        let index_off = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u32::from_le_bytes(footer[8..12].try_into().unwrap()) as u64;
        let bloom_off = u64::from_le_bytes(footer[12..20].try_into().unwrap());
        let bloom_len = u32::from_le_bytes(footer[20..24].try_into().unwrap()) as u64;
        let count = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let index_crc = u32::from_le_bytes(footer[32..36].try_into().unwrap());
        let bloom_crc = u32::from_le_bytes(footer[36..40].try_into().unwrap());
        if u64::from_le_bytes(footer[40..48].try_into().unwrap()) != MAGIC {
            return Err(SegmentError::Corrupt("bad magic"));
        }
        if index_off.checked_add(index_len) != Some(bloom_off)
            || bloom_off.checked_add(bloom_len).is_none_or(|e| e + FOOTER_LEN > len)
        {
            return Err(SegmentError::Corrupt("index/bloom out of bounds"));
        }
        let mut raw = vec![0u8; index_len as usize];
        file.pread_at(&mut raw, index_off)?;
        if crc32fast::hash(&raw) != index_crc {
            return Err(SegmentError::Corrupt("index crc mismatch"));
        }
        let mut index = Vec::new();
        let mut pos = 0usize;
        while pos < raw.len() {
            if pos + 12 > raw.len() {
                return Err(SegmentError::Corrupt("truncated index entry"));
            }
            let klen = u32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
            let off = u64::from_le_bytes(raw[pos + 4..pos + 12].try_into().unwrap());
            pos += 12;
            if pos + klen > raw.len() {
                return Err(SegmentError::Corrupt("truncated index key"));
            }
            index.push((Bytes::copy_from_slice(&raw[pos..pos + klen]), off));
            pos += klen;
        }
        let mut bloom = vec![0u8; bloom_len as usize];
        file.pread_at(&mut bloom, bloom_off)?;
        if crc32fast::hash(&bloom) != bloom_crc {
            return Err(SegmentError::Corrupt("bloom crc mismatch"));
        }
        let id = path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
        let uid = NEXT_UID.fetch_add(1, Ordering::Relaxed);
        Ok(Self { file, path: path.to_path_buf(), id, uid, index, index_off, bloom, count, bytes: len })
    }

    /// Unique id of this segment (its filename generation, 0 if unparseable).
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Process-unique id for cache keying — safe to key on even when two
    /// different files share a filename gen (compaction reuse).
    pub fn uid(&self) -> u64 {
        self.uid
    }

    /// Data offset where a scan for `key` should begin (sparse-index floor).
    /// `None` when `key` precedes the first entry — nothing to scan.
    fn floor(&self, key: &[u8]) -> Option<(u64, u64)> {
        let idx = self.index.partition_point(|(k, _)| k.as_ref() <= key);
        if idx == 0 {
            return None;
        }
        let start = self.index[idx - 1].1;
        let end = if idx < self.index.len() { self.index[idx].1 } else { self.index_off };
        Some((start, end))
    }

    /// Point lookup. `Ok(None)` = key never in this segment;
    /// `Ok(Some(Found::Tombstone))` = deleted here.
    pub fn get(&self, key: &[u8]) -> Result<Option<Found>, SegmentError> {
        if !bloom_maybe(&self.bloom, key) {
            return Ok(None); // definitely absent — skip the disk entirely
        }
        let Some((start, end)) = self.floor(key) else { return Ok(None) };
        let mut cur = Cursor::new(&self.file, start, end);
        loop {
            let Some((klen, vlen)) = entry_begin(&mut cur)? else { return Ok(None) };
            match cur.take(klen)?.cmp(key) {
                std::cmp::Ordering::Less => {
                    if vlen != TOMBSTONE {
                        cur.skip(vlen as u64)?;
                    }
                    entry_crc(&mut cur)?;
                    continue;
                }
                // The entry past the target is still read + verified before
                // its key is trusted to end the scan — a corrupted key must
                // not silently answer "absent".
                std::cmp::Ordering::Greater => {
                    if vlen != TOMBSTONE {
                        cur.skip(vlen as u64)?;
                    }
                    entry_crc(&mut cur)?;
                    return Ok(None);
                }
                std::cmp::Ordering::Equal => {
                    return if vlen == TOMBSTONE {
                        entry_crc(&mut cur)?;
                        Ok(Some(Found::Tombstone))
                    } else {
                        let v = Bytes::copy_from_slice(cur.take(vlen as usize)?);
                        entry_crc(&mut cur)?;
                        Ok(Some(Found::Value(v)))
                    };
                }
            }
        }
    }

    /// Streams every entry in key order. Used by range scans and compaction.
    /// Owns a dup'd fd, so the iterator stays valid if the file is later
    /// deleted by compaction.
    pub fn iter(&self) -> Result<SegmentIter, SegmentError> {
        Ok(SegmentIter { cur: Cursor::new(self.file.try_clone()?, 0, self.index_off), skip_to: None })
    }

    /// Like [`Reader::iter`] but seeks to the sparse-index floor of `start`
    /// first — range scans skip everything before their lower bound.
    pub fn iter_from(&self, start: &[u8]) -> Result<SegmentIter, SegmentError> {
        let pos = self.floor(start).map(|(s, _)| s).unwrap_or(0);
        Ok(SegmentIter {
            cur: Cursor::new(self.file.try_clone()?, pos, self.index_off),
            skip_to: if pos == 0 { None } else { Some(Bytes::copy_from_slice(start)) },
        })
    }

    /// Full integrity pass: streams every entry and checks its CRC (the
    /// index and bloom CRCs already ran at `open`). The on-demand check
    /// behind a `verify` CLI/offline tool — costs one sequential read.
    pub fn verify(&self) -> Result<(), SegmentError> {
        let mut it = self.iter()?;
        while it.next_entry()?.is_some() {}
        Ok(())
    }
}

/// Bounded forward-only reader over `[pos, end)` of the file. Keeps one
/// `CHUNK` window hot so entry scans don't become a syscall-per-field.
struct Cursor<F: ReadAt> {
    file: F,
    pos: u64,
    end: u64,
    buf: Vec<u8>,
    start: usize,
    len: usize,
    /// When armed, every byte consumed via `take`/`skip`/`u32` is hashed —
    /// entry CRCs verify as the entry streams past, no second read pass.
    hasher: Option<crc32fast::Hasher>,
}

impl<F: ReadAt> Cursor<F> {
    fn new(file: F, pos: u64, end: u64) -> Self {
        Self { file, pos, end, buf: vec![0; CHUNK], start: 0, len: 0, hasher: None }
    }

    /// Bytes still buffered but unconsumed.
    fn avail(&self) -> usize {
        self.len - self.start
    }

    /// Total bytes left in the region (buffered + on disk).
    fn remaining(&self) -> u64 {
        self.avail() as u64 + (self.end - self.pos)
    }

    /// Refill the window when empty. Returns bytes available, 0 at `end`.
    fn fill(&mut self) -> io::Result<usize> {
        if self.avail() == 0 && self.pos < self.end {
            let want = (self.end - self.pos).min(CHUNK as u64) as usize;
            self.file.pread_at(&mut self.buf[..want], self.pos)?;
            self.pos += want as u64;
            self.start = 0;
            self.len = want;
        }
        Ok(self.avail())
    }

    /// Compact consumed bytes to the front, growing `buf` until `n` bytes are
    /// contiguous, then return them. `n` is always region-bounded by callers.
    fn take(&mut self, n: usize) -> Result<&[u8], SegmentError> {
        if n as u64 > self.remaining() {
            return Err(SegmentError::Corrupt("entry runs past region end"));
        }
        if self.avail() < n {
            self.buf.copy_within(self.start..self.len, 0);
            self.len = self.avail();
            self.start = 0;
            if self.buf.len() < n {
                self.buf.resize(n, 0);
            }
            while self.len < n {
                let want = (self.end - self.pos).min((n - self.len) as u64) as usize;
                self.file.pread_at(&mut self.buf[self.len..self.len + want], self.pos)?;
                self.pos += want as u64;
                self.len += want;
            }
        }
        let s = &self.buf[self.start..self.start + n];
        if let Some(h) = &mut self.hasher {
            h.update(s);
        }
        self.start += n;
        Ok(s)
    }

    fn u32(&mut self) -> Result<u32, SegmentError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn skip(&mut self, mut n: u64) -> Result<(), SegmentError> {
        if n > self.remaining() {
            return Err(SegmentError::Corrupt("skip runs past region end"));
        }
        while n > 0 {
            if self.fill()? == 0 {
                return Err(SegmentError::Corrupt("unexpected end of segment"));
            }
            let take = self.avail().min(n as usize);
            if let Some(h) = &mut self.hasher {
                h.update(&self.buf[self.start..self.start + take]);
            }
            self.start += take;
            n -= take as u64;
        }
        Ok(())
    }
}

/// Reads `(klen, vlen)` at the cursor, or `None` when the region is exhausted.
fn read_header<F: ReadAt>(cur: &mut Cursor<F>) -> Result<Option<(usize, u32)>, SegmentError> {
    if cur.fill()? == 0 {
        return Ok(None);
    }
    if cur.remaining() < 8 {
        return Err(SegmentError::Corrupt("truncated entry header"));
    }
    let klen = cur.u32()? as usize;
    let vlen = cur.u32()?;
    // Bound both lengths by what remains in the region before anyone
    // allocates against them.
    let need = 8u64 + klen as u64 + if vlen == TOMBSTONE { 0 } else { vlen as u64 } + CRC_LEN as u64;
    if need > cur.remaining() + 8 {
        return Err(SegmentError::Corrupt("entry exceeds region"));
    }
    Ok(Some((klen, vlen)))
}

/// Start one checksummed entry: arm the cursor hasher, then read the
/// `(klen, vlen)` header. `Ok(None)` at region end.
fn entry_begin<F: ReadAt>(cur: &mut Cursor<F>) -> Result<Option<(usize, u32)>, SegmentError> {
    cur.hasher = Some(crc32fast::Hasher::new());
    read_header(cur)
}

/// Read the CRC trailing the entry just consumed (the field itself is not
/// hashed — the hasher is detached first) and verify it.
fn entry_crc<F: ReadAt>(cur: &mut Cursor<F>) -> Result<(), SegmentError> {
    let Some(h) = cur.hasher.take() else { return Ok(()) };
    let want = u32::from_le_bytes(cur.take(CRC_LEN)?.try_into().unwrap());
    if h.finalize() != want {
        return Err(SegmentError::Corrupt("entry crc mismatch"));
    }
    Ok(())
}

/// Sequential entry iterator over a whole segment. Owns its file handle, so
/// it outlives the `Reader` (and survives compaction deleting the path).
pub struct SegmentIter {
    cur: Cursor<File>,
    /// When positioned via [`Reader::iter_from`], entries before this key are
    /// dropped (the sparse index can land us a few entries early).
    skip_to: Option<Bytes>,
}

impl SegmentIter {
    pub fn next_entry(&mut self) -> Result<Option<(Bytes, Option<Bytes>)>, SegmentError> {
        loop {
            let Some((klen, vlen)) = entry_begin(&mut self.cur)? else { return Ok(None) };
            let key = Bytes::copy_from_slice(self.cur.take(klen)?);
            let val = if vlen == TOMBSTONE {
                None
            } else {
                Some(Bytes::copy_from_slice(self.cur.take(vlen as usize)?))
            };
            entry_crc(&mut self.cur)?;
            if let Some(start) = &self.skip_to {
                if key.as_ref() < start.as_ref() {
                    continue;
                }
                self.skip_to = None;
            }
            return Ok(Some((key, val)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(path: &Path, entries: &[(&[u8], Option<&[u8]>)]) {
        let mut b = Builder::create(path, 2).unwrap(); // index_every=2: exercise gaps
        for (k, v) in entries {
            b.add(k, *v).unwrap();
        }
        b.finish().unwrap();
    }

    #[test]
    fn get_hit_tombstone_miss() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.sst");
        build(
            &path,
            &[
                (b"a", Some(b"1")),
                (b"b", None), // tombstone
                (b"c", Some(b"3")),
                (b"d", Some(b"4")),
                (b"e", Some(b"5")),
            ],
        );
        let r = Reader::open(&path).unwrap();
        assert_eq!(r.get(b"a").unwrap(), Some(Found::Value(Bytes::from_static(b"1"))));
        assert_eq!(r.get(b"b").unwrap(), Some(Found::Tombstone));
        assert_eq!(r.get(b"e").unwrap(), Some(Found::Value(Bytes::from_static(b"5"))));
        assert_eq!(r.get(b"zz").unwrap(), None);
        assert_eq!(r.get(b"0").unwrap(), None); // before first key
    }

    #[test]
    fn iter_yields_sorted_and_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("2.sst");
        build(&path, &[(b"a", Some(b"1")), (b"b", None), (b"c", Some(b"3"))]);
        let r = Reader::open(&path).unwrap();
        let mut it = r.iter().unwrap();
        assert_eq!(
            it.next_entry().unwrap(),
            Some((Bytes::from_static(b"a"), Some(Bytes::from_static(b"1"))))
        );
        assert_eq!(it.next_entry().unwrap(), Some((Bytes::from_static(b"b"), None)));
        assert_eq!(
            it.next_entry().unwrap(),
            Some((Bytes::from_static(b"c"), Some(Bytes::from_static(b"3"))))
        );
        assert_eq!(it.next_entry().unwrap(), None);
    }

    #[test]
    fn iter_from_seeks_near_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("3.sst");
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            (0u32..20).map(|i| (format!("k{i:03}").into_bytes(), Some(vec![i as u8]))).collect();
        let mut b = Builder::create(&path, 4).unwrap();
        for (k, v) in &entries {
            b.add(k, v.as_deref()).unwrap();
        }
        b.finish().unwrap();
        let r = Reader::open(&path).unwrap();
        let mut it = r.iter_from(b"k010").unwrap();
        let first = it.next_entry().unwrap().unwrap();
        assert_eq!(first.0, Bytes::from_static(b"k010"));
        assert_eq!(first.1, Some(Bytes::from_static(b"\x0a")));
    }

    #[test]
    fn unsorted_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = Builder::create(&dir.path().join("4.sst"), 4).unwrap();
        b.add(b"b", Some(b"1")).unwrap();
        assert!(matches!(b.add(b"a", Some(b"2")), Err(SegmentError::Unsorted)));
    }

    #[test]
    fn bloom_has_no_false_negatives() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("5.sst");
        let keys: Vec<Vec<u8>> = (0u32..1000).map(|i| format!("bloom{i:05}").into_bytes()).collect();
        let mut b = Builder::create(&path, 8).unwrap();
        for k in &keys {
            b.add(k, Some(b"v")).unwrap();
        }
        b.finish().unwrap();
        let r = Reader::open(&path).unwrap();
        // Every inserted key must be found — a false negative would return
        // None here and silently lose data.
        for k in &keys {
            assert!(r.get(k).unwrap().is_some(), "false negative on {k:?}");
        }
        // And absent keys are cheaply rejected (mostly — FPs just fall through).
        assert_eq!(r.get(b"not-there").unwrap(), None);
    }

    fn flip(path: &Path, off: u64) {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = OpenOptions::new().read(true).write(true).open(path).unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        let mut b = [0u8; 1];
        std::io::Read::read_exact(&mut f, &mut b).unwrap();
        b[0] ^= 0xff;
        f.seek(SeekFrom::Start(off)).unwrap();
        f.write_all(&b).unwrap();
    }

    #[test]
    fn corrupt_entry_detected_on_read_and_verify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("6.sst");
        // index_every=2 → gap0 = entries a,b (a@0, b@14); gap1 = c@27.
        build(&path, &[(b"a", Some(b"1")), (b"b", None), (b"c", Some(b"3"))]);
        // Byte 9 = entry a's value byte ("1" at klen|vlen|key).
        flip(&path, 9);
        let r = Reader::open(&path).unwrap();
        assert!(r.get(b"a").is_err(), "corrupt entry passed crc");
        assert!(r.verify().is_err(), "verify missed corrupt entry");
        // An uncorrupted gap still reads fine — checksums are per-entry.
        assert_eq!(r.get(b"c").unwrap(), Some(Found::Value(Bytes::from_static(b"3"))));
    }

    #[test]
    fn corrupt_index_and_bloom_detected_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("7.sst");
        build(&path, &[(b"a", Some(b"1")), (b"b", None), (b"c", Some(b"3"))]);
        let len = std::fs::metadata(&path).unwrap().len();
        // index_off = 41 (3 entries of 14/13/14 bytes) → first index byte.
        flip(&path, 41);
        assert!(Reader::open(&path).is_err(), "corrupt index passed crc");

        // Rebuild clean, then flip a bloom byte (just before the footer).
        build(&path, &[(b"a", Some(b"1")), (b"b", None), (b"c", Some(b"3"))]);
        flip(&path, len - FOOTER_LEN - 1);
        assert!(Reader::open(&path).is_err(), "corrupt bloom passed crc");
    }
}
