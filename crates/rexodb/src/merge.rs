//! K-way merge over sorted entry streams (memtable snapshots + segment
//! iterators). Source index is the recency rank — lower index wins ties, so
//! `sources[0]` must be the newest layer. Tombstones are yielded, not dropped:
//! range scans skip them, compaction drops them once no older layer remains.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use bytes::Bytes;
use rexodb_segment::SegmentIter;

use crate::Error;

/// One sorted `(key, Option<value>)` stream.
pub enum Source {
    /// Materialized memtable/imm snapshot, already in key order.
    Mem(std::vec::IntoIter<(Bytes, Option<Bytes>)>),
    /// Streaming segment iterator.
    Seg(SegmentIter),
}

impl Source {
    fn next_entry(&mut self) -> Result<Option<(Bytes, Option<Bytes>)>, Error> {
        match self {
            Source::Mem(it) => Ok(it.next()),
            Source::Seg(it) => Ok(it.next_entry()?),
        }
    }
}

/// Heap entry: `(key, source_idx, value)` — the tuple's ordering makes the
/// smallest key pop first and, for duplicate keys, the *newest* source
/// (lowest index) pop before older copies.
type HeapItem = Reverse<(Bytes, usize, Option<Bytes>)>;

pub struct MergeIter {
    heap: BinaryHeap<HeapItem>,
    sources: Vec<Source>,
    /// Last emitted key — repeats of it come from older sources and are
    /// shadowed, so they're skipped.
    last: Option<Bytes>,
}

impl MergeIter {
    /// Sources in newest-first order.
    pub fn new(mut sources: Vec<Source>) -> Result<Self, Error> {
        let mut heap = BinaryHeap::with_capacity(sources.len());
        for (idx, src) in sources.iter_mut().enumerate() {
            if let Some((k, v)) = src.next_entry()? {
                heap.push(Reverse((k, idx, v)));
            }
        }
        Ok(Self { heap, sources, last: None })
    }

    /// Next resolved `(key, value)` pair — `value == None` is a tombstone that
    /// still shadowed every older copy of the key.
    pub fn next_entry(&mut self) -> Result<Option<(Bytes, Option<Bytes>)>, Error> {
        loop {
            let Some(Reverse((key, src, val))) = self.heap.pop() else { return Ok(None) };
            if let Some((k, v)) = self.sources[src].next_entry()? {
                self.heap.push(Reverse((k, src, v)));
            }
            if self.last.as_deref() == Some(key.as_ref()) {
                continue;
            }
            self.last = Some(key.clone());
            return Ok(Some((key, val)));
        }
    }
}
