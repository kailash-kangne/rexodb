use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;
use rexodb_wal::Op;

/// Amortized BTreeMap node overhead for size accounting — deliberately
/// approximate; the byte budget just needs to be in the right ballpark.
const ENTRY_OVERHEAD: usize = 64;

/// Ordered in-memory table. `None` value = tombstone (a `delete` that must
/// shadow older segments until compaction drops it).
#[derive(Default)]
pub struct Memtable {
    map: BTreeMap<Bytes, Option<Bytes>>,
    /// Rough live-byte estimate driving the flush trigger.
    pub bytes: usize,
}

impl Memtable {
    pub fn apply(&mut self, op: &Op) {
        match op {
            Op::Put(k, v) => self.insert(k.clone(), Some(v.clone())),
            Op::Delete(k) => self.insert(k.clone(), None),
        }
    }

    fn insert(&mut self, key: Bytes, val: Option<Bytes>) {
        let klen = key.len();
        let new_size = ENTRY_OVERHEAD + klen + val.as_ref().map_or(0, |v| v.len());
        match self.map.insert(key, val) {
            Some(old) => {
                let old_size = ENTRY_OVERHEAD + klen + old.as_ref().map_or(0, |v| v.len());
                self.bytes = self.bytes.saturating_sub(old_size).saturating_add(new_size);
            }
            None => self.bytes = self.bytes.saturating_add(new_size),
        }
    }

    /// `Some(Some(v))` hit, `Some(None)` tombstone, `None` not present.
    pub fn lookup(&self, key: &[u8]) -> Option<Option<Bytes>> {
        self.map.get(key).cloned()
    }

    /// Owned snapshot of the entries inside `bounds` — `Bytes` clones are
    /// refcount bumps, so this is cheap even for large ranges.
    pub fn snapshot(&self, bounds: (Bound<&[u8]>, Bound<&[u8]>)) -> Vec<(Bytes, Option<Bytes>)> {
        self.map.range::<[u8], _>(bounds).map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Option<Bytes>)> {
        self.map.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overwriting_same_key_does_not_inflate_bytes() {
        let mut m = Memtable::default();
        m.apply(&Op::Put(Bytes::from_static(b"k"), Bytes::from_static(b"value")));
        let after_first = m.bytes;
        for _ in 0..1000 {
            m.apply(&Op::Put(Bytes::from_static(b"k"), Bytes::from_static(b"value")));
        }
        assert_eq!(m.bytes, after_first);
    }
}
