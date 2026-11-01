//! Deterministic workload generation. Every op stream is a pure function of
//! (scenario, seed) so runs are byte-for-byte repeatable across engines and
//! repetitions — the same seed always produces the same keys and values.

/// Same xorshift PRNG the repo's existing benches use.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1)) // xorshift is degenerate at 0
    }
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    /// Uniform in [0, n). Rejection-free modulo — fine for benches.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// `key{i:013}` zero-padded/truncated to `size` bytes (min 16 so the id
/// always fits). Deterministic and lexicographically sortable = insertion
/// order, which keeps sequential vs random workloads distinct.
pub fn key(i: u64, size: usize) -> Vec<u8> {
    let mut k = format!("key{i:013}").into_bytes();
    if k.len() < size {
        k.resize(size, b'_');
    }
    k.truncate(size);
    k
}

/// Value bytes derived from the key index — verifiable after recovery and
/// not compressible into nothing (low entropy per byte, varied across keys).
pub fn value(i: u64, size: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(size);
    let mut x = i.wrapping_mul(0x9e3779b97f4a7c15) | 1;
    while v.len() < size {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(size);
    v
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
    Get(Vec<u8>),
    /// Vec of put ops committed atomically.
    Batch(Vec<(Vec<u8>, Vec<u8>)>),
    Scan(Vec<u8>, usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Work {
    FillSeq,
    FillRand,
    ReadRand,
    DeleteRand,
    Batch128,
    Scan1000,
    /// 50% reads over the preload range, 50% random puts.
    Mixed5050,
}

impl Work {
    pub fn all() -> &'static [Work] {
        &[
            Work::FillSeq,
            Work::FillRand,
            Work::ReadRand,
            Work::DeleteRand,
            Work::Batch128,
            Work::Scan1000,
            Work::Mixed5050,
        ]
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FillSeq => "fill_seq",
            Self::FillRand => "fill_rand",
            Self::ReadRand => "read_rand",
            Self::DeleteRand => "delete_rand",
            Self::Batch128 => "batch_128",
            Self::Scan1000 => "scan_1000",
            Self::Mixed5050 => "mixed_50_50",
        }
    }
    /// Whether the workload touches disk durability (used to scale ops
    /// down for fsync-bound `Strong` runs).
    pub fn writes(&self) -> bool {
        matches!(
            self,
            Self::FillSeq | Self::FillRand | Self::DeleteRand | Self::Batch128 | Self::Mixed5050
        )
    }
}

/// Materialize `n` ops for `work`. `preload` is the number of keys assumed
/// already in the db (reads/dels/scans draw from it).
pub fn stream(work: Work, n: usize, key_size: usize, value_size: usize, preload: u64, seed: u64) -> Vec<Op> {
    let mut rng = Rng::new(seed);
    let space = preload.max(1);
    (0..n)
        .map(|i| {
            let i = i as u64;
            match work {
                Work::FillSeq => Op::Put(key(i, key_size), value(i, value_size)),
                Work::FillRand => {
                    let k = rng.below(preload.max(n as u64 * 4));
                    Op::Put(key(k, key_size), value(k, value_size))
                }
                Work::ReadRand => Op::Get(key(rng.below(space), key_size)),
                Work::DeleteRand => Op::Del(key(rng.below(space), key_size)),
                Work::Batch128 => {
                    let ops = (0..128)
                        .map(|j| {
                            let k = i * 128 + j;
                            (key(k, key_size), value(k, value_size))
                        })
                        .collect();
                    Op::Batch(ops)
                }
                Work::Scan1000 => {
                    let lo = rng.below(space);
                    Op::Scan(key(lo, key_size), 1000)
                }
                Work::Mixed5050 => {
                    if rng.next() & 1 == 0 {
                        Op::Get(key(rng.below(space), key_size))
                    } else {
                        let k = preload + rng.below(n as u64 * 4);
                        Op::Put(key(k, key_size), value(k, value_size))
                    }
                }
            }
        })
        .collect()
}
