//! Model-based property tests. Random op sequences are applied to a
//! `BTreeMap` oracle and to `Db` — on BOTH write paths at once. Every
//! observable result (`get`, `get_many`, `range`) must match the model,
//! which simultaneously proves Direct and Pipeline are observationally
//! equivalent, including across flush/compact/reopen boundaries.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use proptest::prelude::*;
use rexodb::{Config, Db, WriteBatch, WritePath};
use tempfile::TempDir;

#[derive(Debug, Clone)]
enum Act {
    Put(u8, u16),
    Del(u8),
    /// Two puts + a delete committed as one atomic WAL record.
    Batch(u8, u16, u8, u16, u8),
    Get(u8),
    GetMany([u8; 4]),
    Range(u8, u8),
    Flush,
    Compact,
    Sync,
    Reopen,
}

fn act() -> impl Strategy<Value = Act> {
    prop_oneof![
        5 => (any::<u8>(), any::<u16>()).prop_map(|(k, v)| Act::Put(k, v)),
        2 => any::<u8>().prop_map(Act::Del),
        1 => (any::<u8>(), any::<u16>(), any::<u8>(), any::<u16>(), any::<u8>())
            .prop_map(|(a, av, b, bv, c)| Act::Batch(a, av, b, bv, c)),
        4 => any::<u8>().prop_map(Act::Get),
        1 => any::<[u8; 4]>().prop_map(Act::GetMany),
        2 => (any::<u8>(), any::<u8>()).prop_map(|(a, b)| Act::Range(a, b)),
        1 => Just(Act::Flush),
        1 => Just(Act::Compact),
        1 => Just(Act::Sync),
        1 => Just(Act::Reopen),
    ]
}

fn key(k: u8) -> Vec<u8> {
    format!("k{k:03}").into_bytes()
}

fn val(v: u16) -> Vec<u8> {
    format!("v{v:05}").into_bytes()
}

/// One open `Db` under test. `reopen` drops and re-opens on the same dir —
/// the recovery path is part of the model surface.
struct Rig {
    db: Option<Db>,
    path: PathBuf,
    write_path: WritePath,
    _dir: TempDir,
}

impl Rig {
    fn cfg(path: &Path, write_path: WritePath) -> Config {
        let mut c = Config::new(path);
        // Small memtable/ring → constant rotation, flushing, compaction —
        // the interesting state transitions stay hot.
        c.memtable_bytes = 2 << 10;
        c.compact_at = 3;
        c.ring_capacity = 32;
        c.write_path = write_path;
        c
    }

    fn open(write_path: WritePath) -> Self {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("db");
        let db = Db::open(Self::cfg(&path, write_path)).unwrap();
        Self { db: Some(db), path, write_path, _dir: dir }
    }

    fn db(&self) -> &Db {
        self.db.as_ref().expect("rig is open")
    }

    fn reopen(&mut self) {
        drop(self.db.take());
        self.db = Some(Db::open(Self::cfg(&self.path, self.write_path)).unwrap());
    }
}

fn run(ops: &[Act]) {
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut rigs = [Rig::open(WritePath::Direct), Rig::open(WritePath::Pipeline)];
    for (i, a) in ops.iter().enumerate() {
        match *a {
            Act::Put(k, v) => {
                for r in &rigs {
                    r.db().put(key(k), val(v)).unwrap();
                }
                model.insert(key(k), val(v));
            }
            Act::Del(k) => {
                for r in &rigs {
                    r.db().delete(key(k)).unwrap();
                }
                model.remove(&key(k));
            }
            Act::Batch(a, av, b, bv, c) => {
                let mut batch = WriteBatch::new();
                batch.put(key(a), val(av)).put(key(b), val(bv)).delete(key(c));
                for r in &rigs {
                    r.db().write(&batch).unwrap();
                }
                model.insert(key(a), val(av));
                model.insert(key(b), val(bv));
                model.remove(&key(c));
            }
            Act::Get(k) => {
                let want = model.get(&key(k)).cloned();
                for r in &rigs {
                    let got = r.db().get(key(k)).unwrap();
                    assert_eq!(
                        got.as_deref(),
                        want.as_deref(),
                        "op {i} {a:?}: {:?} diverged",
                        r.write_path
                    );
                }
            }
            Act::GetMany(ks) => {
                let keys: Vec<Vec<u8>> = ks.iter().map(|&k| key(k)).collect();
                let want: Vec<Option<Vec<u8>>> = keys.iter().map(|k| model.get(k).cloned()).collect();
                for r in &rigs {
                    let got = r.db().get_many(&keys).unwrap();
                    let got: Vec<Option<Vec<u8>>> = got.iter().map(|v| v.as_ref().map(|b| b.to_vec())).collect();
                    assert_eq!(got, want, "op {i} {a:?}: {:?} diverged", r.write_path);
                }
            }
            Act::Range(a, b) => {
                let (lo, hi) = (key(a.min(b)), key(a.max(b)));
                let want: Vec<(Vec<u8>, Vec<u8>)> =
                    model.range(lo.clone()..=hi.clone()).map(|(k, v)| (k.clone(), v.clone())).collect();
                for r in &rigs {
                    let got: Vec<(Vec<u8>, Vec<u8>)> = r
                        .db()
                        .range(lo.as_slice()..=hi.as_slice())
                        .unwrap()
                        .map(|e| e.map(|(k, v)| (k.to_vec(), v.to_vec())))
                        .collect::<Result<_, _>>()
                        .unwrap();
                    assert_eq!(got, want, "op {i} {a:?}: {:?} diverged", r.write_path);
                }
            }
            Act::Flush => {
                for r in &rigs {
                    r.db().flush().unwrap();
                }
            }
            Act::Compact => {
                for r in &rigs {
                    r.db().compact().unwrap();
                }
            }
            Act::Sync => {
                for r in &rigs {
                    r.db().sync().unwrap();
                }
            }
            Act::Reopen => {
                for r in &mut rigs {
                    r.reopen();
                }
            }
        }
    }
    // Final state must agree too — a divergence that only shows after the
    // last op would otherwise slip through.
    for r in &rigs {
        for (k, v) in &model {
            assert_eq!(
                r.db().get(k).unwrap().as_deref(),
                Some(v.as_slice()),
                "final state: {:?} lost {k:?}",
                r.write_path
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn db_matches_model_on_both_write_paths(ops in prop::collection::vec(act(), 1..100)) {
        run(&ops);
    }
}

/// Fixed-seed variant — same harness, deterministic coverage in CI where
/// proptest's random cases can't be replayed by seed file.
#[test]
fn deterministic_op_sequence_matches_model() {
    let mut x = 0x9E3779B97F4A7C15u64;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let ops: Vec<Act> = (0..300)
        .map(|_| match next() % 12 {
            0..=4 => Act::Put((next() % 64) as u8, next() as u16),
            5 => Act::Del((next() % 64) as u8),
            6 => Act::Get((next() % 64) as u8),
            7 => Act::Range((next() % 64) as u8, (next() % 64) as u8),
            8 => Act::Flush,
            9 => Act::Reopen,
            10 => Act::Compact,
            _ => Act::Batch(
                (next() % 64) as u8,
                next() as u16,
                (next() % 64) as u8,
                next() as u16,
                (next() % 64) as u8,
            ),
        })
        .collect();
    run(&ops);
}
