//! Engine adapters behind a minimal `Kv` trait so every database runs the
//! exact same generated op stream. Each adapter maps `Durability` onto the
//! engine's equivalent sync setting — comparisons are only ever made at the
//! same level.

use std::error::Error;
use std::path::Path;
use std::sync::Mutex;

pub type Res<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Cross-engine durability levels — never compare across levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Durability {
    /// fsync before the write is acknowledged (rexodb `Always`, SQLite
    /// `synchronous=FULL`, LMDB default, redb `Immediate`).
    Strong,
    /// fsync on a timer/checkpoint (rexodb `EveryMs`, SQLite `NORMAL`,
    /// LMDB `NO_META_SYNC`, redb `Eventual`).
    Periodic,
    /// No fsync at all (rexodb `Never`, SQLite `OFF`, LMDB `NO_SYNC`,
    /// redb `None`).
    Weak,
}

impl Durability {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::Periodic => "periodic",
            Self::Weak => "weak",
        }
    }
}

pub enum BatchOp {
    Put(Vec<u8>, Vec<u8>),
}

pub trait Kv: Send + Sync {
    fn get(&self, k: &[u8]) -> Res<Option<Vec<u8>>>;
    fn put(&self, k: &[u8], v: &[u8]) -> Res<()>;
    fn del(&self, k: &[u8]) -> Res<()>;
    fn batch(&self, ops: &[BatchOp]) -> Res<()>;
    /// Ordered scan of up to `limit` entries starting at `lo` (inclusive).
    /// Returns entries consumed.
    fn scan(&self, lo: &[u8], limit: usize) -> Res<usize>;
    /// Durability barrier — everything acknowledged before is on disk.
    fn sync(&self) -> Res<()>;
}

pub fn open(name: &str, dir: &Path, dur: Durability, inmem: bool) -> Res<Box<dyn Kv>> {
    match name {
        "rexodb" => Ok(Box::new(Rexo::open(dir, dur))),
        "sqlite" => Sqlite::open(dir, dur, inmem).map(|d| Box::new(d) as _),
        "lmdb" => Lmdb::open(dir, dur).map(|d| Box::new(d) as _),
        "redb" => Redb::open(dir, dur, inmem).map(|d| Box::new(d) as _),
        "inmem" => Ok(Box::new(InMem::open())),
        #[cfg(feature = "rocksdb")]
        "rocksdb" => Rocks::open(dir, dur).map(|d| Box::new(d) as _),
        other => Err(format!("unknown db '{other}'").into()),
    }
}

// ---------------------------------------------------------------- in-memory floor

/// Raw `BTreeMap` reference — the cost floor every real store is measured
/// against: one lock + ordered-map ops, nothing else. HFT systems are
/// essentially this plus a journal, so a durable engine is honestly read
/// as "how close to the map does it get".
struct InMem {
    map: std::sync::RwLock<std::collections::BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl InMem {
    fn open() -> Self {
        Self { map: std::sync::RwLock::new(std::collections::BTreeMap::new()) }
    }
}

impl Kv for InMem {
    fn get(&self, k: &[u8]) -> Res<Option<Vec<u8>>> {
        Ok(self.map.read().unwrap().get(k).cloned())
    }
    fn put(&self, k: &[u8], v: &[u8]) -> Res<()> {
        self.map.write().unwrap().insert(k.to_vec(), v.to_vec());
        Ok(())
    }
    fn del(&self, k: &[u8]) -> Res<()> {
        self.map.write().unwrap().remove(k);
        Ok(())
    }
    fn batch(&self, ops: &[BatchOp]) -> Res<()> {
        let mut m = self.map.write().unwrap();
        for op in ops {
            let BatchOp::Put(k, v) = op;
            m.insert(k.clone(), v.clone());
        }
        Ok(())
    }
    fn scan(&self, lo: &[u8], limit: usize) -> Res<usize> {
        Ok(self.map.read().unwrap().range(lo.to_vec()..).take(limit).count())
    }
    fn sync(&self) -> Res<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------- rexodb

struct Rexo {
    db: rexodb::Db,
    /// REXO_ASYNC=1 — route puts through `put_async` (pipeline fast path).
    async_writes: bool,
}

impl Rexo {
    fn open(dir: &Path, dur: Durability) -> Self {
        let mut c = rexodb::Config::new(dir.join("db"));
        c.sync = match dur {
            Durability::Strong => rexodb::SyncPolicy::Always,
            Durability::Periodic => rexodb::SyncPolicy::EveryMs(1),
            Durability::Weak => rexodb::SyncPolicy::Never,
        };
        // A/B the write paths: REXO_WRITE_PATH=pipeline opts in;
        // default matches the shipped Config (Direct — sync-put winner).
        c.write_path = match std::env::var("REXO_WRITE_PATH").as_deref() {
            Ok("pipeline") => rexodb::WritePath::Pipeline,
            _ => rexodb::WritePath::Direct,
        };
        // REXO_PIN="4,5" — pin pipeline consumer→core 4, fsync→core 5.
        if let Ok(cores) = std::env::var("REXO_PIN") {
            c.pin_cores = cores.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        }
        // REXO_MEMTABLE / REXO_COMPACT — shrink rotation/compaction
        // thresholds so short runs exercise flushes and merges.
        if let Ok(v) = std::env::var("REXO_MEMTABLE") {
            c.memtable_bytes = v.parse().unwrap();
        }
        if let Ok(v) = std::env::var("REXO_COMPACT") {
            c.compact_at = v.parse().unwrap();
        }
        c.full_merge = std::env::var("REXO_FULLMERGE").as_deref() == Ok("1");
        let async_writes = std::env::var("REXO_ASYNC").as_deref() == Ok("1");
        Self { db: rexodb::Db::open(c).unwrap(), async_writes }
    }
}

impl Kv for Rexo {
    fn get(&self, k: &[u8]) -> Res<Option<Vec<u8>>> {
        Ok(self.db.get(k)?.map(|v| v.to_vec()))
    }
    fn put(&self, k: &[u8], v: &[u8]) -> Res<()> {
        if self.async_writes {
            self.db.put_async(k, v)?;
            return Ok(());
        }
        Ok(self.db.put(k, v)?)
    }
    fn del(&self, k: &[u8]) -> Res<()> {
        Ok(self.db.delete(k)?)
    }
    fn batch(&self, ops: &[BatchOp]) -> Res<()> {
        let mut b = rexodb::WriteBatch::new();
        for op in ops {
            match op {
                BatchOp::Put(k, v) => b.put(k, v),
            };
        }
        Ok(self.db.write(&b)?)
    }
    fn scan(&self, lo: &[u8], limit: usize) -> Res<usize> {
        let it = self.db.range(lo.to_vec()..)?;
        let mut n = 0;
        for e in it {
            e?;
            n += 1;
            if n >= limit {
                break;
            }
        }
        Ok(n)
    }
    fn sync(&self) -> Res<()> {
        Ok(self.db.sync()?)
    }
}

// ---------------------------------------------------------------- sqlite

/// Small connection pool: one shared in-memory database (shared cache URI)
/// or one file-backed WAL db. Popping per op keeps reads concurrent under
/// WAL like real deployments, instead of serializing on one Mutex'd conn.
struct Sqlite {
    conns: Mutex<Vec<rusqlite::Connection>>,
    uri: String,
    dur: Durability,
    inmem: bool,
    /// Keeps a shared-cache in-memory db alive while pooled conns cycle.
    /// Mutex'd because `Connection` is Send but not Sync.
    _anchor: Mutex<Option<rusqlite::Connection>>,
}

impl Sqlite {
    fn open(dir: &Path, dur: Durability, inmem: bool) -> Res<Self> {
        let uri = if inmem {
            "file:rexobench-mem?mode=memory&cache=shared".to_string()
        } else {
            dir.join("sqlite.db").to_string_lossy().into_owned()
        };
        let mk = |uri: &str, dur: Durability, inmem: bool| -> Res<rusqlite::Connection> {
            let c = rusqlite::Connection::open_with_flags(
                uri,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_FULL_MUTEX,
            )?;
            if inmem {
                c.pragma_update(None, "journal_mode", "MEMORY")?;
            } else {
                c.pragma_update(None, "journal_mode", "WAL")?;
            }
            c.pragma_update(
                None,
                "synchronous",
                match dur {
                    Durability::Strong => "FULL",
                    Durability::Periodic => "NORMAL",
                    Durability::Weak => "OFF",
                },
            )?;
            c.execute_batch(
                "CREATE TABLE IF NOT EXISTS kv(k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID",
            )?;
            Ok(c)
        };
        let anchor = inmem.then(|| mk(&uri, dur, true)).transpose()?;
        let first = mk(&uri, dur, inmem)?;
        Ok(Self { conns: Mutex::new(vec![first]), uri, dur, inmem, _anchor: Mutex::new(anchor) })
    }

    fn conn(&self) -> Res<rusqlite::Connection> {
        if let Some(c) = self.conns.lock().unwrap().pop() {
            return Ok(c);
        }
        Sqlite::mk_conn(&self.uri, self.dur, self.inmem)
    }

    fn mk_conn(uri: &str, dur: Durability, inmem: bool) -> Res<rusqlite::Connection> {
        let c = rusqlite::Connection::open_with_flags(
            uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_URI
                | rusqlite::OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )?;
        if !inmem {
            c.pragma_update(None, "journal_mode", "WAL")?;
        }
        c.pragma_update(
            None,
            "synchronous",
            match dur {
                Durability::Strong => "FULL",
                Durability::Periodic => "NORMAL",
                Durability::Weak => "OFF",
            },
        )?;
        Ok(c)
    }

    fn back(&self, c: rusqlite::Connection) {
        self.conns.lock().unwrap().push(c);
    }
}

impl Kv for Sqlite {
    fn get(&self, k: &[u8]) -> Res<Option<Vec<u8>>> {
        let c = self.conn()?;
        let r = c
            .query_row("SELECT v FROM kv WHERE k=?1", [k], |r| r.get::<_, Vec<u8>>(0))
            .optional()?;
        self.back(c);
        Ok(r)
    }
    fn put(&self, k: &[u8], v: &[u8]) -> Res<()> {
        let c = self.conn()?;
        c.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?1,?2)", [k, v])?;
        self.back(c);
        Ok(())
    }
    fn del(&self, k: &[u8]) -> Res<()> {
        let c = self.conn()?;
        c.execute("DELETE FROM kv WHERE k=?1", [k])?;
        self.back(c);
        Ok(())
    }
    fn batch(&self, ops: &[BatchOp]) -> Res<()> {
        let c = self.conn()?;
        c.execute_batch("BEGIN IMMEDIATE")?;
        let r = (|| -> Res<()> {
            for op in ops {
                match op {
                    BatchOp::Put(k, v) => {
                        c.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?1,?2)", [k, v])?;
                    }
                }
            }
            Ok(())
        })();
        match r {
            Ok(()) => c.execute_batch("COMMIT")?,
            Err(e) => {
                let _ = c.execute_batch("ROLLBACK");
                self.back(c);
                return Err(e);
            }
        }
        self.back(c);
        Ok(())
    }
    fn scan(&self, lo: &[u8], limit: usize) -> Res<usize> {
        let c = self.conn()?;
        let mut st = c.prepare("SELECT k,v FROM kv WHERE k>=?1 ORDER BY k LIMIT ?2")?;
        let mut rows = st.query(rusqlite::params![lo, limit as i64])?;
        let mut n = 0;
        while rows.next()?.is_some() {
            n += 1;
        }
        drop(rows);
        drop(st);
        self.back(c);
        Ok(n)
    }
    fn sync(&self) -> Res<()> {
        let c = self.conn()?;
        // Force a WAL checkpoint — the closest SQLite equivalent of an
        // explicit "make it durable now" barrier.
        c.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
        self.back(c);
        Ok(())
    }
}

use rusqlite::OptionalExtension;

// ---------------------------------------------------------------- lmdb (heed)

struct Lmdb {
    env: heed::Env,
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    wlock: Mutex<()>, // LMDB is single-writer; serialize our write txns
}

impl Lmdb {
    fn open(dir: &Path, dur: Durability) -> Res<Self> {
        std::fs::create_dir_all(dir.join("lmdb"))?;
        let mut opts = heed::EnvOpenOptions::new();
        opts.map_size(8 << 30).max_dbs(4);
        unsafe {
            match dur {
                Durability::Strong => {}
                Durability::Periodic => {
                    opts.flags(heed::EnvFlags::NO_META_SYNC);
                }
                Durability::Weak => {
                    opts.flags(heed::EnvFlags::NO_SYNC | heed::EnvFlags::NO_META_SYNC);
                }
            }
        }
        let env = unsafe { opts.open(dir.join("lmdb")) }?;
        let mut w = env.write_txn()?;
        let db = env.create_database(&mut w, Some("kv"))?;
        w.commit()?;
        Ok(Self { env, db, wlock: Mutex::new(()) })
    }
}

impl Kv for Lmdb {
    fn get(&self, k: &[u8]) -> Res<Option<Vec<u8>>> {
        let r = self.env.read_txn()?;
        Ok(self.db.get(&r, k)?.map(|v| v.to_vec()))
    }
    fn put(&self, k: &[u8], v: &[u8]) -> Res<()> {
        let _g = self.wlock.lock().unwrap();
        let mut w = self.env.write_txn()?;
        self.db.put(&mut w, k, v)?;
        w.commit()?;
        Ok(())
    }
    fn del(&self, k: &[u8]) -> Res<()> {
        let _g = self.wlock.lock().unwrap();
        let mut w = self.env.write_txn()?;
        self.db.delete(&mut w, k)?;
        w.commit()?;
        Ok(())
    }
    fn batch(&self, ops: &[BatchOp]) -> Res<()> {
        let _g = self.wlock.lock().unwrap();
        let mut w = self.env.write_txn()?;
        for op in ops {
            match op {
                BatchOp::Put(k, v) => self.db.put(&mut w, k, v)?,
            };
        }
        w.commit()?;
        Ok(())
    }
    fn scan(&self, lo: &[u8], limit: usize) -> Res<usize> {
        let r = self.env.read_txn()?;
        let mut n = 0;
        // (Bound<&[u8]>, Bound<&[u8]>) implements RangeBounds<[u8]>.
        let range = (std::ops::Bound::Included(lo), std::ops::Bound::Unbounded);
        for e in self.db.range(&r, &range)? {
            e?;
            n += 1;
            if n >= limit {
                break;
            }
        }
        Ok(n)
    }
    fn sync(&self) -> Res<()> {
        Ok(self.env.force_sync()?)
    }
}

// ---------------------------------------------------------------- redb

struct Redb {
    db: redb::Database,
    dur: Durability,
}

const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("kv");

impl Redb {
    fn open(dir: &Path, dur: Durability, inmem: bool) -> Res<Self> {
        let db = if inmem {
            redb::Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?
        } else {
            redb::Database::create(dir.join("db.redb"))?
        };
        Ok(Self { db, dur })
    }
    fn durability(&self) -> redb::Durability {
        match self.dur {
            Durability::Strong => redb::Durability::Immediate,
            Durability::Periodic => redb::Durability::Eventual,
            Durability::Weak => redb::Durability::None,
        }
    }
}

impl Kv for Redb {
    fn get(&self, k: &[u8]) -> Res<Option<Vec<u8>>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(TABLE)?;
        Ok(t.get(k)?.map(|v| v.value().to_vec()))
    }
    fn put(&self, k: &[u8], v: &[u8]) -> Res<()> {
        let mut w = self.db.begin_write()?;
        w.set_durability(self.durability());
        {
            let mut t = w.open_table(TABLE)?;
            t.insert(k, v)?;
        }
        w.commit()?;
        Ok(())
    }
    fn del(&self, k: &[u8]) -> Res<()> {
        let mut w = self.db.begin_write()?;
        w.set_durability(self.durability());
        {
            let mut t = w.open_table(TABLE)?;
            t.remove(k)?;
        }
        w.commit()?;
        Ok(())
    }
    fn batch(&self, ops: &[BatchOp]) -> Res<()> {
        let mut w = self.db.begin_write()?;
        w.set_durability(self.durability());
        {
            let mut t = w.open_table(TABLE)?;
            for op in ops {
                match op {
                    BatchOp::Put(k, v) => t.insert(k.as_slice(), v.as_slice())?,
                };
            }
        }
        w.commit()?;
        Ok(())
    }
    fn scan(&self, lo: &[u8], limit: usize) -> Res<usize> {
        let r = self.db.begin_read()?;
        let t = r.open_table(TABLE)?;
        let mut n = 0;
        for e in t.range(lo..)? {
            e?;
            n += 1;
            if n >= limit {
                break;
            }
        }
        Ok(n)
    }
    fn sync(&self) -> Res<()> {
        // redb has no explicit fsync barrier API; an empty Immediate commit
        // is the closest equivalent.
        let mut w = self.db.begin_write()?;
        w.set_durability(redb::Durability::Immediate);
        w.commit()?;
        Ok(())
    }
}

// ---------------------------------------------------------------- rocksdb (optional)

#[cfg(feature = "rocksdb")]
struct Rocks {
    db: rocksdb::DB,
    dur: Durability,
}

#[cfg(feature = "rocksdb")]
impl Rocks {
    fn open(dir: &Path, dur: Durability) -> Res<Self> {
        let mut o = rocksdb::Options::default();
        o.create_if_missing(true);
        Ok(Self { db: rocksdb::DB::open(&o, dir.join("rocksdb"))?, dur })
    }
    fn wopts(&self) -> rocksdb::WriteOptions {
        let mut w = rocksdb::WriteOptions::default();
        w.set_sync(matches!(self.dur, Durability::Strong));
        w
    }
}

#[cfg(feature = "rocksdb")]
impl Kv for Rocks {
    fn get(&self, k: &[u8]) -> Res<Option<Vec<u8>>> {
        Ok(self.db.get(k)?)
    }
    fn put(&self, k: &[u8], v: &[u8]) -> Res<()> {
        Ok(self.db.put_opt(k, v, &self.wopts())?)
    }
    fn del(&self, k: &[u8]) -> Res<()> {
        Ok(self.db.delete_opt(k, &self.wopts())?)
    }
    fn batch(&self, ops: &[BatchOp]) -> Res<()> {
        let mut b = rocksdb::WriteBatch::default();
        for op in ops {
            match op {
                BatchOp::Put(k, v) => b.put(k, v),
            }
        }
        Ok(self.db.write_opt(b, &self.wopts())?)
    }
    fn scan(&self, lo: &[u8], limit: usize) -> Res<usize> {
        let mut it = self.db.raw_iterator();
        it.seek(lo);
        let mut n = 0;
        while it.valid() && n < limit {
            it.next();
            n += 1;
        }
        Ok(n)
    }
    fn sync(&self) -> Res<()> {
        Ok(self.db.flush_wal(true)?)
    }
}
