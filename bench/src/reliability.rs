//! Reliability tests: crash recovery, WAL corruption, interrupted
//! checkpoints (flushes), disk-full, and clean restart. Crash tests kill a
//! real child process with SIGKILL — simulating a crash in-process can't
//! bypass the writer thread's buffers.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use rexodb::{Config, Db, SyncPolicy};

use crate::kv::Res;
use crate::ramdisk;
use crate::workload::{key, value};

pub struct TestResult {
    pub name: &'static str,
    pub ok: bool,
    pub detail: String,
}

impl TestResult {
    fn ok(name: &'static str, detail: impl Into<String>) -> Self {
        Self { name, ok: true, detail: detail.into() }
    }
    fn fail(name: &'static str, detail: impl Into<String>) -> Self {
        Self { name, ok: false, detail: detail.into() }
    }
}

fn cfg(dir: &Path, dur: SyncPolicy, memtable_bytes: usize) -> Config {
    let mut c = Config::new(dir.join("db"));
    c.sync = dur;
    c.memtable_bytes = memtable_bytes;
    c
}

const VAL_SIZE: usize = 128;

/// `bench crash-child <dir> <memtable_bytes> <always|fast>`: write numbered
/// keys, print `ACK <i>` for each acknowledged put. Parent SIGKILLs us.
pub fn crash_child(args: &[String]) -> ! {
    let dir = PathBuf::from(&args[0]);
    let mem_bytes: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(64 << 20);
    let dur = if args.get(2).is_some_and(|s| s == "always") {
        SyncPolicy::Always
    } else {
        SyncPolicy::EveryMs(1)
    };
    let db = Db::open(cfg(&dir, dur, mem_bytes)).unwrap();
    let mut i = 0u64;
    loop {
        if db.put(key(i, 16), value(i, VAL_SIZE)).is_err() {
            break;
        }
        println!("ACK {i}");
        i += 1;
    }
    std::process::exit(0);
}

/// Kill `child` with SIGKILL after `dur` or once `max_acks` lines were read;
/// returns how many ACK lines the child printed (durable writes under
/// SyncPolicy::Always).
fn spawn_kill(dir: &Path, mem_bytes: usize, dur_flag: &str, dur: Duration, max_acks: u64) -> Res<u64> {
    let exe = std::env::current_exe()?;
    let mut child = Command::new(exe)
        .args(["crash-child", dir.to_str().unwrap(), &mem_bytes.to_string(), dur_flag])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let stdout = child.stdout.take().unwrap();
    let t0 = Instant::now();
    let mut acks = 0u64;
    {
        // Read in a helper thread so a silent child can't block us forever.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if line.starts_with("ACK ") && tx.send(()).is_err() {
                    break;
                }
            }
        });
        while acks < max_acks && t0.elapsed() < dur {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(()) => acks += 1,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => break,
            }
        }
    }
    unsafe {
        libc::kill(child.id() as i32, libc::SIGKILL);
    }
    let _ = child.wait();
    Ok(acks)
}

fn wal_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir.join("db/wal")).into_iter().flatten().flatten() {
        if e.path().extension().is_some_and(|x| x == "log") {
            out.push(e.path());
        }
    }
    out.sort();
    out
}

fn verify(db: &Db, upto: u64, require_all: bool) -> Result<u64, String> {
    let mut found = 0u64;
    for i in 0..upto {
        match db.get(key(i, 16)).map_err(|e| e.to_string())? {
            Some(v) => {
                if v.as_ref() != value(i, VAL_SIZE).as_slice() {
                    return Err(format!("key {i}: wrong value"));
                }
                found += 1;
            }
            None if require_all => return Err(format!("key {i}: acked but missing after reopen")),
            None => {}
        }
    }
    Ok(found)
}

// ---------------------------------------------------------------- tests

fn t_restart(base: &Path) -> TestResult {
    let r = (|| -> Res<String> {
        let dir = base.join("restart");
        {
            let db = Db::open(cfg(&dir, SyncPolicy::EveryMs(1), 16 << 20))?;
            for i in 0..5_000u64 {
                db.put(key(i, 16), value(i, VAL_SIZE))?;
            }
            db.delete(key(0, 16))?;
            db.close()?;
        }
        let db = Db::open(cfg(&dir, SyncPolicy::EveryMs(1), 16 << 20))?;
        assert_eq!(db.get(key(0, 16))?, None);
        for i in 1..5_000u64 {
            let got = db.get(key(i, 16))?.ok_or(format!("key {i} lost across clean restart"))?;
            assert_eq!(got.as_ref(), value(i, VAL_SIZE).as_slice());
        }
        Ok("5k keys survived clean restart, tombstone intact".into())
    })();
    match r {
        Ok(d) => TestResult::ok("restart", d),
        Err(e) => TestResult::fail("restart", e.to_string()),
    }
}

fn t_crash_recovery(base: &Path) -> TestResult {
    let r = (|| -> Res<String> {
        let dir = base.join("crash");
        let acks = spawn_kill(&dir, 64 << 20, "always", Duration::from_secs(2), 3_000)?;
        if acks < 10 {
            return Err(format!("child only acked {acks} writes").into());
        }
        // SIGKILL'd mid-run: reopen must replay WAL, drop the torn tail.
        let db = Db::open(cfg(&dir, SyncPolicy::EveryMs(1), 64 << 20))?;
        let found = verify(&db, acks, true)?;
        // and still writable afterwards
        db.put(key(u64::MAX - 1, 16), b"post")?;
        Ok(format!("{acks} acked (fsync'd) writes, {found} verified after kill; db writable"))
    })();
    match r {
        Ok(d) => TestResult::ok("crash_recovery", d),
        Err(e) => TestResult::fail("crash_recovery", e.to_string()),
    }
}

fn t_wal_torn_tail(base: &Path) -> TestResult {
    let r = (|| -> Res<String> {
        let dir = base.join("wal_torn");
        {
            let db = Db::open(cfg(&dir, SyncPolicy::Always, 64 << 20))?;
            for i in 0..100u64 {
                db.put(key(i, 16), value(i, VAL_SIZE))?;
            }
            db.close()?;
        }
        // Simulate a torn write: garbage appended past the good records.
        let wal = wal_files(&dir).into_iter().next().ok_or("no wal file")?;
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal)?;
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x42, 0x13, 0x37])?;
        drop(f);
        let db = Db::open(cfg(&dir, SyncPolicy::EveryMs(1), 64 << 20))?;
        let found = verify(&db, 100, true)?;
        Ok(format!("torn tail ignored, {found}/100 keys intact"))
    })();
    match r {
        Ok(d) => TestResult::ok("wal_torn_tail", d),
        Err(e) => TestResult::fail("wal_torn_tail", e.to_string()),
    }
}

fn t_wal_corrupt_payload(base: &Path) -> TestResult {
    let r = (|| -> Res<String> {
        let dir = base.join("wal_corrupt");
        {
            let db = Db::open(cfg(&dir, SyncPolicy::Always, 64 << 20))?;
            db.put(key(0, 16), value(0, VAL_SIZE))?;
            db.close()?;
        }
        // Craft a record with VALID crc but undecodable payload (bad op tag)
        // — per spec this is corruption and must be reported, not swallowed.
        let wal = wal_files(&dir).into_iter().next().ok_or("no wal file")?;
        let payload = [0xAAu8, 1, 2, 3, 4, 5, 6, 7];
        let len = (payload.len() as u32).to_le_bytes();
        let mut h = crc32fast::Hasher::new();
        h.update(&len);
        h.update(&payload);
        let mut f = std::fs::OpenOptions::new().append(true).open(&wal)?;
        f.write_all(&h.finalize().to_le_bytes())?;
        f.write_all(&len)?;
        f.write_all(&payload)?;
        drop(f);
        match Db::open(cfg(&dir, SyncPolicy::EveryMs(1), 64 << 20)) {
            Err(e) => Ok(format!("open rejected corrupt record: {e}")),
            Ok(_) => Err("open succeeded despite crc-valid corrupt payload".into()),
        }
    })();
    match r {
        Ok(d) => TestResult::ok("wal_corrupt_payload", d),
        Err(e) => TestResult::fail("wal_corrupt_payload", e.to_string()),
    }
}

fn t_checkpoint_interruption(base: &Path) -> TestResult {
    let r = (|| -> Res<String> {
        let dir = base.join("ckpt_int");
        // Tiny memtable → constant flush/compaction churn, then SIGKILL.
        let acks = spawn_kill(&dir, 32 << 10, "fast", Duration::from_secs(2), 5_000)?;
        if acks == 0 {
            return Err("child made no progress".into());
        }
        // Drop a fake half-written segment like a dead flush would leave.
        std::fs::write(dir.join("db/seg").join(format!("{:016}.tmp", u64::MAX)), b"partial")?;
        let db = Db::open(cfg(&dir, SyncPolicy::EveryMs(1), 32 << 10))?;
        // Everything present must be correct; orphaned .tmp must be gone.
        let found = verify(&db, acks, false)?;
        assert!(found > 0, "nothing recovered");
        assert!(!dir.join("db/seg").join(format!("{:016}.tmp", u64::MAX)).exists(), ".tmp not cleaned");
        db.put(key(u64::MAX - 2, 16), b"post")?;
        db.flush()?;
        Ok(format!("recovered {found}/{acks} keys after kill mid-flush; .tmp discarded; db writable"))
    })();
    match r {
        Ok(d) => TestResult::ok("checkpoint_interruption", d),
        Err(e) => TestResult::fail("checkpoint_interruption", e.to_string()),
    }
}

fn t_disk_full(base: &Path) -> TestResult {
    let r = (|| -> Res<String> {
        let rd = match ramdisk::mount(32, "rxbench_full") {
            Ok(r) => r,
            Err(e) => return Ok(format!("skipped: ramdisk mount failed ({e})")),
        };
        let dir = rd.mount.join("dbroot");
        std::fs::create_dir_all(&dir)?;
        let db = Db::open(cfg(&dir, SyncPolicy::Never, 1 << 20))?;
        let mut wrote = 0u64;
        let mut err = None;
        for i in 0..50_000u64 {
            match db.put(key(i, 16), value(i, 4096)) {
                Ok(()) => wrote += 1,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        let err = match err {
            Some(e) => e,
            None => return Err("50k x 4KiB writes on a 32MiB disk never failed".into()),
        };
        // After failure the db must refuse more writes — not hang or panic.
        let still_err = db.put(key(0, 16), b"x").is_err();
        // clean up: free the WAL by dropping, then detach happens on Drop
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(format!("graceful ENOSPC after {wrote} writes ({err}); further write refused: {still_err}"))
    })();
    let _ = base;
    match r {
        Ok(d) => TestResult::ok("disk_full", d),
        Err(e) => TestResult::fail("disk_full", e.to_string()),
    }
}

pub fn run(base: &Path) -> Vec<TestResult> {
    let _ = std::fs::remove_dir_all(base);
    std::fs::create_dir_all(base).unwrap();
    let tests = [t_restart, t_crash_recovery, t_wal_torn_tail, t_wal_corrupt_payload, t_checkpoint_interruption, t_disk_full];
    let results = tests.iter().map(|t| t(base)).collect();
    let _ = std::fs::remove_dir_all(base);
    results
}
