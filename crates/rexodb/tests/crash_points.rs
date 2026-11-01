//! Storage-transition crash tests. A child process is armed with
//! `REXO_CRASH_AT=<point>` and aborts (no destructors — SIGKILL semantics)
//! the moment it reaches that on-disk transition: mid-WAL-write, between
//! fsync and ack, between segment rename and publish, before WAL unlink,
//! mid-compaction, mid-recovery. The parent then reopens and asserts every
//! fsync-acknowledged write survived — the whole matrix runs on Direct and
//! Pipeline, whose on-disk transitions differ.

use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rexodb::{crash, Config, Db, SyncPolicy, WritePath};
use tempfile::TempDir;

fn child_cfg(dir: &Path, pipeline: bool) -> Config {
    let mut c = Config::new(dir);
    c.sync = SyncPolicy::Always; // every acknowledged put is fsync'd
    c.memtable_bytes = 64 << 10;
    c.compact_at = 2;
    if pipeline {
        c.write_path = WritePath::Pipeline;
        c.ring_capacity = 32;
    }
    c
}

/// Child entry — not a standalone test: returns unless `REXO_CRASH_DIR`
/// is set (the parent spawns it through `current_exe`). The workload
/// drives every transition: WAL writes + fsyncs, rotations, segment
/// commits, compaction, and a reopen for the recovery fold.
#[test]
fn crash_point_child() {
    let Ok(dir) = std::env::var("REXO_CRASH_DIR") else { return };
    let dir = Path::new(&dir);
    let pipeline = std::env::var("REXO_CRASH_PIPELINE").is_ok();
    let progress_path = dir.join("progress");
    let mut acked = std::fs::read_to_string(&progress_path).map(|s| s.lines().count()).unwrap_or(0) as u64;
    // Two sessions: the second Db::open replays the first's leftover WAL —
    // that fold-then-unlink is itself an armed transition.
    for _session in 0..2 {
        {
            let db = Db::open(child_cfg(dir, pipeline)).unwrap();
            let mut progress =
                std::fs::OpenOptions::new().create(true).append(true).open(&progress_path).unwrap();
            for _round in 0..2 {
                for _ in 0..8 {
                    db.put(format!("k{acked:07}"), format!("v{acked}")).unwrap();
                    // Under `Always` the put only returned after fsync —
                    // record it as acknowledged-durable.
                    progress.write_all(format!("{acked}\n").as_bytes()).unwrap();
                    progress.flush().unwrap();
                    acked += 1;
                }
                db.flush().unwrap(); // rotate + tmp write + rename + publish + wal unlink
            }
            db.compact().unwrap(); // merge tmp + rename + input unlinks
            // Trailing un-flushed puts: the next session's open replays a
            // non-empty WAL — without these the recovery fold never runs.
            for _ in 0..4 {
                db.put(format!("k{acked:07}"), format!("v{acked}")).unwrap();
                progress.write_all(format!("{acked}\n").as_bytes()).unwrap();
                progress.flush().unwrap();
                acked += 1;
            }
            db.close().unwrap();
        }
    }
    std::process::exit(0);
}

#[test]
fn crash_at_every_storage_transition() {
    for pipeline in [false, true] {
        for point in 1..=crash::COUNT {
            // after=1 hits the first occurrence of the transition, after=2
            // a later one (e.g. second flush — different on-disk state).
            for after in [1u64, 2] {
                let dir = TempDir::new().unwrap();
                let db_dir = dir.path().join("db");
                let mut cmd = Command::new(std::env::current_exe().unwrap());
                cmd.args(["crash_point_child", "--exact"])
                    .env("REXO_CRASH_DIR", &db_dir)
                    .env("REXO_CRASH_AT", point.to_string())
                    .env("REXO_CRASH_AFTER", after.to_string())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null());
                if pipeline {
                    cmd.env("REXO_CRASH_PIPELINE", "1");
                }
                let mut child = cmd.spawn().unwrap();
                // Bound the child — a hang must fail the test, not CI.
                let deadline = Instant::now() + Duration::from_secs(90);
                while child.try_wait().unwrap().is_none() {
                    assert!(
                        Instant::now() < deadline,
                        "child hung: point {point} hit #{after} (pipeline={pipeline})"
                    );
                    std::thread::sleep(Duration::from_millis(25));
                }
                // Either it died at the armed point or ran clean because
                // that point isn't on this write path — both acceptable.
                let _ = child.wait().unwrap();

                // Reopen must succeed and every fsync-acked write intact.
                let progress = db_dir.join("progress");
                let acked =
                    std::fs::read_to_string(&progress).map(|s| s.lines().count()).unwrap_or(0);
                let db = Db::open(Config::new(&db_dir)).unwrap();
                for i in 0..acked as u64 {
                    let got = db.get(format!("k{i:07}")).unwrap();
                    assert_eq!(
                        got.as_deref(),
                        Some(format!("v{i}").as_bytes()),
                        "k{i:07} acked but lost — crash at point {point} hit #{after} (pipeline={pipeline})"
                    );
                }
                // Still writable after recovery.
                db.put(b"post-crash", b"ok").unwrap();
                assert_eq!(db.get(b"post-crash").unwrap(), Some(Bytes::from_static(b"ok")));
            }
        }
    }
}
