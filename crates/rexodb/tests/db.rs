use std::time::{Duration, Instant};

use bytes::Bytes;
use rexodb::{Config, Db, SyncPolicy, WriteBatch, WritePath};
use tempfile::TempDir;

fn cfg(dir: &TempDir, memtable_bytes: usize) -> (Config, std::path::PathBuf) {
    let path = dir.path().join("db");
    let mut c = Config::new(&path);
    c.memtable_bytes = memtable_bytes;
    (c, path)
}

fn wait_for_segments(db: &Db, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while db.stats().live_segments < n {
        assert!(Instant::now() < deadline, "timed out waiting for {n} segments");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn crud_roundtrip() {
    let dir = TempDir::new().unwrap();
    let (c, _) = cfg(&dir, 1 << 20);
    let db = Db::open(c).unwrap();

    db.put(b"k1", b"v1").unwrap();
    assert_eq!(db.get(b"k1").unwrap(), Some(Bytes::from_static(b"v1")));
    assert_eq!(db.get(b"missing").unwrap(), None);

    db.put(b"k1", b"v2").unwrap(); // overwrite
    assert_eq!(db.get(b"k1").unwrap(), Some(Bytes::from_static(b"v2")));

    db.delete(b"k1").unwrap();
    assert_eq!(db.get(b"k1").unwrap(), None);
}

#[test]
fn survives_reopen() {
    let dir = TempDir::new().unwrap();
    let (c, path) = cfg(&dir, 1 << 20);
    {
        let db = Db::open(c).unwrap();
        db.put(b"alive", b"yes").unwrap();
        db.put(b"dead", b"gone").unwrap();
        db.delete(b"dead").unwrap();
        db.close().unwrap();
    }
    let mut c2 = Config::new(&path);
    c2.sync = SyncPolicy::Never; // durability-independent reopen must still work
    let db = Db::open(c2).unwrap();
    assert_eq!(db.get(b"alive").unwrap(), Some(Bytes::from_static(b"yes")));
    assert_eq!(db.get(b"dead").unwrap(), None);
}

#[test]
fn batch_applies_atomically() {
    let dir = TempDir::new().unwrap();
    let (c, _) = cfg(&dir, 1 << 20);
    let db = Db::open(c).unwrap();

    let mut b = WriteBatch::new();
    b.put(b"a", b"1").put(b"b", b"2").delete(b"c");
    db.put(b"c", b"old").unwrap();
    db.write(&b).unwrap();

    assert_eq!(db.get(b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(db.get(b"b").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(db.get(b"c").unwrap(), None);
}

#[test]
fn range_scans_ordered_and_filter_tombstones() {
    let dir = TempDir::new().unwrap();
    let (c, _) = cfg(&dir, 1 << 20);
    let db = Db::open(c).unwrap();

    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        db.put(k, v).unwrap();
    }
    db.delete(b"b").unwrap();

    let got: Vec<(Bytes, Bytes)> =
        db.range(b"a".as_slice()..b"d".as_slice()).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(
        got,
        vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"c"), Bytes::from_static(b"3")),
        ]
    );

    let all: Vec<(Bytes, Bytes)> = db.range::<[u8], _>(..).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(all.len(), 3); // a, c, d — b tombstoned

    // Reversed bounds must not panic (BTreeMap::range would) — empty result.
    let empty: Vec<_> =
        db.range(b"d".as_slice()..b"a".as_slice()).unwrap().collect::<Result<_, _>>().unwrap();
    assert!(empty.is_empty());
}

#[test]
fn flush_to_segment_and_read_through() {
    let dir = TempDir::new().unwrap();
    let (c, _) = cfg(&dir, 256); // tiny memtable → fast flushes
    let db = Db::open(c).unwrap();

    for i in 0..50u32 {
        db.put(format!("key{i:03}"), format!("val{i}")).unwrap();
    }
    wait_for_segments(&db, 1);

    // Reads must span memtable + imm + segments.
    for i in 0..50u32 {
        let v = db.get(format!("key{i:03}")).unwrap();
        assert_eq!(v, Some(Bytes::from(format!("val{i}"))), "key{i}");
    }
}

#[test]
fn tombstones_survive_flush_and_reopen() {
    let dir = TempDir::new().unwrap();
    let (c, path) = cfg(&dir, 128);
    {
        let db = Db::open(c).unwrap();
        for i in 0..20u32 {
            db.put(format!("k{i:02}"), b"v").unwrap();
        }
        wait_for_segments(&db, 1);
        db.delete(b"k05").unwrap();
        db.flush().unwrap();
        db.close().unwrap();
    }
    let db = Db::open(Config::new(&path)).unwrap();
    assert_eq!(db.get(b"k05").unwrap(), None);
    assert_eq!(db.get(b"k06").unwrap(), Some(Bytes::from_static(b"v")));
}

#[test]
fn compaction_merges_and_drops_tombstones() {
    let dir = TempDir::new().unwrap();
    let (mut c, _) = cfg(&dir, 128);
    c.compact_at = 100; // manual compaction only
    let db = Db::open(c).unwrap();

    // Round 1: values in segment 1.
    for i in 0..10u32 {
        db.put(format!("k{i:02}"), b"old").unwrap();
    }
    db.flush().unwrap();
    // Round 2: overwrites + a delete in segment 2.
    for i in 0..10u32 {
        db.put(format!("k{i:02}"), b"new").unwrap();
    }
    db.delete(b"k03").unwrap();
    db.flush().unwrap();
    assert!(db.stats().live_segments >= 2);

    db.compact().unwrap();
    assert_eq!(db.stats().live_segments, 1);
    assert_eq!(db.get(b"k00").unwrap(), Some(Bytes::from_static(b"new")));
    assert_eq!(db.get(b"k03").unwrap(), None);
}

#[test]
fn sync_policy_always_acknowledges_durably() {
    let dir = TempDir::new().unwrap();
    let mut c = Config::new(dir.path().join("db"));
    c.sync = SyncPolicy::Always;
    let db = Db::open(c).unwrap();
    db.put(b"durable", b"yes").unwrap(); // returns only after fsync
    assert_eq!(db.get(b"durable").unwrap(), Some(Bytes::from_static(b"yes")));
}

#[test]
fn oversized_inputs_rejected_not_panicked() {
    let dir = TempDir::new().unwrap();
    let mut c = Config::new(dir.path().join("db"));
    c.max_key_bytes = 8;
    c.max_value_bytes = 16;
    let db = Db::open(c).unwrap();

    assert!(db.put(vec![b'x'; 9], b"v").is_err()); // key too big
    assert!(db.put(b"k", vec![b'x'; 17]).is_err()); // value too big
    assert!(db.get(vec![b'x'; 9]).is_err());
    assert!(db.put(b"ok", b"fine").is_ok()); // db still usable after rejections
}

#[test]
fn cache_never_shadows_fresher_data() {
    let dir = TempDir::new().unwrap();
    let (mut c, _) = cfg(&dir, 128);
    c.compact_at = 100;
    let db = Db::open(c).unwrap();

    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap(); // seg1: k→v1; read populates cache for seg1
    assert_eq!(db.get(b"k").unwrap(), Some(Bytes::from_static(b"v1")));

    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap(); // seg2 wins over seg1's cached entry
    assert_eq!(db.get(b"k").unwrap(), Some(Bytes::from_static(b"v2")));

    db.compact().unwrap(); // seg1+seg2 → merged; cached seg1 entry unreachable
    assert_eq!(db.get(b"k").unwrap(), Some(Bytes::from_static(b"v2")));
    let hits = db.stats().cache_hits;
    db.get(b"k").unwrap();
    assert!(db.stats().cache_hits > hits); // merged seg entry now served hot

    // Cached tombstone must not shadow a fresher memtable write.
    db.delete(b"k").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
    db.put(b"k", b"v3").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(Bytes::from_static(b"v3")));
}

#[test]
fn direct_write_path_roundtrip() {
    let dir = TempDir::new().unwrap();
    let (mut c, path) = cfg(&dir, 256);
    c.write_path = WritePath::Direct;
    {
        let db = Db::open(c).unwrap();
        for i in 0..30u32 {
            db.put(format!("k{i:02}"), format!("v{i}")).unwrap();
        }
        db.put(b"gone", b"x").unwrap();
        db.delete(b"gone").unwrap();
        wait_for_segments(&db, 1);
        db.close().unwrap();
    }
    let mut c2 = Config::new(&path);
    c2.write_path = WritePath::Direct;
    let db = Db::open(c2).unwrap();
    assert_eq!(db.get(b"k07").unwrap(), Some(Bytes::from_static(b"v7")));
    assert_eq!(db.get(b"gone").unwrap(), None);
}

#[test]
fn pipeline_ring_wraps_and_stays_consistent() {
    let dir = TempDir::new().unwrap();
    let (mut c, path) = cfg(&dir, 1 << 20);
    c.write_path = WritePath::Pipeline;
    c.ring_capacity = 16; // force constant wrap-around
    c.sync = SyncPolicy::Never;
    let db = std::sync::Arc::new(Db::open(c).unwrap());

    // 4 producers × 200 puts >> capacity — exercises wrap backpressure and
    // multi-producer claim ordering.
    std::thread::scope(|s| {
        for t in 0..4u32 {
            let db = db.clone();
            s.spawn(move || {
                for i in 0..200u32 {
                    db.put(format!("t{t}k{i:03}"), format!("v{i}")).unwrap();
                }
            });
        }
    });
    for t in 0..4u32 {
        for i in 0..200u32 {
            let v = db.get(format!("t{t}k{i:03}")).unwrap();
            assert_eq!(v, Some(Bytes::from(format!("v{i}"))), "t{t}k{i:03}");
        }
    }
    db.close().unwrap();
    drop(db);

    // `Never` durability + close(): the Close barrier fsyncs, so the data
    // must survive reopen.
    let db = Db::open(Config::new(&path)).unwrap();
    assert_eq!(db.get(b"t2k123").unwrap(), Some(Bytes::from_static(b"v123")));
}

#[test]
fn pipeline_flush_rotation_race_repro() {
    // Mirror of tombstones_survive_flush_and_reopen, looped — tiny
    // memtable makes auto-rotation race the explicit flush/close/reopen.
    for round in 0..40 {
        let dir = TempDir::new().unwrap();
        let (mut c, path) = cfg(&dir, 128);
        c.write_path = WritePath::Pipeline;
        {
            let db = Db::open(c).unwrap();
            for i in 0..20u32 {
                db.put(format!("k{i:02}"), b"v").unwrap();
            }
            wait_for_segments(&db, 1);
            db.delete(b"k05").unwrap();
            db.flush().unwrap();
            db.close().unwrap();
        }
        let dump = || {
            let mut s = String::new();
            for e in std::fs::read_dir(path.join("seg")).unwrap() {
                s.push_str(&format!("seg/{} ", e.unwrap().file_name().to_string_lossy()));
            }
            for e in std::fs::read_dir(path.join("wal")).unwrap() {
                s.push_str(&format!("wal/{} ", e.unwrap().file_name().to_string_lossy()));
            }
            s
        };
        let db = Db::open(Config::new(&path)).unwrap();
        assert_eq!(db.get(b"k05").unwrap(), None, "round {round}: tombstone lost; {}", dump());
        assert_eq!(
            db.get(b"k06").unwrap(),
            Some(Bytes::from_static(b"v")),
            "round {round}: put lost; {}",
            dump()
        );
    }
}

#[test]
fn put_async_same_thread_reads_own_writes() {
    let dir = TempDir::new().unwrap();
    let (mut c, path) = cfg(&dir, 1 << 20);
    c.write_path = WritePath::Pipeline;
    {
        let db = Db::open(c).unwrap();
        for i in 0..50u32 {
            db.put_async(format!("k{i:03}"), format!("v{i}")).unwrap();
            assert_eq!(
                db.get(format!("k{i:03}")).unwrap(),
                Some(Bytes::from(format!("v{i}"))),
                "read-your-writes broken at k{i:03}"
            );
        }
        db.delete_async(b"k001").unwrap();
        assert_eq!(db.get(b"k001").unwrap(), None);
        db.close().unwrap();
    }
    let db = Db::open(Config::new(&path)).unwrap();
    assert_eq!(db.get(b"k049").unwrap(), Some(Bytes::from_static(b"v49")));
    assert_eq!(db.get(b"k001").unwrap(), None);
}

#[test]
fn put_durable_forces_fsync_under_never() {
    // `Never` never fsyncs on its own — `put_durable` must force a barrier
    // or it hangs. (Power-loss durability isn't observable in a test.)
    for write_path in [WritePath::Direct, WritePath::Pipeline] {
        let dir = TempDir::new().unwrap();
        let (mut c, path) = cfg(&dir, 1 << 20);
        c.write_path = write_path;
        c.sync = SyncPolicy::Never;
        {
            let db = Db::open(c).unwrap();
            db.put_durable(b"trade", b"filled").unwrap();
            db.delete_durable(b"gone").unwrap();
            db.write_durable(&{
                let mut b = WriteBatch::new();
                b.put(b"batch", b"v");
                b
            })
            .unwrap();
            db.close().unwrap();
        }
        let db = Db::open(Config::new(&path)).unwrap();
        assert_eq!(db.get(b"trade").unwrap(), Some(Bytes::from_static(b"filled")));
        assert_eq!(db.get(b"batch").unwrap(), Some(Bytes::from_static(b"v")));
    }
}

#[test]
fn get_many_spans_layers_and_tombstones() {
    let dir = TempDir::new().unwrap();
    let (c, _) = cfg(&dir, 128);
    let db = Db::open(c).unwrap();

    db.put(b"a", b"1").unwrap();
    db.flush().unwrap(); // segment
    db.put(b"b", b"2").unwrap(); // memtable
    db.put(b"c", b"3").unwrap();
    db.delete(b"c").unwrap(); // tombstone in memtable
                              // d never written

    let r = db.get_many(&[b"a".as_slice(), b"b", b"c", b"d"]).unwrap();
    assert_eq!(r, vec![Some(Bytes::from_static(b"1")), Some(Bytes::from_static(b"2")), None, None]);
}

#[test]
fn pipeline_pin_cores_roundtrip() {
    // Exercises the pin_cores plumbing end to end — the affinity call is
    // best-effort, so an id outside the current cpuset must not break open.
    let dir = TempDir::new().unwrap();
    let (mut c, path) = cfg(&dir, 1 << 20);
    c.write_path = WritePath::Pipeline;
    c.ring_capacity = 16;
    c.pin_cores = vec![0, 1];
    let db = Db::open(c).unwrap();
    for i in 0..100u32 {
        db.put_async(format!("k{i:03}"), format!("v{i}")).unwrap();
    }
    db.close().unwrap();
    let db = Db::open(Config::new(&path)).unwrap();
    assert_eq!(db.get(b"k099").unwrap(), Some(Bytes::from_static(b"v99")));
}

#[test]
fn dir_lock_blocks_second_open() {
    let dir = TempDir::new().unwrap();
    let (c, path) = cfg(&dir, 1 << 20);
    let db = Db::open(c).unwrap();
    assert!(Db::open(Config::new(&path)).is_err());
    db.close().unwrap();
    // close() released the lock — reopen works even while `db` is in scope.
    Db::open(Config::new(&path)).unwrap();
}

/// Child-process entry for [`crash_acknowledged_writes_survive`]: writes
/// `k{i} -> v{i}` one fsync-acked `put` at a time and appends each index
/// to `progress`. Not a standalone test — returns unless `REXO_CRASH_DIR`
/// is set. The count is capped so an orphaned child can't fill the disk.
#[test]
fn crash_writer_child() {
    let Ok(dir) = std::env::var("REXO_CRASH_DIR") else { return };
    use std::io::Write;
    let mut c = Config::new(&dir);
    c.sync = SyncPolicy::Always;
    if std::env::var("REXO_CRASH_PIPELINE").is_ok() {
        c.write_path = WritePath::Pipeline;
    }
    let db = Db::open(c).unwrap();
    let mut progress =
        std::fs::OpenOptions::new().create(true).append(true).open(format!("{dir}/progress")).unwrap();
    for i in 0u64..10_000 {
        db.put(format!("k{i:07}"), format!("v{i}")).unwrap();
        progress.write_all(format!("{i}\n").as_bytes()).unwrap();
    }
}

#[test]
fn incremental_merge_keeps_tombstone_shadow() {
    let dir = TempDir::new().unwrap();
    let (mut c, path) = cfg(&dir, 256);
    c.compact_at = 3;
    let db = Db::open(c).unwrap();

    // Fat oldest segment: never the "smallest pair", so the first merge is
    // a mid-window merge that must write tombstones through, not drop them.
    for i in 0..200 {
        db.put(format!("old{i:03}"), format!("v{i}")).unwrap();
    }
    db.put(b"victim", b"old").unwrap();
    db.flush().unwrap();
    db.put(b"m1", b"x").unwrap();
    db.flush().unwrap();
    db.delete(b"victim").unwrap();
    db.put(b"m2", b"y").unwrap();
    db.flush().unwrap();
    wait_for_segments(&db, 3);

    db.put(b"trig", b"t").unwrap();
    db.flush().unwrap(); // 4 > compact_at → background incremental merge
    let deadline = Instant::now() + Duration::from_secs(60);
    while db.stats().live_segments > 3 {
        assert!(Instant::now() < deadline, "incremental merge never ran");
        std::thread::sleep(Duration::from_millis(5));
    }

    assert_eq!(db.get(b"victim").unwrap(), None);
    assert_eq!(db.get(b"m1").unwrap().as_deref(), Some(b"x".as_slice()));
    assert_eq!(db.get(b"m2").unwrap().as_deref(), Some(b"y".as_slice()));
    assert_eq!(db.get(b"trig").unwrap().as_deref(), Some(b"t".as_slice()));
    assert_eq!(db.get(b"old000").unwrap().as_deref(), Some(b"v0".as_slice()));

    db.close().unwrap();
    let db = Db::open(Config::new(&path)).unwrap();
    assert_eq!(db.get(b"victim").unwrap(), None);
}

#[test]
fn crash_acknowledged_writes_survive() {
    for pipeline in [false, true] {
        for round in 0..3 {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("db");
            let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
            cmd.args(["crash_writer_child", "--exact"])
                .env("REXO_CRASH_DIR", &path)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            if pipeline {
                cmd.env("REXO_CRASH_PIPELINE", "1");
            }
            let mut child = cmd.spawn().unwrap();

            // Kill once the child has fsync-acked enough writes — a
            // different depth per round plus real-time jitter.
            let jitter =
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos()
                    as usize;
            let target = 5 + round * 15 + jitter % 20;
            let deadline = Instant::now() + Duration::from_secs(30);
            let progress = path.join("progress");
            loop {
                let acked = std::fs::read_to_string(&progress).map(|s| s.lines().count()).unwrap_or(0);
                if acked >= target || child.try_wait().unwrap().is_some() || Instant::now() > deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let _ = child.kill();
            let _ = child.wait();

            // Reopen — replay must succeed and every write that was
            // fsync-acked must be intact.
            let acked = std::fs::read_to_string(&progress).map(|s| s.lines().count()).unwrap_or(0);
            let db = Db::open(Config::new(&path)).unwrap();
            for i in 0..acked as u64 {
                let got = db.get(format!("k{i:07}")).unwrap();
                assert_eq!(
                    got.as_deref(),
                    Some(format!("v{i}").as_bytes()),
                    "k{i:07} was fsync-acked but lost on crash (pipeline={pipeline})"
                );
            }
        }
    }
}
