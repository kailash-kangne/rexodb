use std::time::{Duration, Instant};

use bytes::Bytes;
use rexodb::{Config, Db, WritePath};
use tempfile::TempDir;

fn cfg(dir: &TempDir, memtable_bytes: usize) -> (Config, std::path::PathBuf) {
    let path = dir.path().join("db");
    let mut c = Config::new(&path);
    c.memtable_bytes = memtable_bytes;
    (c, path)
}

fn wait_for_segments(db: &Db, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.stats().live_segments < n {
        assert!(Instant::now() < deadline, "timed out waiting for {n} segments");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn dump(path: &std::path::Path) -> String {
    let mut s = String::new();
    let seg_dir = path.join("seg");
    if let Ok(rd) = std::fs::read_dir(&seg_dir) {
        let mut names: Vec<_> = rd.flatten().map(|e| e.file_name()).collect();
        names.sort();
        for n in names {
            let hits = match rexodb_segment::Reader::open(&seg_dir.join(&n)) {
                Ok(r) => {
                    let mut hits = Vec::new();
                    if let Ok(mut it) = r.iter() {
                        while let Ok(Some((k, v))) = it.next_entry() {
                            if k.as_ref() == b"k05" {
                                hits.push(if v.is_some() { "v" } else { "TOMB" });
                            }
                        }
                    }
                    hits.join(",")
                }
                Err(_) => "?".to_string(),
            };
            s.push_str(&format!("{}[{}] ", n.to_string_lossy(), hits));
        }
    }
    let wal_dir = path.join("wal");
    if let Ok(rd) = std::fs::read_dir(&wal_dir) {
        let mut names: Vec<_> = rd.flatten().map(|e| e.file_name()).collect();
        names.sort();
        for n in names {
            let mut hits = Vec::new();
            let _ = rexodb_wal::replay(&wal_dir.join(&n), |ops| {
                for op in &ops {
                    match op {
                        rexodb_wal::Op::Put(k, _) if k.as_ref() == b"k05" => hits.push("v"),
                        rexodb_wal::Op::Delete(k) if k.as_ref() == b"k05" => hits.push("TOMB"),
                        _ => {}
                    }
                }
            });
            s.push_str(&format!("wal:{}[{}] ", n.to_string_lossy(), hits.join(",")));
        }
    }
    s
}

#[test]
fn repro_tombstone_loss() {
    for round in 0..300 {
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
        let db = Db::open(Config::new(&path)).unwrap();
        let got = db.get(b"k05").unwrap();
        if got.is_some() {
            panic!("round {round}: tombstone lost (got {got:?});\n{}", dump(&path));
        }
        assert_eq!(db.get(b"k06").unwrap(), Some(Bytes::from_static(b"v")), "round {round}");
    }
}
