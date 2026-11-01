//! Tiny tour: writes a few keys, flushes, prints the on-disk layout.
//! Run: cargo run -p rexodb --example tour -- /tmp/rexo_tour

use rexodb::{Config, Db};

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/rexo_tour".into());
    let _ = std::fs::remove_dir_all(&dir);

    let mut cfg = Config::new(&dir);
    cfg.memtable_bytes = 1024; // tiny, so flush happens after a few puts
    let db = Db::open(cfg).unwrap();

    // Writes go to wal/*.log first (durable), then into the in-RAM memtable.
    for i in 0..20 {
        db.put(format!("user:{i:02}"), format!("kankai-{i}")).unwrap();
    }
    db.put("greeting", "hello world").unwrap();

    println!(
        "get(greeting) = {:?}",
        db.get(b"greeting").unwrap().map(|b| String::from_utf8_lossy(&b).into_owned())
    );
    println!(
        "get(user:07)  = {:?}",
        db.get(b"user:07").unwrap().map(|b| String::from_utf8_lossy(&b).into_owned())
    );

    db.flush().unwrap(); // push memtable -> seg/*.sst, delete its wal file
    let s = db.stats();
    println!(
        "stats: puts={} gets={} memtable_hits={} segment_reads={} flushes={} live_segments={}",
        s.puts, s.gets, s.memtable_hits, s.segment_reads, s.flushes, s.live_segments
    );
    println!("db dir: {dir}");
}
