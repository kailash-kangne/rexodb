//! rexodb-bench — comparative benchmark + reliability harness for rexodb.
//!
//! ```text
//! cargo run --release -- run [--dbs rexodb,sqlite,lmdb,redb] [--threads 1,2,4,8,16,32]
//!     [--reps 10] [--durability periodic|strong|weak] [--inmem] [--quick]
//!     [--workdir DIR] [--out DIR] [--baseline FILE] [--save-baseline FILE]
//! cargo run --release -- reliability [--workdir DIR]
//! cargo run --release -- env
//! ```

mod kv;
mod metrics;
mod ramdisk;
mod reliability;
mod report;
mod sysinfo;
mod workload;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Arc;
use std::time::Instant;

use kv::{BatchOp, Durability, Kv};
use metrics::{Lat, RepResult};
use report::{Scenario, ScenarioResult};
use workload::{key, value, Op, Rng, Work};

struct Args {
    kv: HashMap<String, String>,
    flags: Vec<String>,
}

impl Args {
    fn parse(args: &[String]) -> Self {
        let mut kv = HashMap::new();
        let mut flags = Vec::new();
        let mut i = 0;
        while i < args.len() {
            if let Some(k) = args[i].strip_prefix("--") {
                if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                    kv.insert(k.to_string(), args[i + 1].clone());
                    i += 2;
                } else {
                    flags.push(k.to_string());
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
        Self { kv, flags }
    }
    fn has(&self, f: &str) -> bool {
        self.flags.iter().any(|x| x == f)
    }
    fn get(&self, k: &str, dflt: &str) -> String {
        self.kv.get(k).cloned().unwrap_or_else(|| dflt.into())
    }
    fn list<T: std::str::FromStr + Clone>(&self, k: &str, dflt: &[T]) -> Vec<T> {
        match self.kv.get(k) {
            Some(s) => s.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
            None => dflt.to_vec(),
        }
    }
}

fn base_ops(w: Work) -> usize {
    match w {
        Work::FillSeq | Work::FillRand => 20_000,
        Work::ReadRand => 50_000,
        Work::DeleteRand => 10_000,
        Work::Batch128 => 200,
        Work::Scan1000 => 200,
        Work::Mixed5050 => 20_000,
    }
}

fn exec_op(db: &dyn Kv, op: &Op) -> kv::Res<()> {
    match op {
        Op::Put(k, v) => db.put(k, v),
        Op::Del(k) => db.del(k),
        Op::Get(k) => db.get(k).map(|_| ()),
        Op::Batch(puts) => db.batch(&puts.iter().map(|(k, v)| BatchOp::Put(k.clone(), v.clone())).collect::<Vec<_>>()),
        Op::Scan(lo, lim) => db.scan(lo, *lim).map(|_| ()),
    }
}

fn exec_slice(db: &dyn Kv, ops: &[Op], lat: &mut Lat) -> Result<(), String> {
    for op in ops {
        let t = Instant::now();
        exec_op(db, op).map_err(|e| e.to_string())?;
        lat.rec(t.elapsed());
    }
    Ok(())
}

/// Execute `ops` across `threads` workers; returns (latencies, wall_secs,
/// cpu_ms, max_rss_kib). Op errors fail the rep — never silently dropped.
fn run_ops(db: &Arc<dyn Kv>, ops: &[Op], threads: usize) -> Result<(Lat, f64, f64, u64), String> {
    let u0 = sysinfo::usage();
    let t0 = Instant::now();
    let mut lat = Lat::with_capacity(ops.len());
    if threads <= 1 {
        exec_slice(&**db, ops, &mut lat)?;
    } else {
        let chunk = ops.len().div_ceil(threads);
        let mut lats = Vec::with_capacity(threads);
        let mut first_err = None;
        std::thread::scope(|s| {
            let mut handles = Vec::with_capacity(threads);
            for part in ops.chunks(chunk) {
                let db = db.clone();
                handles.push(s.spawn(move || {
                    let mut l = Lat::with_capacity(part.len());
                    exec_slice(&*db, part, &mut l).map(|_| l)
                }));
            }
            for h in handles {
                match h.join() {
                    Ok(Ok(l)) => lats.push(l),
                    Ok(Err(e)) if first_err.is_none() => first_err = Some(e),
                    Ok(Err(_)) => {}
                    Err(_) if first_err.is_none() => first_err = Some("worker panicked".into()),
                    Err(_) => {}
                }
            }
        });
        if let Some(e) = first_err {
            return Err(e);
        }
        for l in lats {
            lat.merge(l);
        }
    }
    let wall = t0.elapsed().as_secs_f64();
    let u1 = sysinfo::usage();
    Ok((lat, wall, u1.cpu_ms_since(&u0), u1.max_rss_kib))
}

/// Preload `n` keys in batches of 256 so setup isn't the bottleneck.
fn preload(db: &dyn Kv, n: u64, ks: usize, vs: usize) -> Result<(), String> {
    let mut i = 0u64;
    while i < n {
        let end = (i + 256).min(n);
        let ops: Vec<BatchOp> = (i..end).map(|j| BatchOp::Put(key(j, ks), value(j, vs))).collect();
        db.batch(&ops).map_err(|e| e.to_string())?;
        i = end;
    }
    db.sync().map_err(|e| e.to_string())
}

/// Does this workload only read/mutate-preloaded keys? Then one db instance
/// can serve all reps (independent op streams, same data) — avoids paying
/// preload cost 10x for read-dominated runs.
fn shared_preload(w: Work) -> bool {
    matches!(w, Work::ReadRand | Work::Scan1000 | Work::Mixed5050 | Work::DeleteRand)
}

fn ops_for_rep(sc: &Scenario, rep: usize) -> Vec<Op> {
    let seed = sc.seed + rep as u64;
    if sc.workload == Work::DeleteRand {
        // Disjoint partitions so reps don't delete the same keys twice.
        let chunk = sc.preload.max(1) / 16;
        let lo = rep as u64 % 16 * chunk;
        let mut rng = Rng::new(seed);
        return (0..sc.ops).map(|_| Op::Del(key(lo + rng.below(chunk), sc.key_size))).collect();
    }
    workload::stream(sc.workload, sc.ops, sc.key_size, sc.value_size, sc.preload, seed)
}

fn run_scenario(sc: &Scenario, workdir: &Path, reps: usize, dur: Durability) -> ScenarioResult {
    let mut rep_results = Vec::with_capacity(reps);
    let dir = workdir.join(format!("s{:x}", sc.seed));
    let shared = shared_preload(sc.workload);

    // Shared-preload workloads build the db once; fills rebuild per rep.
    let mut held_db: Option<Arc<dyn Kv>> = None;
    if shared {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let d: Arc<dyn Kv> = Arc::from(kv::open(&sc.db, &dir, dur, sc.inmem).unwrap());
        if sc.preload > 0 {
            preload(&*d, sc.preload, sc.key_size, sc.value_size).unwrap();
        }
        held_db = Some(d);
    }

    for r in 0..reps {
        let db = match &held_db {
            Some(d) => d.clone(),
            None => {
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir).unwrap();
                Arc::from(kv::open(&sc.db, &dir, dur, sc.inmem).unwrap())
            }
        };
        let ops = ops_for_rep(sc, r);
        match run_ops(&db, &ops, sc.threads) {
            Ok((lat, wall, cpu_ms, rss)) => {
                let [p50, p95, p99, p999] = lat.percentiles();
                rep_results.push(RepResult {
                    ops: ops.len() as u64,
                    wall_secs: wall,
                    ops_per_sec: ops.len() as f64 / wall,
                    p50_ns: p50,
                    p95_ns: p95,
                    p99_ns: p99,
                    p999_ns: p999,
                    cpu_ms,
                    max_rss_kib: rss,
                    disk_bytes: sysinfo::dir_size(&dir),
                });
            }
            Err(e) => eprintln!("  rep {r} failed for {}: {e}", sc.id()),
        }
    }
    // Scratch dbs are per-scenario and regenerated per rep — never leave
    // them on disk after the run.
    drop(held_db);
    let _ = std::fs::remove_dir_all(&dir);
    ScenarioResult { scenario: sc.clone(), stats: metrics::aggregate(&rep_results), regression_pct: None }
}

fn scenarios(args: &Args) -> (Vec<Scenario>, Durability) {
    let dbs: Vec<String> = args.get("dbs", "rexodb,sqlite,lmdb,redb").split(',').map(|s| s.trim().into()).collect();
    let quick = args.has("quick");
    let threads: Vec<usize> = args.list("threads", if quick { &[1, 8] } else { &[1, 2, 4, 8, 16, 32] });
    let dur = match args.get("durability", "periodic").as_str() {
        "strong" => Durability::Strong,
        "weak" => Durability::Weak,
        _ => Durability::Periodic,
    };
    let works: &[Work] = if quick {
        &[Work::FillRand, Work::ReadRand, Work::Mixed5050]
    } else {
        Work::all()
    };
    let sizes: &[(usize, usize)] = if quick { &[(16, 128)] } else { &[(16, 128), (64, 4096)] };
    let mut out = Vec::new();
    let mut seed = 0x5245_584fu64; // "REXO"
    for db in &dbs {
        for &w in works {
            for &t in &threads {
                for &(ks, vs) in sizes {
                    // One size combo is enough for delete/batch/scan —
                    // fill/read benefit most from the size dimension.
                    if (ks, vs) != (16, 128)
                        && !matches!(w, Work::FillSeq | Work::FillRand | Work::ReadRand | Work::Mixed5050)
                    {
                        continue;
                    }
                    let mut ops = base_ops(w);
                    if dur == Durability::Strong && w.writes() {
                        ops = (ops / 25).max(50); // fsync-bound — keep runtime sane
                    }
                    // Read/mixed/scan/delete need data present first.
                    let preload =
                        if matches!(w, Work::FillSeq | Work::FillRand | Work::Batch128) { 0 } else { 150_000 };
                    out.push(Scenario {
                        db: db.clone(),
                        workload: w,
                        key_size: ks,
                        value_size: vs,
                        threads: t,
                        durability: dur.as_str().into(),
                        inmem: false,
                        preload,
                        ops,
                        seed,
                    });
                    seed += 1;
                }
            }
        }
    }
    (out, dur)
}

fn usage() -> ! {
    eprintln!(
        "rexodb-bench <cmd>\n  run   [--dbs ..] [--threads ..] [--reps N] [--durability strong|periodic|weak]\n        [--inmem] [--quick] [--workdir DIR] [--out DIR] [--baseline F] [--save-baseline F]\n  reliability [--workdir DIR] [--out DIR]\n  env"
    );
    exit(2)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("run") => cmd_run(&args[2..]),
        Some("reliability") => cmd_reliability(&args[2..]),
        Some("crash-child") => reliability::crash_child(&args[2..]),
        Some("env") => println!("{}", sysinfo::json(".")),
        _ => usage(),
    }
}

fn cmd_run(argv: &[String]) {
    let args = Args::parse(argv);
    let reps: usize = args.get("reps", "10").parse().unwrap();
    let workdir = PathBuf::from(args.get("workdir", "work"));
    let outdir = PathBuf::from(args.get("out", "out"));
    std::fs::create_dir_all(&workdir).unwrap();
    std::fs::create_dir_all(&outdir).unwrap();

    let (mut scs, dur) = scenarios(&args);

    // In-memory mode: subset of workloads on a RAM disk.
    let mut ram = None;
    if args.has("inmem") {
        match ramdisk::mount(2048, "rxbench_mem") {
            Ok(r) => {
                eprintln!("ramdisk mounted at {}", r.mount.display());
                ram = Some(r);
            }
            Err(e) => eprintln!("ramdisk mount failed ({e}); skipping inmem scenarios"),
        }
    }

    let mut results = Vec::with_capacity(scs.len());
    let mut notes = Vec::new();
    notes.push("durability mapping: strong = Always|synchronous=FULL|lmdb default|redb Immediate; periodic = EveryMs(1)|NORMAL|NO_META_SYNC|Eventual; weak = Never|OFF|NO_SYNC|None".to_string());
    notes.push("disk_bytes = logical on-disk size (du); macOS ru_inblock unreliable, so disk I/O is reported as bytes written to the db dir".to_string());
    notes.push("read/scan/mixed share one preloaded db across reps; delete uses disjoint key partitions; fills rebuild per rep".to_string());
    if !cfg!(feature = "rocksdb") {
        notes.push("rocksdb adapter present but disabled: build with --features rocksdb (needs cmake)".to_string());
    }

    for sc in scs.drain(..) {
        if sc.inmem && ram.is_none() {
            continue;
        }
        let wd = if sc.inmem { ram.as_ref().unwrap().mount.clone() } else { workdir.clone() };
        eprintln!("▶ {}", sc.id());
        results.push(run_scenario(&sc, &wd, reps, dur));
    }

    // In-memory pass on the ramdisk for a workload subset.
    if let Some(rd) = &ram {
        for sc in scs_subset_inmem(&args) {
            eprintln!("▶ {} (inmem)", sc.id());
            results.push(run_scenario(&sc, &rd.mount, reps, dur));
        }
    }
    let _ = std::fs::remove_dir(&workdir); // only succeeds when left empty

    let mut regressions = Vec::new();
    if let Some(p) = args.kv.get("baseline") {
        match report::load_baseline(Path::new(p)) {
            Ok(b) => regressions = report::check_regressions(&mut results, &b, 15.0),
            Err(e) => eprintln!("baseline load failed: {e}"),
        }
    }
    if let Some(p) = args.kv.get("save-baseline") {
        if let Err(e) = report::save_baseline(&results, Path::new(p)) {
            eprintln!("baseline save failed: {e}");
        }
    }

    let rep = report::Report {
        sysinfo: sysinfo::capture(&workdir.display().to_string()),
        generated_at: chrono_now(),
        reps_requested: reps,
        results,
        regressions,
        notes,
    };
    report::write_json(&rep, &outdir.join("results.json")).unwrap();
    report::write_md(&rep, &outdir.join("report.md")).unwrap();
    eprintln!("wrote {}", outdir.join("report.md").display());
}

fn scs_subset_inmem(args: &Args) -> Vec<Scenario> {
    let dbs: Vec<String> = args.get("dbs", "rexodb,sqlite,lmdb,redb").split(',').map(|s| s.trim().into()).collect();
    let mut out = Vec::new();
    let mut seed = 0x5245_584f_8000u64;
    for db in dbs {
        for w in [Work::FillRand, Work::ReadRand, Work::Mixed5050] {
            for t in [1usize, 8, 32] {
                let preload = if w == Work::FillRand { 0 } else { 100_000 };
                out.push(Scenario {
                    db: db.clone(),
                    workload: w,
                    key_size: 16,
                    value_size: 128,
                    threads: t,
                    durability: "inmem".into(),
                    inmem: true,
                    preload,
                    ops: base_ops(w),
                    seed,
                });
                seed += 1;
            }
        }
    }
    out
}

fn cmd_reliability(argv: &[String]) {
    let args = Args::parse(argv);
    let workdir = PathBuf::from(args.get("workdir", "work-rel"));
    eprintln!("running reliability tests...");
    let results = reliability::run(&workdir);
    let mut all_ok = true;
    for r in &results {
        eprintln!("  {} {:<26} {}", if r.ok { "PASS" } else { "FAIL" }, r.name, r.detail);
        all_ok &= r.ok;
    }
    if let Some(out) = args.kv.get("out") {
        #[derive(serde::Serialize)]
        struct R<'a> {
            name: &'a str,
            ok: bool,
            detail: &'a str,
        }
        let v: Vec<_> = results.iter().map(|r| R { name: r.name, ok: r.ok, detail: &r.detail }).collect();
        let p = Path::new(out).join("reliability.json");
        std::fs::create_dir_all(out).unwrap();
        std::fs::write(&p, serde_json::to_string_pretty(&v).unwrap()).unwrap();
        eprintln!("wrote {}", p.display());
    }
    if !all_ok {
        exit(1);
    }
}

fn chrono_now() -> String {
    // No chrono dep: epoch seconds is enough for a report timestamp.
    let s = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    format!("unix:{s}")
}
