//! JSON + Markdown report generation and baseline regression detection.

use std::collections::HashMap;
use std::path::Path;

use crate::kv::Res;
use crate::metrics::RepStats;
use crate::sysinfo::SysInfo;
use crate::workload::Work;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Scenario {
    pub db: String,
    pub workload: Work,
    pub key_size: usize,
    pub value_size: usize,
    pub threads: usize,
    pub durability: String,
    pub inmem: bool,
    pub preload: u64,
    pub ops: usize,
    pub seed: u64,
}

impl Scenario {
    /// Stable identity for baseline comparison.
    pub fn id(&self) -> String {
        format!(
            "{}|{}|k{}|v{}|t{}|{}|{}|p{}",
            self.db,
            self.workload.as_str(),
            self.key_size,
            self.value_size,
            self.threads,
            self.durability,
            if self.inmem { "mem" } else { "disk" },
            self.preload
        )
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ScenarioResult {
    #[serde(flatten)]
    pub scenario: Scenario,
    pub stats: RepStats,
    /// Regression % vs baseline throughput (negative = slower), if compared.
    pub regression_pct: Option<f64>,
}

#[derive(Debug, serde::Serialize)]
pub struct Report {
    pub sysinfo: SysInfo,
    pub generated_at: String,
    pub reps_requested: usize,
    pub results: Vec<ScenarioResult>,
    pub regressions: Vec<String>,
    pub notes: Vec<String>,
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1} µs", ns as f64 / 1e3)
    } else {
        format!("{ns} ns")
    }
}

fn fmt_ops(n: f64) -> String {
    if n >= 1e6 {
        format!("{:.2} M", n / 1e6)
    } else if n >= 1e3 {
        format!("{:.1} k", n / 1e3)
    } else {
        format!("{n:.0}")
    }
}

pub fn write_json(r: &Report, path: &Path) -> Res<()> {
    std::fs::write(path, serde_json::to_string_pretty(r)?)?;
    Ok(())
}

pub fn write_md(r: &Report, path: &Path) -> Res<()> {
    let mut s = String::new();
    s.push_str("# Benchmark report\n\n");
    s.push_str(&format!("- host: {} / {} / {}\n", r.sysinfo.cpu, r.sysinfo.os, r.sysinfo.kernel));
    s.push_str(&format!("- ram: {:.1} GiB, arch: {}\n", r.sysinfo.ram_bytes as f64 / (1 << 30) as f64, r.sysinfo.arch));
    s.push_str(&format!("- rustc: {}\n- profile: {}\n", r.sysinfo.rustc, r.sysinfo.profile));
    s.push_str(&format!("- storage: `{}`\n", r.sysinfo.storage));
    s.push_str(&format!("- generated: {}\n\n", r.generated_at));

    if !r.regressions.is_empty() {
        s.push_str("## Regressions vs baseline\n\n");
        for x in &r.regressions {
            s.push_str(&format!("- {x}\n"));
        }
        s.push('\n');
    }

    s.push_str("## Results\n\n");
    s.push_str("| db | workload | threads | k/v bytes | mode | dur | ops/s (median) | ±sd | p50 | p99 | p999 | rss MiB | disk MiB | out |\n");
    s.push_str("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n");
    for x in &r.results {
        let sc = &x.scenario;
        let st = &x.stats;
        s.push_str(&format!(
            "| {} | {} | {} | {}/{} | {} | {} | {} | {} | {} | {} | {} | {:.0} | {:.1} | {} |\n",
            sc.db,
            sc.workload.as_str(),
            sc.threads,
            sc.key_size,
            sc.value_size,
            if sc.inmem { "mem" } else { "disk" },
            sc.durability,
            fmt_ops(st.ops_per_sec_median),
            fmt_ops(st.ops_per_sec_stddev),
            fmt_ns(st.p50_ns),
            fmt_ns(st.p99_ns),
            fmt_ns(st.p999_ns),
            st.max_rss_kib as f64 / 1024.0,
            st.disk_bytes as f64 / (1 << 20) as f64,
            st.outliers,
        ));
    }
    s.push_str("\n## Notes\n\n");
    for n in &r.notes {
        s.push_str(&format!("- {n}\n"));
    }
    std::fs::write(path, s)?;
    Ok(())
}

// ------------------------------------------------------------ baseline

/// `scenario.id() -> ops_per_sec_median`
pub type Baseline = HashMap<String, f64>;

pub fn load_baseline(path: &Path) -> Res<Baseline> {
    let s = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&s)?)
}

pub fn save_baseline(results: &[ScenarioResult], path: &Path) -> Res<()> {
    let b: Baseline =
        results.iter().map(|r| (r.scenario.id(), r.stats.ops_per_sec_median)).collect();
    std::fs::write(path, serde_json::to_string_pretty(&b)?)?;
    Ok(())
}

/// Annotate results with regression vs baseline; returns human-readable
/// flags for every scenario slower than `threshold_pct` below baseline.
pub fn check_regressions(results: &mut [ScenarioResult], base: &Baseline, threshold_pct: f64) -> Vec<String> {
    let mut out = Vec::new();
    for r in results.iter_mut() {
        let Some(&b) = base.get(&r.scenario.id()) else { continue };
        if b <= 0.0 {
            continue;
        }
        let pct = (r.stats.ops_per_sec_median - b) / b * 100.0;
        r.regression_pct = Some(pct);
        if pct < -threshold_pct {
            out.push(format!("{}: {:.1}% slower ({:.0} -> {:.0} ops/s)", r.scenario.id(), -pct, b, r.stats.ops_per_sec_median));
        }
    }
    out
}
