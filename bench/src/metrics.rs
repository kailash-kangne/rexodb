//! Per-op latency recording and cross-repetition statistics.

use std::time::Duration;

/// Nanosecond latencies for one rep. Stored as u64 ns; ~50k ops = 400KB.
#[derive(Default)]
pub struct Lat {
    ns: Vec<u64>,
}

impl Lat {
    pub fn with_capacity(n: usize) -> Self {
        Self { ns: Vec::with_capacity(n) }
    }
    pub fn rec(&mut self, d: Duration) {
        self.ns.push(d.as_nanos() as u64);
    }
    pub fn merge(&mut self, other: Lat) {
        self.ns.extend_from_slice(&other.ns);
    }
    /// (p50, p95, p99, p999) in nanoseconds.
    pub fn percentiles(&self) -> [u64; 4] {
        if self.ns.is_empty() {
            return [0; 4];
        }
        let mut v = self.ns.clone();
        v.sort_unstable();
        let pick = |p: f64| v[((v.len() - 1) as f64 * p) as usize];
        [pick(0.50), pick(0.95), pick(0.99), pick(0.999)]
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RepResult {
    pub ops: u64,
    pub wall_secs: f64,
    pub ops_per_sec: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    /// user+sys CPU consumed by the process during the rep.
    pub cpu_ms: f64,
    /// Peak RSS (kiB) at rep end — a max-watermark, not a delta.
    pub max_rss_kib: u64,
    /// On-disk size of the db directory at rep end.
    pub disk_bytes: u64,
}

/// Aggregate across reps: median throughput, variability, outliers.
#[derive(Debug, serde::Serialize)]
pub struct RepStats {
    pub reps: usize,
    pub ops_per_sec_median: f64,
    pub ops_per_sec_min: f64,
    pub ops_per_sec_max: f64,
    pub ops_per_sec_stddev: f64,
    /// Reps outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR].
    pub outliers: usize,
    /// Median of each rep's percentiles.
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub cpu_ms_median: f64,
    pub max_rss_kib: u64,
    pub disk_bytes: u64,
}

fn sorted(v: &[f64]) -> Vec<f64> {
    let mut v = v.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v
}

fn median(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let s = sorted(v);
    s[s.len() / 2]
}

fn median_u64(v: &[u64]) -> u64 {
    if v.is_empty() {
        return 0;
    }
    let mut s = v.to_vec();
    s.sort_unstable();
    s[s.len() / 2]
}

pub fn aggregate(reps: &[RepResult]) -> RepStats {
    if reps.is_empty() {
        return RepStats {
            reps: 0,
            ops_per_sec_median: 0.0,
            ops_per_sec_min: 0.0,
            ops_per_sec_max: 0.0,
            ops_per_sec_stddev: 0.0,
            outliers: 0,
            p50_ns: 0,
            p95_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            cpu_ms_median: 0.0,
            max_rss_kib: 0,
            disk_bytes: 0,
        };
    }
    let thr: Vec<f64> = reps.iter().map(|r| r.ops_per_sec).collect();
    let med = median(&thr);
    let mean = thr.iter().sum::<f64>() / thr.len().max(1) as f64;
    let var = thr.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / thr.len().saturating_sub(1).max(1) as f64;
    let s = sorted(&thr);
    let (q1, q3) = (s[s.len() / 4], s[3 * s.len() / 4]);
    let iqr = q3 - q1;
    let outliers = thr.iter().filter(|&&x| x < q1 - 1.5 * iqr || x > q3 + 1.5 * iqr).count();
    RepStats {
        reps: reps.len(),
        ops_per_sec_median: med,
        ops_per_sec_min: *s.first().unwrap_or(&0.0),
        ops_per_sec_max: *s.last().unwrap_or(&0.0),
        ops_per_sec_stddev: var.sqrt(),
        outliers,
        p50_ns: median_u64(&reps.iter().map(|r| r.p50_ns).collect::<Vec<_>>()),
        p95_ns: median_u64(&reps.iter().map(|r| r.p95_ns).collect::<Vec<_>>()),
        p99_ns: median_u64(&reps.iter().map(|r| r.p99_ns).collect::<Vec<_>>()),
        p999_ns: median_u64(&reps.iter().map(|r| r.p999_ns).collect::<Vec<_>>()),
        cpu_ms_median: median(&reps.iter().map(|r| r.cpu_ms).collect::<Vec<_>>()),
        max_rss_kib: reps.iter().map(|r| r.max_rss_kib).max().unwrap_or(0),
        disk_bytes: median_u64(&reps.iter().map(|r| r.disk_bytes).collect::<Vec<_>>()),
    }
}
