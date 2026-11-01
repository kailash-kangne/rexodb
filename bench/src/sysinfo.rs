//! Environment capture (CPU, RAM, OS, toolchain, storage) and per-rep
//! resource usage via `getrusage`.
//!
//! Honesty notes:
//! - `max_rss` is a process high-water mark, not a delta.
//! - macOS's `ru_inblock`/`ru_oublock` are unreliable, so disk I/O is
//!   reported as logical bytes: the on-disk size of the db directory.

use std::process::Command;

#[derive(Debug, serde::Serialize)]
pub struct SysInfo {
    pub os: String,
    pub kernel: String,
    pub arch: String,
    pub cpu: String,
    pub ram_bytes: u64,
    pub rustc: String,
    pub profile: String,
    pub rustflags: String,
    pub storage: String,
    pub workdir: String,
}

fn sh(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

pub fn capture(workdir: &str) -> SysInfo {
    SysInfo {
        os: sh("sw_vers", &["-productName"])
            + " "
            + &sh("sw_vers", &["-productVersion"]),
        kernel: sh("uname", &["-r"]),
        arch: sh("uname", &["-m"]),
        cpu: sh("sysctl", &["-n", "machdep.cpu.brand_string"]),
        ram_bytes: sh("sysctl", &["-n", "hw.memsize"]).parse().unwrap_or(0),
        rustc: sh("rustc", &["--version"]),
        profile: "release opt-level=3 lto=thin codegen-units=1".into(),
        rustflags: std::env::var("RUSTFLAGS").unwrap_or_default(),
        storage: sh("sh", &["-c", "mount | grep ' on / '"]),
        workdir: workdir.into(),
    }
}

pub fn json(workdir: &str) -> String {
    serde_json::to_string_pretty(&capture(workdir)).unwrap()
}

/// rusage snapshot for delta-ing around a rep.
#[derive(Clone, Copy)]
pub struct Usage {
    pub user_ms: f64,
    pub sys_ms: f64,
    pub max_rss_kib: u64,
}

pub fn usage() -> Usage {
    unsafe {
        let mut r: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut r);
        Usage {
            user_ms: r.ru_utime.tv_sec as f64 * 1e3 + r.ru_utime.tv_usec as f64 / 1e3,
            sys_ms: r.ru_stime.tv_sec as f64 * 1e3 + r.ru_stime.tv_usec as f64 / 1e3,
            // macOS reports ru_maxrss in bytes, linux in kiB.
            max_rss_kib: if cfg!(target_os = "macos") {
                r.ru_maxrss as u64 / 1024
            } else {
                r.ru_maxrss as u64
            },
        }
    }
}

impl Usage {
    pub fn cpu_ms_since(&self, earlier: &Usage) -> f64 {
        (self.user_ms - earlier.user_ms) + (self.sys_ms - earlier.sys_ms)
    }
}

/// Total size of every regular file under `dir`.
pub fn dir_size(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(m) = e.metadata() {
                total += m.len();
            }
        }
    }
    total
}
