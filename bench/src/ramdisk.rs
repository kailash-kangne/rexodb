//! RAM-disk helper for in-memory benchmarks and the disk-full test.
//! macOS: `hdiutil` ram device + `diskutil erasevolume` (auto-mounts).
//! Linux: uses `/dev/shm` directly (already tmpfs).

use std::path::PathBuf;
use std::process::Command;

use crate::kv::Res;

pub struct Ramdisk {
    #[allow(dead_code)]
    dev: String,
    pub mount: PathBuf,
}

#[cfg(target_os = "macos")]
fn run(cmd: &str, args: &[&str]) -> Res<String> {
    let o = Command::new(cmd).args(args).output()?;
    if !o.status.success() {
        return Err(format!("{cmd} {:?}: {}", args, String::from_utf8_lossy(&o.stderr)).into());
    }
    Ok(String::from_utf8_lossy(&o.stdout).trim().to_string())
}

#[cfg(target_os = "macos")]
pub fn mount(mb: u64, name: &str) -> Res<Ramdisk> {
    let dev = run("hdiutil", &["attach", "-nomount", &format!("ram://{}", mb * 2048)])?;
    run("diskutil", &["erasevolume", "HFS+", name, &dev])?;
    Ok(Ramdisk { dev, mount: PathBuf::from(format!("/Volumes/{name}")) })
}

#[cfg(target_os = "linux")]
pub fn mount(mb: u64, name: &str) -> Res<Ramdisk> {
    let mount = PathBuf::from("/dev/shm").join(name);
    std::fs::create_dir_all(&mount)?;
    let _ = mb; // /dev/shm sizing is kernel-controlled; caller must not exceed it
    Ok(Ramdisk { dev: "tmpfs".into(), mount })
}

impl Drop for Ramdisk {
    fn drop(&mut self) {
        #[cfg(target_os = "macos")]
        let _ = Command::new("hdiutil").args(["detach", &self.dev, "-force"]).output();
        #[cfg(target_os = "linux")]
        let _ = std::fs::remove_dir_all(&self.mount);
    }
}
