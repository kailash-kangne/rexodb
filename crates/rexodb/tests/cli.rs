use std::process::Command;

const CLI: &str = env!("CARGO_BIN_EXE_rexodb-cli");

fn cli(dir: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(CLI).arg(dir).args(args).output().expect("failed to run rexodb-cli")
}

fn stdout(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn cli_put_get_scan_delete_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path();

    for (k, v) in [("a1", "v1"), ("a2", "v2"), ("b1", "v3")] {
        assert!(cli(db, &["put", k, v]).status.success());
    }
    assert_eq!(stdout(&cli(db, &["get", "a1"])), "v1\n");

    // scan prints sorted order, --to is exclusive
    let out = stdout(&cli(db, &["scan", "--to", "b1"]));
    assert_eq!(out, "a1 = v1\na2 = v2\n");

    // miss exits nonzero
    assert!(cli(db, &["delete", "a1"]).status.success());
    assert!(!cli(db, &["get", "a1"]).status.success());

    // hex keys and escaped binary output round-trip
    assert!(cli(db, &["put", "0xdead", "0xbeef"]).status.success());
    assert_eq!(stdout(&cli(db, &["get", "0xdead"])), "\\xbe\\xef\n");

    // reads refuse to create a db on a typo'd path
    assert!(!cli(&dir.path().join("nope"), &["get", "k"]).status.success());
}
