//! Minimal CLI for inspecting and editing a rexodb database directory.
//!
//! Usage: `rexodb-cli <dir> <command>` — run with no args for the list.

use std::ops::Bound;
use std::path::Path;
use std::process::ExitCode;

use bytes::Bytes;
use rexodb::{Config, Db};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (dir, cmd) = match (args.next(), args.next()) {
        (Some(d), Some(c)) if d != "--help" => (d, c),
        _ => {
            usage();
            return Ok(());
        }
    };
    let rest: Vec<String> = args.collect();

    // Read commands shouldn't create a fresh database on a typo'd path.
    let readonly = matches!(cmd.as_str(), "get" | "scan" | "stats");
    if readonly && !Path::new(&dir).join("seg").exists() && !Path::new(&dir).join("wal").exists() {
        return Err(format!("{dir}: not an existing rexodb directory").into());
    }

    let db = Db::open(Config::new(&dir))?;
    match cmd.as_str() {
        "get" => {
            let [key] = expect(&rest, "get <key>")?;
            match db.get(parse_bytes(key)?)? {
                Some(v) => println!("{}", display(&v)),
                None => return Err("key not found".into()),
            }
        }
        "put" => {
            let [key, val] = expect(&rest, "put <key> <value>")?;
            db.put(parse_bytes(key)?, parse_bytes(val)?)?;
        }
        "delete" => {
            let [key] = expect(&rest, "delete <key>")?;
            db.delete(parse_bytes(key)?)?;
        }
        "scan" => {
            let mut from = Bound::Unbounded;
            let mut to = Bound::Unbounded;
            let mut limit = usize::MAX;
            let mut i = 0;
            while i < rest.len() {
                let val = || rest.get(i + 1).map(String::as_str);
                match rest[i].as_str() {
                    "--from" => from = Bound::Included(parse_bytes(val().ok_or("--from needs a key")?)?),
                    "--to" => to = Bound::Excluded(parse_bytes(val().ok_or("--to needs a key")?)?),
                    "--limit" => limit = val().ok_or("--limit needs a number")?.parse()?,
                    other => return Err(format!("unknown scan flag {other}").into()),
                }
                i += 2;
            }
            let mut n = 0usize;
            for item in db.range((from, to))? {
                let (k, v) = item?;
                println!("{} = {}", display(&k), display(&v));
                n += 1;
                if n >= limit {
                    break;
                }
            }
            eprintln!("({n} keys)");
        }
        "stats" => {
            let s = db.stats();
            println!("{s:#?}");
        }
        "flush" => db.flush()?,
        "compact" => db.compact()?,
        other => return Err(format!("unknown command {other}").into()),
    }
    Ok(())
}

fn usage() {
    eprintln!(
        "rexodb-cli <dir> <command>\n\
         \n\
         commands:\n\
         \x20 get <key>\n\
         \x20 put <key> <value>\n\
         \x20 delete <key>\n\
         \x20 scan [--from K] [--to K) [--limit N]\n\
         \x20 stats\n\
         \x20 flush\n\
         \x20 compact\n\
         \n\
         keys/values are UTF-8; prefix with 0x for hex-encoded binary"
    );
}

fn expect<'a, const N: usize>(args: &'a [String], sig: &str) -> Result<[&'a str; N], String> {
    let arr: [&str; N] = args
        .iter()
        .take(N)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| format!("usage: {sig}"))?;
    Ok(arr)
}

/// `0x...` decodes as hex, everything else is UTF-8 bytes.
fn parse_bytes(s: &str) -> Result<Bytes, Box<dyn std::error::Error>> {
    match s.strip_prefix("0x") {
        None => Ok(Bytes::copy_from_slice(s.as_bytes())),
        Some(hex) => {
            if hex.len() % 2 != 0 {
                return Err("hex needs an even number of digits".into());
            }
            let bytes: Result<Vec<u8>, _> =
                (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16)).collect();
            Ok(Bytes::from(bytes?))
        }
    }
}

/// Printable UTF-8 shown as-is; anything else escaped as \xNN.
fn display(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) if s.chars().all(|c| !c.is_control()) => s.to_string(),
        _ => b.iter().map(|c| format!("\\x{c:02x}")).collect(),
    }
}
