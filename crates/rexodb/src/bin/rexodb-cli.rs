//! Minimal CLI for inspecting and editing a rexodb database directory.
//!
//! Usage: `rexodb-cli <dir> <command>` — run with no args for the list.

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::ops::Bound;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use bytes::Bytes;
use rexodb::{Config, Db, Stats};

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
        "ui" => {
            // 7396 = REXO on a T9 keypad (like Redis 6379 = MERZ)
            let mut port = 7396u16;
            let mut i = 0;
            while i < rest.len() {
                match rest[i].as_str() {
                    "--port" => port = rest.get(i + 1).ok_or("--port needs a number")?.parse()?,
                    other => return Err(format!("unknown ui flag {other}").into()),
                }
                i += 2;
            }
            serve_ui(db, port, &dir)?;
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
         \x20 ui [--port N]        (browser UI on 127.0.0.1, default port 7396)\n\
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
        Some(hex) => unhex(hex).map_err(Into::into),
    }
}

fn unhex(h: &str) -> Result<Bytes, String> {
    if !h.len().is_multiple_of(2) {
        return Err("hex needs an even number of digits".into());
    }
    (0..h.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&h[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .map(Bytes::from)
        .map_err(|e| e.to_string())
}

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::with_capacity(b.len() * 2), |mut s, c| {
        let _ = write!(s, "{c:02x}");
        s
    })
}

/// Printable UTF-8 shown as-is; anything else escaped as \xNN.
fn display(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) if s.chars().all(|c| !c.is_control()) => s.to_string(),
        _ => b.iter().map(|c| format!("\\x{c:02x}")).collect(),
    }
}

// ---- ui: localhost debug web page -----------------------------------------
// Minimal hand-rolled HTTP (std only). Keys/values travel as hex so binary
// round-trips losslessly and JSON output needs no escaping.

const PAGE: &str = include_str!("ui.html");
const LOGO: &[u8] = include_bytes!("logo.png");

fn serve_ui(db: Db, port: u16, dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
    eprintln!("rexodb ui → http://127.0.0.1:{port}/  (Ctrl-C to stop)");
    let db = Arc::new(db);
    let safe_dir = dir.replace('&', "&amp;").replace('<', "&lt;");
    let page = Arc::new(PAGE.replace("__DBDIR__", &safe_dir));
    for stream in listener.incoming().flatten() {
        let (db, page) = (Arc::clone(&db), Arc::clone(&page));
        thread::spawn(move || {
            let _ = handle_conn(stream, &db, &page);
        });
    }
    Ok(())
}

fn handle_conn(mut s: TcpStream, db: &Db, page: &str) -> Result<(), Box<dyn std::error::Error>> {
    const MAX_HEAD: usize = 64 * 1024;
    const MAX_BODY: usize = 64 * 1024 * 1024;
    let mut buf = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
        if buf.len() > MAX_HEAD {
            return respond(&mut s, 431, "text/plain", "headers too large");
        }
        let n = s.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let (method, target, body_len) = {
        let head = String::from_utf8_lossy(&buf[..head_end]);
        let mut lines = head.split("\r\n");
        let mut req = lines.next().unwrap_or_default().split_whitespace();
        let method = req.next().unwrap_or_default().to_owned();
        let target = req.next().unwrap_or_default().to_owned();
        let mut body_len = 0usize;
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().eq_ignore_ascii_case("content-length") {
                    body_len = v.trim().parse().unwrap_or(0);
                }
            }
        }
        (method, target, body_len)
    };
    if body_len > MAX_BODY {
        return respond(&mut s, 413, "text/plain", "body too large");
    }
    while buf.len() < head_end + body_len {
        let n = s.read(&mut chunk)?;
        if n == 0 {
            return respond(&mut s, 400, "text/plain", "truncated body");
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = &buf[head_end..head_end + body_len];
    let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
    // HEAD aliases GET — the page never sends it, but curl -I does
    let method = if method == "HEAD" { "GET" } else { method.as_str() };
    route(&mut s, db, method, path, query, body, page)
}

fn route(
    s: &mut TcpStream,
    db: &Db,
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
    page: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    const CT: (&str, &str) = ("text/plain", "application/json");
    let key = qparam(query, "k").map(unhex);
    match (method, path) {
        ("GET", "/") => respond(s, 200, "text/html", page),
        ("GET", "/logo.png") => respond(s, 200, "image/png", LOGO),
        ("GET", "/favicon.ico") => respond(s, 204, CT.0, ""),
        ("GET", "/api/stats") => respond(s, 200, CT.1, stats_json(&db.stats())),
        ("GET", "/api/get") => match key {
            Some(Ok(k)) => match db.get(k)? {
                Some(v) => respond(s, 200, CT.1, format!("\"{}\"", hex(&v))),
                None => respond(s, 404, CT.0, "key not found"),
            },
            Some(Err(e)) => respond(s, 400, CT.0, e),
            None => respond(s, 400, CT.0, "missing k"),
        },
        ("GET", "/api/scan") => {
            let (from, to) = match qparam(query, "prefix") {
                // prefix wins over raw from/to when both are sent
                Some(h) if !h.is_empty() => {
                    let p = unhex(h)?;
                    (Bound::Included(p.clone()), prefix_upper(&p))
                }
                _ => {
                    let from = match qparam(query, "from") {
                        Some(h) if !h.is_empty() => Bound::Included(unhex(h)?),
                        _ => Bound::Unbounded,
                    };
                    let to = match qparam(query, "to") {
                        Some(h) if !h.is_empty() => Bound::Excluded(unhex(h)?),
                        _ => Bound::Unbounded,
                    };
                    (from, to)
                }
            };
            let limit = qparam(query, "limit").and_then(|v| v.parse().ok()).unwrap_or(1000).min(100_000);
            let mut rows = String::from("[");
            let mut n = 0usize;
            for item in db.range((from, to))? {
                let (k, v) = item?;
                if n > 0 {
                    rows.push(',');
                }
                let _ = write!(rows, "[\"{}\",\"{}\"]", hex(&k), hex(&v));
                n += 1;
                if n >= limit {
                    break;
                }
            }
            rows.push(']');
            respond(s, 200, CT.1, rows)
        }
        ("POST", "/api/put") => match (key, std::str::from_utf8(body).ok().map(unhex)) {
            (Some(Ok(k)), Some(Ok(v))) => {
                db.put(k, v)?;
                respond(s, 200, CT.0, "ok")
            }
            (Some(Err(e)), _) | (_, Some(Err(e))) => respond(s, 400, CT.0, e),
            _ => respond(s, 400, CT.0, "missing k or body"),
        },
        ("POST", "/api/del") => match key {
            Some(Ok(k)) => {
                db.delete(k)?;
                respond(s, 200, CT.0, "ok")
            }
            Some(Err(e)) => respond(s, 400, CT.0, e),
            None => respond(s, 400, CT.0, "missing k"),
        },
        ("POST", "/api/flush") => {
            db.flush()?;
            respond(s, 200, CT.0, "ok")
        }
        ("POST", "/api/compact") => {
            db.compact()?;
            respond(s, 200, CT.0, "ok")
        }
        ("GET", "/api/bench") => respond(s, 200, CT.1, bench(db, query)?),
        _ => respond(s, 404, CT.0, "not found"),
    }
}

/// Times `ops` real puts then gets against the live db, then deletes the
/// bench keys. Hot-path numbers (memtable/cache), not a durability test.
fn bench(db: &Db, query: &str) -> Result<String, Box<dyn std::error::Error>> {
    let n: usize = qparam(query, "ops").and_then(|v| v.parse().ok()).unwrap_or(10_000).clamp(100, 1_000_000);
    let val = Bytes::from_static(b"rexodb-bench-payload-0000032b");
    let key = |i: usize| format!("__bench__{i:010}");

    let mut put_ns = Vec::with_capacity(n);
    for i in 0..n {
        let t = Instant::now();
        db.put(key(i).as_bytes(), &val)?;
        put_ns.push(t.elapsed().as_nanos() as u64);
    }
    let mut get_ns = Vec::with_capacity(n);
    for i in 0..n {
        let t = Instant::now();
        db.get(key(i).as_bytes())?;
        get_ns.push(t.elapsed().as_nanos() as u64);
    }
    for i in 0..n {
        db.delete(key(i).as_bytes())?;
    }

    let stats = |mut v: Vec<u64>| {
        v.sort_unstable();
        let total: u64 = v.iter().sum();
        (n as f64 / (total as f64 / 1e9), v[n / 2] as f64 / 1e3, v[n * 99 / 100] as f64 / 1e3)
    };
    let (put_rate, put_p50, put_p99) = stats(put_ns);
    let (get_rate, get_p50, get_p99) = stats(get_ns);
    Ok(format!(
        "{{\"ops\":{n},\"put_ops_sec\":{put_rate:.0},\"put_p50_us\":{put_p50:.2},\"put_p99_us\":{put_p99:.2},\
         \"get_ops_sec\":{get_rate:.0},\"get_p50_us\":{get_p50:.2},\"get_p99_us\":{get_p99:.2}}}"
    ))
}

/// Smallest key greater than every key starting with `p` — drop trailing
/// 0xFF bytes, bump the last remaining byte. All-0xFF prefixes are
/// unbounded above.
fn prefix_upper(p: &[u8]) -> Bound<Bytes> {
    let mut end = p.to_vec();
    while end.last() == Some(&0xFF) {
        end.pop();
    }
    match end.last_mut() {
        Some(b) => {
            *b += 1;
            Bound::Excluded(Bytes::from(end))
        }
        None => Bound::Unbounded,
    }
}

fn qparam<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|p| p.strip_prefix(name)?.strip_prefix('='))
}

fn stats_json(s: &Stats) -> String {
    format!(
        "{{\"puts\":{},\"deletes\":{},\"gets\":{},\"memtable_hits\":{},\"segment_reads\":{},\
         \"cache_hits\":{},\"batches\":{},\"flushes\":{},\"compactions\":{},\
         \"live_segments\":{},\"memtable_bytes\":{}}}",
        s.puts,
        s.deletes,
        s.gets,
        s.memtable_hits,
        s.segment_reads,
        s.cache_hits,
        s.batches,
        s.flushes,
        s.compactions,
        s.live_segments,
        s.memtable_bytes
    )
}

fn respond(
    s: &mut TcpStream,
    code: u16,
    ctype: &str,
    body: impl AsRef<[u8]>,
) -> Result<(), Box<dyn std::error::Error>> {
    let reason = match code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        _ => "Error",
    };
    let body = body.as_ref();
    write!(
        s,
        "HTTP/1.1 {code} {reason}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\ncache-control: no-store\r\nconnection: close\r\n\r\n",
        body.len()
    )?;
    s.write_all(body)?;
    Ok(())
}
