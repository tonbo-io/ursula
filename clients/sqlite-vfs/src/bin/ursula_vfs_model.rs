//! `ursula-vfs-model`: the model of the Ursula 0.8 server (#581) over HTTP/1.1, for the e2e
//! suites. It listens on loopback, prints `{"port": <n>}` on one line of stdout once it accepts
//! connections, and serves until it is killed.
//!
//! Options: `--port <n>` (0, the default, picks a free port), `--cluster-id <id>` (`model`) and
//! `--seed <n>` (1), which draws the history ids.

use std::error::Error;
use std::io::Write;
use std::net::Ipv4Addr;
use std::net::TcpListener;

fn main() -> Result<(), Box<dyn Error>> {
    let mut port: u16 = 0;
    let mut cluster_id = String::from("model");
    let mut seed: u64 = 1;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--port" => port = value.parse()?,
            "--cluster-id" => cluster_id = value,
            "--seed" => seed = value.parse()?,
            _ => return Err(format!("unknown option {flag}").into()),
        }
    }
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
    let port = listener.local_addr()?.port();
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{{\"port\": {port}}}")?;
    stdout.flush()?;
    drop(stdout);
    sqlite_ursula_vfs::model::serve_forever(listener, &cluster_id, seed)?;
    Ok(())
}
