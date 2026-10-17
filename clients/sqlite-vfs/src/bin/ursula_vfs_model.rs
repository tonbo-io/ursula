//! `ursula-vfs-model`: the model of the Ursula 0.8 server over HTTP/1.1, for the e2e
//! suites. It listens on loopback, prints `{"port": <n>}` on one line of stdout once it accepts
//! connections, and serves until it is killed.
//!
//! Options: `--port <n>` (0, the default, picks a free port), `--cluster-id <id>` (`model`),
//! `--seed <n>` (1), which draws the history ids, and `--exit-with-stdin`, which also ends it when
//! its stdin closes, so a test harness that dies without stopping it takes it along.

use std::error::Error;
use std::io::Read;
use std::io::Write;
use std::net::Ipv4Addr;
use std::net::TcpListener;

fn main() -> Result<(), Box<dyn Error>> {
    let mut port: u16 = 0;
    let mut cluster_id = String::from("model");
    let mut seed: u64 = 1;
    let mut exit_with_stdin = false;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--exit-with-stdin" {
            exit_with_stdin = true;
            continue;
        }
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
    if exit_with_stdin {
        std::thread::spawn(|| {
            let mut sink = [0_u8; 256];
            let mut stdin = std::io::stdin().lock();
            // Until end of file or an error: either way the harness is gone.
            while matches!(stdin.read(&mut sink), Ok(n) if n > 0) {}
            std::process::exit(0);
        });
    }
    sqlite_ursula_vfs::model::serve_forever(listener, &cluster_id, seed)?;
    Ok(())
}
