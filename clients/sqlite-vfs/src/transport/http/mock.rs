//! A scripted HTTP/1.1 server on loopback for the transport tests: one answer per connection.

use std::io::ErrorKind;
use std::io::Read as _;
use std::io::Write as _;
use std::net::TcpListener;
use std::net::TcpStream;
use std::time::Duration;
use std::time::Instant;

/// A loopback port nothing listens on: connects to it are refused while the [`Refusing`]
/// lives.
pub(crate) fn refusing() -> (std::net::SocketAddr, Refusing) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let connection = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (accepted, _peer) = listener.accept().unwrap();
    // The listener's port, which bind chose: Linux's connect never hands a port bind chose to
    // another connect, so no test's own connect can take it as its source port and reach
    // itself. The accepted end keeps holding it once the listener is dropped here.
    (accepted.local_addr().unwrap(), Refusing {
        _connection: (connection, accepted),
    })
}

/// Holds the port of [`refusing`]: an end of an open loopback connection, so `bind(0)` never
/// returns it and nothing listens on it (a connect to it is refused).
/// A socket that is only bound would hold the port too, but macOS drops a connect to it
/// instead of refusing it, so the connect waits for its timeout.
pub(crate) struct Refusing {
    _connection: (TcpStream, TcpStream),
}

/// Answers one connection per answer, in order, and returns each request's head. An empty
/// answer closes the connection without one: the attempt's outcome is unknown. The request's
/// body is read whole before the answer: a connection closed with unread bytes is reset, which
/// can drop the answer before the client reads it. A connection it waits for over a minute
/// fails it, so a client that sends fewer requests than answers fails the test's `join`
/// instead of hanging it.
pub(crate) fn serve(answers: Vec<String>) -> (String, std::thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}/b/s", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        answers
            .into_iter()
            .enumerate()
            .map(|(i, answer)| {
                let mut conn = accepted(&listener, i);
                let mut request = Vec::new();
                let mut byte = [0u8; 1];
                while !request.ends_with(b"\r\n\r\n") {
                    conn.read_exact(&mut byte).unwrap();
                    request.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&request).into_owned();
                let len = request_header(&head, "content-length").map_or(0, |n| n.parse().unwrap());
                conn.read_exact(&mut vec![0u8; len]).unwrap();
                conn.write_all(answer.as_bytes()).unwrap();
                head
            })
            .collect()
    });
    (url, server)
}

/// The `i`-th connection on `listener`, which does not block, within a minute.
pub(crate) fn accepted(listener: &TcpListener, i: usize) -> TcpStream {
    let start = Instant::now();
    loop {
        match listener.accept() {
            Ok((conn, _)) => {
                conn.set_nonblocking(false).unwrap();
                return conn;
            }
            Err(e)
                if e.kind() == ErrorKind::WouldBlock
                    && start.elapsed() < Duration::from_secs(60) =>
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("no request {i}: {e}"),
        }
    }
}

/// An answer without a body: `head` is the status and the headers.
pub(crate) fn answer(head: &str) -> String {
    format!("HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n")
}

/// The value of header `name` in a request head `serve` returned.
pub(crate) fn request_header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.split("\r\n").find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}
