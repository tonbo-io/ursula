//! The stream, over blocking HTTP: appends with the idempotent producer, reads, `HEAD`,
//! snapshot and retention requests, and their retries.

use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use crate::config::retry_budget;
use crate::error::Attempt;
use crate::error::Error;
use crate::error::Gone;

/// `Producer-Id` prefix; the stream incarnation follows (`producer_id`).
const PRODUCER: &str = "sqlite-ursula-vfs";

const CONTENT_TYPE: &str = "application/octet-stream";

/// The protocol's offset for the beginning of a stream, and "none" for an offset that may be absent
/// (a snapshot, retention, the local state): it sorts before every offset the server mints.
pub(crate) const START: &str = "-1";

fn agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .http_status_as_error(false)
            .build()
            .into()
    })
}

fn header_u64(r: &ureq::http::Response<ureq::Body>, name: &str) -> Option<u64> {
    r.headers().get(name)?.to_str().ok()?.trim().parse().ok()
}

/// An offset as the server wrote it: opaque, kept verbatim and compared only lexicographically
/// (never parsed or computed with). `None` unless it is usable in a URL path or query and a
/// sidecar as it is: 1 to 64 unreserved URL characters.
pub(crate) fn offset_token(v: &str) -> Option<String> {
    let ok = |b: u8| b.is_ascii_alphanumeric() || b"-._~".contains(&b);
    (!v.is_empty() && v.len() <= 64 && v.bytes().all(ok)).then(|| v.to_owned())
}

pub(crate) fn header_offset(r: &ureq::http::Response<ureq::Body>, name: &str) -> Option<String> {
    offset_token(r.headers().get(name)?.to_str().ok()?.trim())
}

/// Waits before the next retry: no sooner than `retry_after`, and at least `backoff`, which then
/// doubles (up to 1 s). Returns false, without waiting, when the wait would end past `deadline`
/// (or overflow, for an absurd Retry-After).
pub(crate) fn pause(
    retry_after: Option<Duration>,
    backoff: &mut Duration,
    deadline: Instant,
) -> bool {
    let wait = retry_after.map_or(*backoff, |after| after.max(*backoff));
    if Instant::now()
        .checked_add(wait)
        .is_none_or(|end| end > deadline)
    {
        return false;
    }
    std::thread::sleep(wait);
    *backoff = (*backoff * 2).min(Duration::from_secs(1));
    true
}

/// Sends a read, retrying 429 (rate limiting) and 503 (overload, or a `consistency=leader` read,
/// HEAD or snapshot read the leader could not confirm with a quorum in time) the way `append`
/// does: no sooner than Retry-After (seconds), with backoff, within `retry_budget()`. Returns the
/// first other answer, or the last 429/503 once the budget is spent or `stopped` (a re-attach is
/// waiting for the snapshot thread).
fn read_retrying(
    send: impl Fn() -> Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    stopped: &dyn Fn() -> bool,
) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    let deadline = Instant::now() + retry_budget();
    let mut backoff = Duration::from_millis(20);
    loop {
        let r = send()?;
        if !matches!(r.status().as_u16(), 429 | 503) {
            return Ok(r);
        }
        let retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
        if stopped() || !pause(retry_after, &mut backoff, deadline) {
            return Ok(r);
        }
    }
}

pub(crate) enum Append {
    /// Applied (or a duplicate of an applied append); the stream offset after it when known.
    Acked { next: Option<String>, attempts: u32 },
    /// 403: a newer epoch owns the stream.
    Fenced { current: Option<u64> },
    /// 412: the stream is no longer the incarnation the append was sent to (deleted and
    /// recreated); nothing was appended.
    Recreated,
    /// 409 expecting sequence 0: the server does not know this producer: it expired it after 7
    /// idle days (a recreated stream answers 412 first).
    ProducerExpired,
    /// 409 refusing the commit's `Stream-Seq` (not above the stream's last one): another writer
    /// appended with a higher one (see `stream_seq`). `body` is the server's explanation.
    SeqConflict { body: String },
    /// A definite rejection, or no answer within the retry budget.
    Failed(Error),
}

/// The `Producer-Id` of every owner of one stream incarnation: owners of the same incarnation fence
/// each other by epoch (a recreated stream refuses an owner of the deleted one with 412, see
/// `append`).
pub(crate) fn producer_id(incarnation: &str) -> String {
    format!("{PRODUCER}/{incarnation}")
}

/// One idempotent append by `producer` to stream incarnation `incarnation` (sent as the
/// `Stream-Incarnation` precondition): retried with the same producer sequence until the outcome
/// is known.
///
/// A duplicate answer is taken as proof of *our* earlier attempt only for commits (seq >= 1), and
/// only with its receipt (`Stream-Next-Offset`, checked by `commit`): they are sent after a
/// verified claim (see `claim_once`), which makes this owner the only writer of its incarnation's
/// producer at its epoch, so whatever holds (epoch, seq) there is ours. A claim's answer is
/// verified separately. Commits also carry their `Stream-Seq` (see `stream_seq`).
pub(crate) fn append(
    url: &str,
    incarnation: &str,
    producer: &str,
    body: &[u8],
    epoch: u64,
    seq: u64,
) -> Append {
    let deadline = Instant::now() + retry_budget();
    let mut backoff = Duration::from_millis(20);
    let mut attempts = 0;
    loop {
        attempts += 1;
        let mut req = agent()
            .post(url)
            .header("content-type", CONTENT_TYPE)
            .header("stream-incarnation", incarnation)
            .header("producer-id", producer)
            .header("producer-epoch", epoch.to_string())
            .header("producer-seq", seq.to_string());
        if seq > 0 {
            req = req.header("stream-seq", stream_seq((epoch, seq)));
        }
        let sent = req.send(body);
        let mut retry_after = None;
        let unknown = match sent {
            Ok(mut r) => {
                let status = r.status().as_u16();
                // Rate limiting (429, e.g. ursulagw) and overload (503) are transient: retried
                // with the same producer sequence, no sooner than Retry-After (seconds).
                retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
                match status {
                    200..=299 => {
                        return Append::Acked {
                            next: header_offset(&r, "stream-next-offset"),
                            attempts,
                        };
                    }
                    403 => {
                        return Append::Fenced {
                            current: header_u64(&r, "producer-epoch"),
                        };
                    }
                    // The stream was deleted and recreated: nothing was appended.
                    412 => return Append::Recreated,
                    409 if seq > 0 && header_u64(&r, "producer-expected-seq") == Some(0) => {
                        return Append::ProducerExpired;
                    }
                    // Neither a producer sequence conflict nor a closed stream: the `Stream-Seq`.
                    409 if seq > 0
                        && !r.headers().contains_key("producer-expected-seq")
                        && !r.headers().contains_key("stream-closed") =>
                    {
                        return Append::SeqConflict {
                            body: body_text(&mut r),
                        };
                    }
                    429 => Attempt::Status {
                        status,
                        body: body_text(&mut r),
                    },
                    400..=499 => {
                        return Append::Failed(Error::AppendRejected {
                            status,
                            body: body_text(&mut r),
                        });
                    }
                    _ => Attempt::Status {
                        status,
                        body: body_text(&mut r),
                    },
                }
            }
            Err(e) => Attempt::Transport(Box::new(e)),
        };
        if !pause(retry_after, &mut backoff, deadline) {
            return Append::Failed(Error::AppendUnknown {
                attempts,
                last: unknown,
            });
        }
    }
}

pub(crate) fn create_stream(url: &str) -> Result<(), Error> {
    let mut r = agent()
        .put(url)
        .header("content-type", CONTENT_TYPE)
        .send_empty()
        .map_err(|e| http_error("create", url, e))?;
    let status = r.status().as_u16();
    let body = body_text(&mut r);
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(Error::Status {
            op: "create",
            url: url.to_owned(),
            status,
            body,
        })
    }
}

/// A request that got no answer.
fn http_error(op: &'static str, url: &str, e: ureq::Error) -> Error {
    Error::Http {
        op,
        url: url.to_owned(),
        source: Box::new(e),
    }
}

/// A response's body as text, for an error message (empty when it cannot be read).
fn body_text(r: &mut ureq::http::Response<ureq::Body>) -> String {
    r.body_mut().read_to_string().unwrap_or_default()
}

/// One read from `offset` of stream incarnation `incarnation` (a 412 when it is not, see
/// `recreated`): the bytes and the server's offset after them (empty at the tail). Reads
/// the leader's applied state: a follower may lag behind an acknowledged append (a claim, a
/// commit). A 200 without a usable `Stream-Next-Offset` is an error: offsets are never computed.
pub(crate) fn read_from(
    url: &str,
    incarnation: &str,
    offset: &str,
) -> Result<(Vec<u8>, String), Error> {
    let request = format!("{url}?offset={offset}&consistency=leader");
    let mut r = read_retrying(
        || {
            agent()
                .get(request.as_str())
                .header("stream-incarnation", incarnation)
                .call()
        },
        &|| false,
    )
    .map_err(|e| http_error("read", &request, e))?;
    let status = r.status().as_u16();
    if status == 412 {
        return Err(recreated(url, incarnation));
    }
    let next = header_offset(&r, "stream-next-offset");
    if status == 204 {
        return Ok((Vec::new(), next.unwrap_or_else(|| offset.to_owned())));
    }
    let body = r
        .body_mut()
        .with_config()
        .limit(1 << 30)
        .read_to_vec()
        .map_err(|e| http_error("read the body of", &request, e))?;
    if status == 410 {
        return Err(Error::Gone(Gone::BelowRetention {
            url: url.to_owned(),
            offset: offset.to_owned(),
        }));
    }
    if status == 416 {
        // The server answers 416, not 412, for a stream recreated shorter than `offset` (RFC 9110
        // §13.2.1): only a HEAD tells the two apart, and a recreate rebuilds.
        if head(url, &|| false).is_ok_and(|h| h.incarnation.as_deref() != Some(incarnation)) {
            return Err(recreated(url, incarnation));
        }
        return Err(Error::LostData {
            url: url.to_owned(),
            offset: offset.to_owned(),
        });
    }
    if status != 200 {
        return Err(Error::Status {
            op: "read",
            url: request,
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    let Some(next) = next else {
        return Err(Error::NoNextOffset {
            op: "read",
            url: request,
        });
    };
    Ok((body, next))
}

/// A read loop's step: `next`, answered by a read at `at` that returned bytes, must sort past `at`.
/// Checked in the loops only, where both are server offsets (or `START`): an older sidecar's
/// unpadded offset, read once to probe for a 416, compares meaninglessly.
pub(crate) fn advanced(url: &str, at: &str, len: usize, next: &str) -> Result<(), Error> {
    if next <= at {
        return Err(Error::OffsetNotAdvanced {
            url: url.to_owned(),
            at: at.to_owned(),
            len,
            next: next.to_owned(),
        });
    }
    Ok(())
}

/// Snapshot transfers move whole databases: a longer timeout than appends.
fn bulk_agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(120)))
            .http_status_as_error(false)
            .build()
            .into()
    })
}

pub(crate) struct Head {
    /// `START` when absent.
    pub(crate) retained: String,
    /// `None` when absent or `-1` (no snapshot).
    pub(crate) snapshot: Option<String>,
    /// `Stream-Incarnation`: opaque, changes when the stream is deleted and recreated; compared
    /// for equality only. `None` when absent (or unusable in a sidecar or `Producer-Id`): attach
    /// refuses.
    pub(crate) incarnation: Option<String>,
}

/// `stopped` ends the retries of a 429/503 early (see `read_retrying`).
pub(crate) fn head(url: &str, stopped: &dyn Fn() -> bool) -> Result<Head, Error> {
    let r = read_retrying(|| agent().head(url).call(), stopped)
        .map_err(|e| http_error("head", url, e))?;
    let status = r.status().as_u16();
    if status != 200 {
        return Err(Error::Status {
            op: "head",
            url: url.to_owned(),
            status,
            body: String::new(),
        });
    }
    Ok(Head {
        retained: header_offset(&r, "stream-retained-offset").unwrap_or_else(|| START.into()),
        snapshot: header_offset(&r, "stream-snapshot-offset").filter(|s| s != START),
        incarnation: r
            .headers()
            .get("stream-incarnation")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty() && !v.contains(char::is_whitespace))
            .map(str::to_owned),
    })
}

/// The snapshot at `offset`; `None` when it does not exist (superseded, or not yet visible here).
/// `stopped` ends the retries of a 429/503 early (see `read_retrying`).
pub(crate) fn get_snapshot(
    url: &str,
    incarnation: &str,
    offset: &str,
    stopped: &dyn Fn() -> bool,
) -> Result<Option<Vec<u8>>, Error> {
    let request = format!("{url}/snapshot/{offset}");
    let mut r = read_retrying(
        || {
            bulk_agent()
                .get(request.as_str())
                .header("stream-incarnation", incarnation)
                .call()
        },
        stopped,
    )
    .map_err(|e| http_error("get snapshot", &request, e))?;
    let status = r.status().as_u16();
    if status == 412 {
        return Err(recreated(url, incarnation));
    }
    // A body cut short: the snapshot was superseded and its cold object deleted (after its grace)
    // while it was streaming, or the connection dropped. Either way, start again from `HEAD`.
    let Ok(body) = r.body_mut().with_config().limit(2 << 30).read_to_vec() else {
        return Ok(None);
    };
    match status {
        200 => Ok(Some(body)),
        404 | 410 => Ok(None),
        _ => Err(Error::Status {
            op: "get snapshot",
            url: request,
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        }),
    }
}

/// `PUT` to stream incarnation `incarnation` (a 412 when it is not) with retries while the outcome
/// is unknown (transport errors, 5xx): both publishing a snapshot and advancing retention are
/// idempotent. Returns the status and response. Gives up once `stopped` (a re-attach is waiting
/// for the snapshot thread).
pub(crate) fn put_idempotent(
    url: &str,
    incarnation: &str,
    body: &[u8],
    stopped: &dyn Fn() -> bool,
) -> Result<(u16, ureq::http::Response<ureq::Body>), Error> {
    let deadline = Instant::now() + retry_budget();
    let mut backoff = Duration::from_millis(50);
    loop {
        let unknown = match bulk_agent()
            .put(url)
            .header("content-type", CONTENT_TYPE)
            .header("stream-incarnation", incarnation)
            .send(body)
        {
            Ok(r) if r.status().as_u16() < 500 => return Ok((r.status().as_u16(), r)),
            Ok(mut r) => Attempt::Status {
                status: r.status().as_u16(),
                body: body_text(&mut r),
            },
            Err(e) => Attempt::Transport(Box::new(e)),
        };
        if Instant::now() + backoff > deadline || stopped() {
            return Err(Error::PutUnknown {
                url: url.to_owned(),
                last: unknown,
            });
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

/// The error for a 412: `url` is no longer stream incarnation `incarnation`.
pub(crate) fn recreated(url: &str, incarnation: &str) -> Error {
    Error::Recreated {
        url: url.to_owned(),
        incarnation: incarnation.to_owned(),
    }
}

/// A commit's `Stream-Seq`: the owner's (epoch, producer sequence), each zero-padded to 20 digits,
/// so it sorts like the pair. Owners of one incarnation claim ever higher epochs and number commits
/// upwards within one, so every commit's is above every earlier commit's: the server's check
/// (refused unless above the stream's last `Stream-Seq`) then fences a writer outside this protocol
/// that appended with a higher one since this owner's last commit (one with a lower or equal
/// `Stream-Seq` is itself refused). Writers without `Stream-Seq` go unnoticed: offsets are opaque,
/// so the VFS does not check where its frame landed.
pub(crate) fn stream_seq((epoch, seq): (u64, u64)) -> String {
    format!("{epoch:020}{seq:020}")
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::io::Write as _;
    use std::net::TcpListener;

    use super::get_snapshot;
    use super::head;
    use super::read_from;
    use crate::error::Error;

    // A leader read the server could not confirm with a quorum in time answers 503 (Retry-After),
    // and a gateway may rate-limit with 429: catch-up reads and HEAD retry both, as appends do,
    // so attach, the claim check and reclaim never fail on them.
    #[test]
    fn reads_retry_transient_unavailability() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let answer = |head: &str, body: &str| {
            format!(
                "HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let unavailable = answer(
            "503 Service Unavailable\r\nretry-after: 0",
            "leader unknown",
        );
        let answers = [
            unavailable.clone(),
            answer("429 Too Many Requests", ""),
            answer("200 OK\r\nstream-next-offset: 7", "abc"),
            unavailable.clone(),
            answer("200 OK\r\nstream-retained-offset: 2", ""),
            // A Retry-After too large to wait for (or even add to now) ends the retries at once.
            answer(
                "503 Service Unavailable\r\nretry-after: 18446744073709551615",
                "leader unknown",
            ),
            // So does the stop flag (a re-attach waiting for the snapshot thread).
            unavailable,
        ];
        let server = std::thread::spawn(move || {
            answers
                .into_iter()
                .map(|answer| {
                    let (mut conn, _) = listener.accept().unwrap();
                    let mut request = Vec::new();
                    let mut byte = [0u8; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        conn.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    conn.write_all(answer.as_bytes()).unwrap();
                    let request = String::from_utf8_lossy(&request);
                    request.split(" HTTP/").next().unwrap().to_owned()
                })
                .collect::<Vec<_>>()
        });
        let (bytes, next) = read_from(&url, "1", "4").unwrap();
        assert_eq!((&bytes[..], next.as_str()), (&b"abc"[..], "7"));
        assert_eq!(head(&url, &|| false).unwrap().retained, "2");
        let refused = get_snapshot(&url, "1", "9", &|| false).unwrap_err();
        assert!(
            matches!(refused, Error::Status { status: 503, .. }),
            "{refused}"
        );
        let refused = head(&url, &|| true).err().unwrap();
        assert!(
            matches!(refused, Error::Status { status: 503, .. }),
            "{refused}"
        );
        let read = "GET /b/s?offset=4&consistency=leader";
        assert_eq!(server.join().unwrap(), [
            read,
            read,
            read,
            "HEAD /b/s",
            "HEAD /b/s",
            "GET /b/s/snapshot/9",
            "HEAD /b/s"
        ]);
    }
}
