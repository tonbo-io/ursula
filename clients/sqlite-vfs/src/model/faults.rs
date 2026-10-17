//! Scripted faults: what the network or a layer in front of the server does to the next matching
//! requests, in the order they were scripted.

use std::collections::VecDeque;

use ursula_headers::ErrorCode;

use crate::wire::Method;

/// What happens to a request instead of, or around, the server's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Fault {
    /// No connection is made: nothing goes out (`NotSent`). Over HTTP, the connection is closed at
    /// once, which the client sees as a lost answer.
    Unreachable,
    /// The request is lost before the server sees it: nothing applies, no answer.
    DropRequest,
    /// The server applies the request and the answer is lost.
    DropAnswer,
    /// A layer answers `status` in place of the server (a `5xx`, `429` or `408`), nothing applied.
    Status {
        status: u16,
        retry_after: Option<u64>,
    },
    /// A layer redirects with `307` to `location`, nothing applied.
    Redirect { location: String },
    /// A layer that does not implement the size refusals (extensions: Server Metadata) refuses the
    /// size with a `413` that names no limit.
    UntypedTooLarge,
    /// The server's answer comes after `ms` (over HTTP only; in process it comes at once).
    Delay { ms: u64 },
    /// A layer strips `Range`: a snapshot read gets the whole body, `200`.
    IgnoreRange,
    /// The node answers a parts commit or discard `503` with `Retry-After: 1` and this error
    /// body code, nothing applied.
    CommitRefused(CommitRefusal),
}

/// Why a node refuses a parts commit for now (extensions: Publish Snapshot): the client repeats it
/// as is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitRefusal {
    /// Every copy slot is taken: `SNAPSHOT_COMMITS_BUSY`.
    Busy,
    /// The node runs a commit or a discard of this stream incarnation now: one at a time,
    /// `SNAPSHOT_COMMIT_IN_PROGRESS`. Scripted on a `PUT` for a commit, on a `DELETE` for a
    /// discard.
    InProgress,
}

impl CommitRefusal {
    /// The code of the `503`'s error body.
    pub(crate) fn code(self) -> ErrorCode {
        match self {
            Self::Busy => ErrorCode::SnapshotCommitsBusy,
            Self::InProgress => ErrorCode::SnapshotCommitInProgress,
        }
    }
}

/// A fault script line that does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown fault `{0}`")]
pub(crate) struct FaultSyntax(pub(crate) String);

impl Fault {
    /// Parses one fault, as the control route takes it: `unreachable`, `drop-request`,
    /// `drop-answer`, `status <code> [<retry-after seconds>]`, `redirect <location>`,
    /// `untyped-413`, `delay <ms>`, `ignore-range`, `commits-busy` or `commit-in-progress`.
    pub(crate) fn parse(text: &str) -> Result<Self, FaultSyntax> {
        let syntax = || FaultSyntax(text.to_owned());
        let words: Vec<&str> = text.split_whitespace().collect();
        let number = |word: Option<&&str>| word.and_then(|word| word.parse().ok());
        match words.as_slice() {
            ["unreachable"] => Ok(Self::Unreachable),
            ["drop-request"] => Ok(Self::DropRequest),
            ["drop-answer"] => Ok(Self::DropAnswer),
            ["untyped-413"] => Ok(Self::UntypedTooLarge),
            ["ignore-range"] => Ok(Self::IgnoreRange),
            ["commits-busy"] => Ok(Self::CommitRefused(CommitRefusal::Busy)),
            ["commit-in-progress"] => Ok(Self::CommitRefused(CommitRefusal::InProgress)),
            ["status", code, rest @ ..] if rest.len() <= 1 => Ok(Self::Status {
                status: code.parse().map_err(|_not_a_status| syntax())?,
                retry_after: match rest.first() {
                    None => None,
                    some => Some(number(some).ok_or_else(syntax)?),
                },
            }),
            ["redirect", location] => Ok(Self::Redirect {
                location: (*location).to_owned(),
            }),
            ["delay", ms] => Ok(Self::Delay {
                ms: ms.parse().map_err(|_not_a_number| syntax())?,
            }),
            _ => Err(syntax()),
        }
    }
}

/// A fault for the next `times` requests with method `method` (any method when `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scripted {
    pub(crate) method: Option<Method>,
    pub(crate) fault: Fault,
    pub(crate) times: u32,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Faults(VecDeque<Scripted>);

impl Faults {
    pub(crate) fn push(&mut self, scripted: Scripted) {
        if scripted.times > 0 {
            self.0.push_back(scripted);
        }
    }

    /// The fault for a request with `method`: the first scripted one that matches, used once.
    pub(crate) fn take(&mut self, method: Method) -> Option<Fault> {
        let at = self
            .0
            .iter()
            .position(|s| s.method.is_none_or(|m| m == method))?;
        let scripted = self.0.get_mut(at)?;
        let fault = scripted.fault.clone();
        scripted.times = scripted.times.saturating_sub(1);
        if scripted.times == 0 {
            self.0.remove(at);
        }
        Some(fault)
    }
}

#[cfg(test)]
mod tests {
    use super::CommitRefusal;
    use super::Fault;
    use super::FaultSyntax;
    use super::Faults;
    use super::Scripted;
    use crate::wire::Method;

    #[test]
    fn faults_parse_from_their_script_lines() {
        assert_eq!(
            Fault::parse("status 503 2"),
            Ok(Fault::Status {
                status: 503,
                retry_after: Some(2),
            })
        );
        assert_eq!(
            Fault::parse("status 429"),
            Ok(Fault::Status {
                status: 429,
                retry_after: None,
            })
        );
        assert_eq!(
            Fault::parse("redirect http://a/s"),
            Ok(Fault::Redirect {
                location: "http://a/s".to_owned(),
            })
        );
        assert_eq!(Fault::parse("delay 20"), Ok(Fault::Delay { ms: 20 }));
        assert_eq!(Fault::parse("ignore-range"), Ok(Fault::IgnoreRange));
        assert_eq!(
            Fault::parse("commits-busy"),
            Ok(Fault::CommitRefused(CommitRefusal::Busy))
        );
        assert_eq!(
            Fault::parse("commit-in-progress"),
            Ok(Fault::CommitRefused(CommitRefusal::InProgress))
        );
        for bad in ["status x", "status 503 2 3", "explode"] {
            assert_eq!(Fault::parse(bad), Err(FaultSyntax(bad.to_owned())));
        }
    }

    #[test]
    fn scripted_faults_match_by_method_and_run_out() {
        let mut faults = Faults::default();
        faults.push(Scripted {
            method: Some(Method::Post),
            fault: Fault::DropAnswer,
            times: 2,
        });
        assert_eq!(faults.take(Method::Head), None);
        assert_eq!(faults.take(Method::Post), Some(Fault::DropAnswer));
        assert_eq!(faults.take(Method::Post), Some(Fault::DropAnswer));
        assert_eq!(faults.take(Method::Post), None);
    }
}
