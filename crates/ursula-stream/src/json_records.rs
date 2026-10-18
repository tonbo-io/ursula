//! Canonical JSON message text (EXT "JSON Message Text").
//!
//! An `application/json` stream stores each message as one compact JSON
//! value followed by one LF, with no other LF. Create and append validate
//! that shape, and the message count feeds the usage counter
//! `committed_records`. Messages carry no ordinals: a JSON message
//! boundary is an offset whose preceding byte is LF.

use crate::command::len_u64;

/// Whether `content_type` is `application/json` (parameters ignored).
pub fn is_json_record_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

/// A JSON payload that does not end with LF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonCanonicalJsonPayload;

/// The number of canonical messages in `payload`: one per LF. Zero for any
/// other content type and for an empty payload. The LF bytes are the
/// message boundaries, so a count is all a log entry needs to carry.
pub fn canonical_json_message_count(
    content_type: &str,
    payload: &[u8],
) -> Result<u64, NonCanonicalJsonPayload> {
    if !is_json_record_content_type(content_type) || payload.is_empty() {
        return Ok(0);
    }
    if payload.last() != Some(&b'\n') {
        return Err(NonCanonicalJsonPayload);
    }
    Ok(len_u64(memchr::memchr_iter(b'\n', payload).count()))
}

/// Whether `message_count` (from the HTTP layer, for an external payload)
/// can be the message count of a `payload_len`-byte canonical payload: zero
/// for a non-JSON stream or an empty payload, and for a JSON stream at least
/// one and at most `payload_len / 2`, since a canonical message is at least
/// one value byte plus its LF.
pub(crate) fn message_count_valid(json: bool, payload_len: u64, message_count: u64) -> bool {
    if !json || payload_len == 0 {
        return message_count == 0;
    }
    (1..=payload_len / 2).contains(&message_count)
}

#[cfg(test)]
mod tests {
    use super::NonCanonicalJsonPayload;
    use super::canonical_json_message_count;
    use super::message_count_valid;

    #[test]
    fn canonical_messages_count_each_lf() {
        assert_eq!(
            canonical_json_message_count(
                "application/json; charset=utf-8",
                b"{\"a\":1}\n{\"b\":2}\n"
            ),
            Ok(2)
        );
        assert_eq!(
            canonical_json_message_count("application/octet-stream", b"x"),
            Ok(0)
        );
        assert_eq!(canonical_json_message_count("application/json", b""), Ok(0));
        assert_eq!(
            canonical_json_message_count("application/json", b"{}"),
            Err(NonCanonicalJsonPayload)
        );
    }

    #[test]
    fn message_counts_are_bounded_by_the_payload() {
        assert!(message_count_valid(true, 16, 8));
        assert!(message_count_valid(true, 16, 1));
        assert!(!message_count_valid(true, 16, 9));
        assert!(!message_count_valid(true, 16, 0));
        assert!(!message_count_valid(false, 16, 1));
        assert!(message_count_valid(false, 16, 0));
        assert!(message_count_valid(true, 0, 0));
        assert!(!message_count_valid(true, 0, 1));
    }
}
