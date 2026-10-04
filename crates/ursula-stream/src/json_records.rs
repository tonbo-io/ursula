//! Canonical JSON message text (EXT "JSON Message Text").
//!
//! An `application/json` stream stores each message as one compact JSON
//! value followed by one LF, with no other LF. Create and append validate
//! that shape, and the message ends feed the usage counter
//! `committed_records`. Messages carry no ordinals: a JSON message
//! boundary is an offset whose preceding byte is LF.

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

/// The relative end offsets of the canonical messages in `payload`: one
/// per LF. Empty for any other content type and for an empty payload.
pub fn canonical_json_record_ends(
    content_type: &str,
    payload: &[u8],
) -> Result<Vec<u64>, NonCanonicalJsonPayload> {
    if !is_json_record_content_type(content_type) || payload.is_empty() {
        return Ok(Vec::new());
    }
    if payload.last() != Some(&b'\n') {
        return Err(NonCanonicalJsonPayload);
    }
    Ok(memchr::memchr_iter(b'\n', payload)
        .map(|index| u64::try_from(index.saturating_add(1)).expect("payload offset fits u64"))
        .collect())
}

/// Whether `record_ends` (from the HTTP layer, for an external payload) are
/// valid message ends of a `payload_len`-byte payload: none for a non-JSON
/// stream, and for a JSON stream strictly increasing, non-zero and ending
/// exactly at `payload_len`.
pub(crate) fn record_ends_valid(json: bool, payload_len: u64, record_ends: &[u64]) -> bool {
    if !json {
        return record_ends.is_empty();
    }
    if payload_len == 0 {
        return record_ends.is_empty();
    }
    record_ends.last() == Some(&payload_len)
        && record_ends.first().is_some_and(|first| *first > 0)
        && record_ends.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_ends_follow_each_lf() {
        assert_eq!(
            canonical_json_record_ends(
                "application/json; charset=utf-8",
                b"{\"a\":1}\n{\"b\":2}\n"
            ),
            Ok(vec![8, 16])
        );
        assert_eq!(
            canonical_json_record_ends("application/octet-stream", b"x"),
            Ok(Vec::new())
        );
        assert_eq!(
            canonical_json_record_ends("application/json", b"{}"),
            Err(NonCanonicalJsonPayload)
        );
        assert!(record_ends_valid(true, 16, &[8, 16]));
        assert!(!record_ends_valid(true, 16, &[8]));
        assert!(!record_ends_valid(true, 16, &[8, 8, 16]));
        assert!(!record_ends_valid(false, 16, &[16]));
    }
}
