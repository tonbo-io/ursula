//! Object-store layout of keyed-state projection namespaces (keyed-streams
//! §3.4 step 3): `.keyed/{bucket}/{key}/{incarnation:016x}/v{fmt}/…`, where
//! `key` is the stream's bucket-local name (`stream` or `affinity/stream`)
//! percent-encoded as one path component (`%` → `%25`, `/` → `%2F`).
//!
//! `.keyed/` is a top-level prefix. A bucket ID cannot contain `.`, so no
//! bucket's erasure domain `{bucket}/` collides with it. The node uses these
//! helpers for stream-delete GC (U22) and bucket purge (U23); the indexer
//! writes below the same prefixes.

use crate::BucketStreamId;

/// Top-level object-store directory of every keyed-state namespace.
pub const KEYED_NAMESPACE_ROOT: &str = ".keyed/";

/// The stream's bucket-local name (`stream`, or `affinity/stream`) encoded as
/// one path component: `%` becomes `%25` and `/` becomes `%2F`.
pub fn keyed_namespace_key(stream_id: &BucketStreamId) -> String {
    let local = match &stream_id.affinity_key {
        Some(affinity) => format!("{affinity}/{}", stream_id.stream_id),
        None => stream_id.stream_id.clone(),
    };
    encode_key_component(&local)
}

/// Percent-encodes `%` and `/` so a bucket-local stream name is one path
/// component.
pub fn encode_key_component(local_name: &str) -> String {
    let mut encoded = String::with_capacity(local_name.len());
    for ch in local_name.chars() {
        match ch {
            '%' => encoded.push_str("%25"),
            '/' => encoded.push_str("%2F"),
            other => encoded.push(other),
        }
    }
    encoded
}

/// `.keyed/{bucket}/`: every namespace of one bucket. Bucket purge erases it.
pub fn keyed_bucket_prefix(bucket_id: &str) -> String {
    format!("{KEYED_NAMESPACE_ROOT}{bucket_id}/")
}

/// `.keyed/{bucket}/{key}/{incarnation:016x}/`: every projection format of
/// one stream incarnation. Stream-delete GC removes it as a prefix.
pub fn keyed_incarnation_prefix(stream_id: &BucketStreamId, incarnation: u64) -> String {
    format!(
        "{}{}/{incarnation:016x}/",
        keyed_bucket_prefix(&stream_id.bucket_id),
        keyed_namespace_key(stream_id)
    )
}

/// Whether `path` names one incarnation's namespace prefix as
/// [`keyed_incarnation_prefix`] renders it: exactly
/// `.keyed/{bucket}/{key}/{16 hex}/` with non-empty bucket and key. The cold
/// GC worker removes such paths as prefixes; every other GC path is one
/// object name.
pub fn is_keyed_incarnation_prefix(path: &str) -> bool {
    let Some(rest) = path.strip_prefix(KEYED_NAMESPACE_ROOT) else {
        return false;
    };
    let Some(rest) = rest.strip_suffix('/') else {
        return false;
    };
    let mut parts = rest.split('/');
    let (Some(bucket), Some(key), Some(incarnation), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    !bucket.is_empty()
        && !key.is_empty()
        && incarnation.len() == 16
        && incarnation.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_one_percent_encoded_component() {
        assert_eq!(
            keyed_namespace_key(&BucketStreamId::new("b", "run-1")),
            "run-1"
        );
        assert_eq!(
            keyed_namespace_key(&BucketStreamId::with_affinity("b", "a", "s")),
            "a%2Fs"
        );
        assert_eq!(encode_key_component("x%2Fy/z"), "x%252Fy%2Fz");
    }

    #[test]
    fn two_segment_and_affinity_namespaces_are_disjoint() {
        let two = keyed_incarnation_prefix(&BucketStreamId::new("b", "s"), 7);
        let affinity =
            keyed_incarnation_prefix(&BucketStreamId::with_affinity("b", "s", "chunks"), 7);
        assert_eq!(two, ".keyed/b/s/0000000000000007/");
        assert_eq!(affinity, ".keyed/b/s%2Fchunks/0000000000000007/");
        assert!(!affinity.starts_with(&two));
        assert!(two.starts_with(&keyed_bucket_prefix("b")));
    }

    #[test]
    fn incarnation_prefix_predicate_matches_only_rendered_prefixes() {
        let rendered =
            keyed_incarnation_prefix(&BucketStreamId::with_affinity("b", "a", "s"), u64::MAX);
        assert!(is_keyed_incarnation_prefix(&rendered));
        for path in [
            ".keyed/b/",
            ".keyed/b/s/",
            ".keyed/b/s/0000000000000007",
            ".keyed/b/s/0000000000000007/v1/",
            ".keyed//s/0000000000000007/",
            ".keyed/b//0000000000000007/",
            ".keyed/b/s/000000000000000g/",
            "b/s/chunks/0000000000000007/",
            "",
        ] {
            assert!(!is_keyed_incarnation_prefix(path), "{path}");
        }
    }
}
