use ursula_shard::BucketStreamId;

pub fn validate_bucket_id(bucket_id: &str) -> Result<(), String> {
    if !(4..=64).contains(&bucket_id.len()) {
        return Err(format!(
            "bucket_id must be 4 to 64 bytes, got {} bytes",
            bucket_id.len()
        ));
    }
    if !bucket_id.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
    }) {
        return Err("bucket_id must match ^[a-z0-9_-]{4,64}$".to_owned());
    }
    // `snapshots` is the default raft-snapshot namespace in the object store,
    // which sits beside the bucket prefixes: a tenant bucket of that name
    // would share it, and purging it would erase raft snapshots. `__` names
    // stay free for Ursula's own namespaces (format epoch 2).
    if bucket_id == "snapshots" {
        return Err("bucket_id 'snapshots' is reserved".to_owned());
    }
    if bucket_id.starts_with("__") {
        return Err("bucket_id must not start with '__'".to_owned());
    }
    Ok(())
}

/// Validates a stream identity on apply.
pub fn validate_stream_id(stream_id: &BucketStreamId) -> Result<(), String> {
    let local = stream_id.stream_id.as_str();
    validate_path_segment(local)?;
    if local == "streams" {
        return Err("stream_id 'streams' is reserved".to_owned());
    }
    // `$`-prefixed names stay free for future bucket-level subresources
    // (see `RESERVED_SUBRESOURCE_NAMES`).
    if local.starts_with('$') {
        return Err("stream_id must not start with '$'".to_owned());
    }
    let combined_len = stream_id.bucket_id.len() + 1 + local.len();
    if combined_len > 122 {
        return Err(format!(
            "bucket/stream identity must not exceed 122 bytes, got {combined_len} bytes"
        ));
    }
    Ok(())
}

fn validate_path_segment(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("stream_id must not be empty".to_owned());
    }
    if value.len() > 122 {
        return Err(format!(
            "stream_id must not exceed 122 bytes, got {} bytes",
            value.len()
        ));
    }
    if value.contains('/') || value.contains('\0') || value.contains("..") {
        return Err("stream_id must not contain '/', NUL, or '..'".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_namespace_and_double_underscore_bucket_ids_are_reserved() {
        assert!(
            validate_bucket_id("snapshots")
                .expect_err("reserved bucket")
                .contains("reserved")
        );
        for bucket in ["__ursula", "__ab"] {
            assert!(
                validate_bucket_id(bucket)
                    .expect_err("reserved bucket")
                    .contains("'__'")
            );
        }
        for bucket in ["snapshot", "my-snapshots", "a__b", "_abc"] {
            assert_eq!(validate_bucket_id(bucket), Ok(()));
        }
    }

    #[test]
    fn stream_ids_starting_with_dollar_are_reserved() {
        for stream in ["$", "$x", "$txn"] {
            assert!(
                validate_stream_id(&BucketStreamId::new("test", stream))
                    .expect_err("reserved stream")
                    .contains("must not start with '$'")
            );
        }
        // Subresource names are reserved as path segments only, so a
        // two-segment stream may still use them.
        for stream in ["snapshot", "a$b"] {
            assert_eq!(
                validate_stream_id(&BucketStreamId::new("test", stream)),
                Ok(())
            );
        }
    }

    #[test]
    fn identity_is_capped_at_122_bytes_including_the_bucket() {
        // "test/" is 5 bytes.
        assert_eq!(
            validate_stream_id(&BucketStreamId::new("test", "a".repeat(117))),
            Ok(())
        );
        assert!(
            validate_stream_id(&BucketStreamId::new("test", "a".repeat(118)))
                .expect_err("123-byte identity")
                .contains("must not exceed 122 bytes")
        );
    }
}
