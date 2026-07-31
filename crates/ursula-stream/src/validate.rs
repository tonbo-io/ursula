use ursula_shard::BucketStreamId;
use ursula_shard::is_reserved_affinity_stream_id;

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
    Ok(())
}

pub fn validate_stream_id(stream_id: &BucketStreamId) -> Result<(), String> {
    if let Some(affinity_key) = &stream_id.affinity_key {
        validate_path_segment("affinity_key", affinity_key)?;
    }
    let local = stream_id.stream_id.as_str();
    validate_path_segment("stream_id", local)?;
    if local == "streams" {
        return Err("stream_id 'streams' is reserved".to_owned());
    }
    if stream_id.affinity_key.is_some() && is_reserved_affinity_stream_id(local) {
        return Err(format!(
            "stream_id '{local}' is reserved under an affinity path"
        ));
    }
    let combined_len = stream_id.bucket_id.len()
        + 1
        + stream_id
            .affinity_key
            .as_ref()
            .map_or(0, |affinity_key| affinity_key.len() + 1)
        + local.len();
    if combined_len > 122 {
        return Err(format!(
            "bucket/affinity/stream identity must not exceed 122 bytes, got {combined_len} bytes"
        ));
    }
    Ok(())
}

fn validate_path_segment(name: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if value.len() > 122 {
        return Err(format!(
            "{name} must not exceed 122 bytes, got {} bytes",
            value.len()
        ));
    }
    if value.contains('/') || value.contains('\0') || value.contains("..") {
        return Err(format!("{name} must not contain '/', NUL, or '..'"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn affinity_identity_uses_the_existing_total_length_limit() {
        let valid = BucketStreamId::with_affinity("test", "run-42", "queue");
        assert_eq!(validate_stream_id(&valid), Ok(()));

        let too_long = BucketStreamId::with_affinity("test", "a".repeat(112), "queue");
        assert!(
            validate_stream_id(&too_long)
                .expect_err("identity exceeds limit")
                .contains("must not exceed 122 bytes")
        );
    }

    #[test]
    fn affinity_rejects_ambiguous_subresource_names() {
        for stream in [
            "append-batch",
            "attrs",
            "bootstrap",
            "retention",
            "snapshot",
        ] {
            let stream_id = BucketStreamId::with_affinity("test", "run-42", stream);
            assert!(
                validate_stream_id(&stream_id)
                    .expect_err("reserved stream")
                    .contains("reserved under an affinity path")
            );
        }

        assert_eq!(
            validate_stream_id(&BucketStreamId::new("test", "snapshot")),
            Ok(())
        );
    }
}
