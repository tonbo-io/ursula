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
    Ok(())
}

pub fn validate_stream_id(stream_id: &BucketStreamId) -> Result<(), String> {
    let local = stream_id.stream_id.as_str();
    if local.is_empty() {
        return Err("stream_id must not be empty".to_owned());
    }
    if local.len() > 122 {
        return Err(format!(
            "stream_id must not exceed 122 bytes, got {} bytes",
            local.len()
        ));
    }
    if local == "streams" {
        return Err("stream_id 'streams' is reserved".to_owned());
    }
    if local.contains('/') || local.contains('\0') || local.contains("..") {
        return Err("stream_id must not contain '/', NUL, or '..'".to_owned());
    }
    let combined_len = stream_id.bucket_id.len() + 1 + local.len();
    if combined_len > 122 {
        return Err(format!(
            "bucket_id/stream_id must not exceed 122 bytes, got {combined_len} bytes"
        ));
    }
    Ok(())
}
