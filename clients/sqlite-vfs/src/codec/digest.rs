//! Digests the server checks. `Producer-Digest` is the BLAKE3-256 of an append body. A snapshot's
//! `Stream-Snapshot-Digest` is the server's `SnapshotDigest` (`crates/ursula-stream/src/model.rs`):
//! BLAKE3 over the content type's length (u64 little-endian), the content type and the body.
//! `ursula_headers` holds the digest types and their header syntax. This module computes them.

use ursula_headers::BodyDigest;
use ursula_headers::SnapshotDigest;

/// The BLAKE3-256 of an append body, sent as `Producer-Digest`.
pub fn producer_digest(body: &[u8]) -> BodyDigest {
    BodyDigest::from_bytes(*blake3::hash(body).as_bytes())
}

/// The snapshot digest, fed the body piece by piece as it is written.
#[derive(Debug, Clone)]
pub struct SnapshotDigester(blake3::Hasher);

impl SnapshotDigester {
    pub fn new(content_type: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(content_type.len() as u64).to_le_bytes());
        hasher.update(content_type.as_bytes());
        Self(hasher)
    }

    pub fn update(&mut self, body: &[u8]) {
        self.0.update(body);
    }

    pub fn finalize(&self) -> SnapshotDigest {
        SnapshotDigest::from_bytes(*self.0.finalize().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::SnapshotDigester;
    use super::producer_digest;
    use crate::codec::frame::CONTENT_TYPE;

    fn digest(content_type: &str, pieces: &[&[u8]]) -> String {
        let mut d = SnapshotDigester::new(content_type);
        for p in pieces {
            d.update(p);
        }
        d.finalize().to_string()
    }

    // Golden vectors computed with the server's `ursula_stream::SnapshotDigest`.
    #[test]
    fn snapshot_digests_equal_the_servers() {
        for (content_type, body, expected) in GOLDEN {
            assert_eq!(digest(content_type, &[body]), *expected, "{content_type}");
        }
        // Piece by piece is the same digest.
        let body = GOLDEN[2].1;
        let (a, b) = body.split_at(5);
        assert_eq!(digest(CONTENT_TYPE, &[a, &[], b]), GOLDEN[2].2);
    }

    #[test]
    fn producer_digests_are_blake3() {
        assert_eq!(
            *producer_digest(b"").as_bytes(),
            *blake3::Hash::from_hex(
                "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
            )
            .unwrap()
            .as_bytes()
        );
        assert_eq!(
            *producer_digest(b"abc").as_bytes(),
            *blake3::Hash::from_hex(
                "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
            )
            .unwrap()
            .as_bytes()
        );
    }

    const GOLDEN: &[(&str, &[u8], &str)] = &[
        (
            "application/octet-stream",
            b"",
            "cecc1097efdd5bad6b90256bcfb122a72337573558de9814ba531cffeb63d8e5",
        ),
        (
            CONTENT_TYPE,
            b"",
            "371c39981bea7983a5a808eb95990a08f65b45c0bc9b43da37d80eed459e5d93",
        ),
        (
            CONTENT_TYPE,
            b"USS3 snapshot body",
            "9aad98f4f8ac3bc444c489d16a3817c62996622b3ff4ec0ef238435622b81092",
        ),
    ];
}
