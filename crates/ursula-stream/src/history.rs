//! History identity (#581 section 4): the group's history, the public
//! `Stream-Incarnation` token derived from it, and what a stream's current
//! history inherited from the one before.
//!
//! - **History.** Every Raft group carries one history id, a random non-zero
//!   64-bit value drawn by the proposer of the latest history event: the
//!   group's genesis, an import (restore) or an accepted loss of
//!   acknowledged writes ([`crate::StreamCommand::NewHistory`],
//!   [`crate::StreamCommand::ImportSnapshot`]). A group whose genesis has not
//!   applied has no history ([`HistoryEvent`] absent) and serves nothing.
//! - **Token.** [`incarnation_token`] folds a stream's `created_at_ms` with a
//!   key of the history id: `2^52 | ((created_at_ms ^ key(history)) &
//!   (2^52 - 1))`. Within one history it is a bijection of `created_at_ms`
//!   below 2^52, which C7 makes unique per group, so a token is never reused
//!   for a URL. Across histories and clusters two tokens of one URL coincide
//!   with probability 2^-52. Every token lies in `[2^52, 2^53)`: never 0,
//!   exact in a JavaScript `Number`, and disjoint from every 0.7 token (a
//!   `created_at_ms` below 2^52). The golden vectors in the tests freeze it.
//! - **Inheritance (W5).** A history event touches no stream. A stream keeps
//!   the history id it was created or last written in; while that differs
//!   from the group's, no byte of the stream changed since the event, so
//!   `HEAD` reports the stream's token in that history and its tail. The
//!   first write after the event records the pair ([`InheritedHistory`]),
//!   so the report survives later writes until the next event.

use std::num::NonZeroU64;

use serde::Deserialize;
use serde::Serialize;

/// Bit 52: set in every token.
pub const INCARNATION_TAG: u64 = 1 << 52;

/// The low 52 bits of a token.
const INCARNATION_MASK: u64 = INCARNATION_TAG - 1;

/// The largest `created_at_ms` a create assigns. A create at or above 2^52
/// (a clock about 140,000 years ahead) is refused, so the token's mask never
/// drops a bit of `created_at_ms`.
pub const MAX_CREATED_AT_MS: u64 = INCARNATION_MASK;

/// The BLAKE3-256 digest of a backup, which keys its import (#581 section
/// 4.4).
pub type BackupDigest = [u8; 32];

/// The key a history id contributes to every token of its history: the
/// first 8 bytes of a domain-separated BLAKE3 digest of the id, masked to 52
/// bits.
pub fn history_key(history_id: u64) -> u64 {
    let digest = blake3::Hasher::new()
        .update(b"ursula incarnation key v1")
        .update(&history_id.to_le_bytes())
        .finalize();
    let mut first = [0u8; 8];
    for (to, from) in first.iter_mut().zip(digest.as_bytes()) {
        *to = *from;
    }
    u64::from_le_bytes(first) & INCARNATION_MASK
}

/// The public `Stream-Incarnation` token of the stream incarnation created at
/// `created_at_ms`, in the history `history_id` (extensions Section 2.4).
pub fn incarnation_token(created_at_ms: u64, history_id: u64) -> u64 {
    INCARNATION_TAG | ((created_at_ms ^ history_key(history_id)) & INCARNATION_MASK)
}

/// A fresh, random, non-zero history id for a history event. The proposer
/// draws it, never apply. Under madsim the draw is deterministic per seed.
pub fn draw_history_id() -> NonZeroU64 {
    loop {
        if let Some(id) = NonZeroU64::new(rand::random()) {
            return id;
        }
    }
}

/// The latest history event a group applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEvent {
    /// The random id the event's proposer drew.
    pub id: NonZeroU64,
    /// The log index of the entry that applied the event. A recovery plan is
    /// satisfied once this is above the plan's point (#581 section 4.5).
    pub event_index: u64,
    /// The digest of the backup this group was restored from, if any: a
    /// repeat of that import answers `already`.
    pub imported: Option<BackupDigest>,
}

/// What a stream's current history began from: its history id before the
/// latest event it went through, and its tail at that event. Bytes below
/// `offset` are the same under both histories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InheritedHistory {
    pub history: u64,
    pub offset: u64,
}

/// What `HEAD` reports about the history a stream's current one began from
/// (`Stream-Previous-Incarnation`, `Stream-Inherited-Offset`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InheritedReport {
    /// The stream's token in the earlier history.
    pub previous_incarnation: u64,
    /// The offset below which the bytes are the same under both tokens.
    pub offset: u64,
}

#[cfg(test)]
mod tests {
    use super::INCARNATION_TAG;
    use super::MAX_CREATED_AT_MS;
    use super::history_key;
    use super::incarnation_token;

    /// Golden vectors: tokens live in client logs, caches and cache keys
    /// from the first 0.8 release on, so the function never changes.
    #[test]
    fn tokens_match_the_golden_vectors() {
        let vectors: [(u64, u64, u64); 5] = [
            (1_700_000_000_000, 1, 0x0018_c5c3_ccf5_e2fc),
            (1_700_000_000_001, 1, 0x0018_c5c3_ccf5_e2fd),
            (
                1_700_000_000_000,
                0x0123_4567_89ab_cdef,
                0x0016_2aa9_b77a_907c,
            ),
            (0, u64::MAX, 0x001c_cfb2_9ca6_3d21),
            (MAX_CREATED_AT_MS, 42, 0x0010_84d9_9b2f_5d1d),
        ];
        for (created_at_ms, history, token) in vectors {
            assert_eq!(
                incarnation_token(created_at_ms, history),
                token,
                "token of ({created_at_ms}, {history:#x})"
            );
        }
    }

    #[test]
    fn tokens_lie_in_the_tagged_range() {
        for created_at_ms in [0, 1, 1_700_000_000_000, MAX_CREATED_AT_MS] {
            for history in [1, 2, u64::MAX, 0x8000_0000_0000_0000] {
                let token = incarnation_token(created_at_ms, history);
                assert!((INCARNATION_TAG..1 << 53).contains(&token));
            }
        }
    }

    #[test]
    fn a_token_is_a_bijection_of_created_at_within_one_history() {
        let key = history_key(7);
        for created_at_ms in [0, 1, 99, MAX_CREATED_AT_MS] {
            let token = incarnation_token(created_at_ms, 7);
            assert_eq!((token ^ key) & MAX_CREATED_AT_MS, created_at_ms);
        }
    }

    #[test]
    fn equal_created_at_in_two_histories_gives_two_tokens() {
        assert_ne!(
            incarnation_token(1_700_000_000_000, 1),
            incarnation_token(1_700_000_000_000, 2)
        );
    }
}
