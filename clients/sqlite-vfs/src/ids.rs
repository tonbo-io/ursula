//! The VFS's own identifiers: a local generation's lineage, a generation directory's id, a host
//! activation, a record's position in its log, and the commit id `(lineage, position)` that frames
//! carry in the clear.
//! Protocol values (epochs, sequence numbers, offsets, incarnation tokens, digests) are
//! `ursula_headers` types.

use std::fs::File;
use std::io::Read;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// The device read for random identifiers.
pub const URANDOM: &str = "/dev/urandom";

/// 16 random bytes per local generation: tells one local history from another (design: Layout).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lineage(pub [u8; 16]);

/// 16 random bytes per generation directory (`gen-<id32>`): a copy of a generation keeps its
/// lineage in a directory of its own, so the directory is named by this id, and a generation's
/// log segments each name the directory it supersedes by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GenId(pub [u8; 16]);

impl GenId {
    /// A fresh id from [`URANDOM`].
    pub fn random() -> std::io::Result<GenId> {
        random16().map(GenId)
    }

    /// 32 lowercase hex digits.
    pub fn hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The id 32 hex digits spell, if they do.
    pub fn parse_hex(s: &str) -> Option<GenId> {
        if s.len() != 32 {
            return None;
        }
        let mut out = [0u8; 16];
        for (i, o) in out.iter_mut().enumerate() {
            let at = i.checked_mul(2)?;
            *o = u8::from_str_radix(s.get(at..at.checked_add(2)?)?, 16).ok()?;
        }
        Some(GenId(out))
    }
}

/// 16 bytes per activation of a host, carried by CLAIM frames (design: Claims, Clones).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Activation(pub [u8; 16]);

impl Activation {
    /// A fresh activation for an open that names none: 16 random bytes per open (design: Claims).
    /// Without [`URANDOM`], the clock, the process id and a per-process count, so two opens of one
    /// process still differ.
    pub fn draw() -> Activation {
        static DRAWN: AtomicU64 = AtomicU64::new(0);
        let bytes = random16().unwrap_or_else(|_unavailable| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let count = DRAWN.fetch_add(1, Ordering::Relaxed);
            let salt = u128::from(std::process::id()).rotate_left(64) | u128::from(count);
            (nanos ^ salt).to_le_bytes()
        });
        Activation(bytes)
    }
}

/// A record's place in its log: positions grow by one per record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(pub u64);

/// A commit's identity: the generation that logged it and its position in that generation's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitId {
    pub lineage: Lineage,
    pub position: Position,
}

/// 16 bytes from [`URANDOM`].
pub fn random16() -> std::io::Result<[u8; 16]> {
    let mut n = [0u8; 16];
    File::open(URANDOM).and_then(|mut f| f.read_exact(&mut n))?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::GenId;
    use super::random16;

    #[test]
    fn random_identifiers_differ() {
        assert_ne!(random16().unwrap(), random16().unwrap());
    }

    #[test]
    fn generation_ids_spell_as_32_hex_digits() {
        let id = GenId([0xa5; 16]);
        assert_eq!(id.hex(), "a5".repeat(16));
        assert_eq!(GenId::parse_hex(&id.hex()), Some(id));
        assert_eq!(GenId::parse_hex("a5"), None);
        assert_eq!(GenId::parse_hex(&"zz".repeat(16)), None);
    }
}
