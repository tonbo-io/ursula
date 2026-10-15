//! Identifiers shared by the formats and the replicator: a local generation's lineage, a host
//! activation, a record's position in its log, producer epochs and sequence numbers, and the
//! commit id `(lineage, position)` that frames carry in the clear.

use std::fs::File;
use std::io::Read;

/// The device read for random identifiers.
pub const URANDOM: &str = "/dev/urandom";

/// 16 random bytes per local generation: tells one local history from another (design §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lineage(pub [u8; 16]);

/// 16 bytes per activation of a host, carried by CLAIM frames (design §4.8, §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Activation(pub [u8; 16]);

/// A record's place in its log: positions grow by one per record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(pub u64);

/// A producer epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u64);

/// A producer sequence number within an epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(pub u64);

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
    use super::random16;

    #[test]
    fn random_identifiers_differ() {
        assert_ne!(random16().unwrap(), random16().unwrap());
    }
}
