//! Small state files the WAL replaces whole: each core's metadata file and
//! the node's run-state and topology files.
//!
//! A state file is written to a temporary file next to it, `fsync`ed, renamed
//! over the old file and published with an `fsync` of its directory, all
//! through the [`Disk`] seam. A crash at any step leaves either the old or the
//! new version. The file also carries a checksum, so damage is reported
//! instead of read as state.
//!
//! Layout, little-endian:
//!
//! ```text
//! magic (8) | u16 version | u16 zero | u32 payload length
//!           | payload (MessagePack) | u32 CRC32 of every byte before it
//! ```
//!
//! The version is the format epoch, as in the journal header.

use std::io;
use std::path::Path;
use std::path::PathBuf;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::disk::Disk;
use super::disk::JournalDisk;
use super::disk::JournalFile;
use super::journal::JOURNAL_VERSION;
use super::journal::JournalError;
use super::journal::JournalOp;

const HEADER_LEN: usize = 16;
const CHECKSUM_LEN: usize = 4;
/// State files hold a few bytes per raft group; anything larger is not one.
const MAX_STATE_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Which state file a file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateFileKind {
    /// The votes and `initialized` flags of one core's raft groups.
    CoreMetadata,
    /// The node's run state.
    RunState,
    /// The immutable routing configuration of this WAL root.
    Topology,
}

impl StateFileKind {
    fn magic(self) -> [u8; 8] {
        match self {
            Self::CoreMetadata => *b"URSWMETA",
            Self::RunState => *b"URSWRUN\0",
            Self::Topology => *b"URSWTOPO",
        }
    }
}

/// Why a state file failed verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StateFileDefect {
    #[error("the file is shorter than its header and checksum")]
    Truncated,
    #[error("the declared payload length does not match the file length")]
    Length,
    #[error("reserved header bytes are not zero")]
    Reserved,
    #[error("the checksum does not match")]
    Checksum,
}

/// Failure to read a state file.
#[derive(Debug, thiserror::Error)]
pub enum StateFileError {
    #[error("{op} WAL state file '{}': {source}", .path.display())]
    Io {
        path: PathBuf,
        op: JournalOp,
        #[source]
        source: io::Error,
    },
    #[error("'{}' is not an Ursula WAL {kind:?} file", .path.display())]
    WrongKind { path: PathBuf, kind: StateFileKind },
    #[error("{}", ursula_stream::format_epoch_refusal(
        &format!("WAL state file '{}'", .path.display()),
        &format!("uses Ursula WAL version {version}"),
    ))]
    UnsupportedVersion { path: PathBuf, version: u16 },
    #[error("WAL state file '{}' is corrupt: {defect}", .path.display())]
    Corrupt {
        path: PathBuf,
        defect: StateFileDefect,
    },
    #[error("WAL state file '{}' does not decode: {source}", .path.display())]
    Undecodable {
        path: PathBuf,
        #[source]
        source: rmp_serde::decode::Error,
    },
}

/// Encodes `value` as a state file of `kind`.
pub(crate) fn encode<T: Serialize>(kind: StateFileKind, value: &T) -> Vec<u8> {
    let payload = rmp_serde::to_vec_named(value).expect("WAL state serializes to MessagePack");
    let payload_len = u32::try_from(payload.len()).expect("a WAL state file fits in 4 GiB");
    let mut bytes = Vec::with_capacity(
        HEADER_LEN
            .saturating_add(payload.len())
            .saturating_add(CHECKSUM_LEN),
    );
    bytes.extend_from_slice(&kind.magic());
    bytes.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
    bytes.extend_from_slice(&[0; 2]);
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload);
    let checksum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes
}

/// Verifies and decodes the state file `bytes` read from `path`.
pub(crate) fn decode<T: DeserializeOwned>(
    kind: StateFileKind,
    path: &Path,
    bytes: &[u8],
) -> Result<T, StateFileError> {
    let corrupt = |defect| StateFileError::Corrupt {
        path: path.to_owned(),
        defect,
    };
    let magic = kind.magic();
    let magic_len = bytes.len().min(magic.len());
    if bytes.get(..magic_len) != magic.get(..magic_len) {
        return Err(StateFileError::WrongKind {
            path: path.to_owned(),
            kind,
        });
    }
    let (header, rest) = bytes
        .split_first_chunk::<HEADER_LEN>()
        .ok_or_else(|| corrupt(StateFileDefect::Truncated))?;
    let [_, _, _, _, _, _, _, _, v0, v1, r0, r1, l0, l1, l2, l3] = *header;
    let version = u16::from_le_bytes([v0, v1]);
    if version != JOURNAL_VERSION {
        return Err(StateFileError::UnsupportedVersion {
            path: path.to_owned(),
            version,
        });
    }
    let (body, checksum) = rest
        .split_last_chunk::<CHECKSUM_LEN>()
        .ok_or_else(|| corrupt(StateFileDefect::Truncated))?;
    let declared = usize::try_from(u32::from_le_bytes([l0, l1, l2, l3]))
        .map_err(|_overflow| corrupt(StateFileDefect::Length))?;
    if body.len() != declared {
        return Err(corrupt(StateFileDefect::Length));
    }
    let checked = bytes
        .len()
        .checked_sub(CHECKSUM_LEN)
        .and_then(|len| bytes.get(..len))
        .ok_or_else(|| corrupt(StateFileDefect::Truncated))?;
    if crc32fast::hash(checked) != u32::from_le_bytes(*checksum) {
        return Err(corrupt(StateFileDefect::Checksum));
    }
    if [r0, r1] != [0, 0] {
        return Err(corrupt(StateFileDefect::Reserved));
    }
    rmp_serde::from_slice(body).map_err(|source| StateFileError::Undecodable {
        path: path.to_owned(),
        source,
    })
}

/// Reads the state file at `path`; `None` when there is none.
pub(crate) fn read<T: DeserializeOwned>(
    kind: StateFileKind,
    path: &Path,
) -> Result<Option<T>, StateFileError> {
    if !Disk::exists(path) {
        return Ok(None);
    }
    let io_error = |op| {
        move |source| StateFileError::Io {
            path: path.to_owned(),
            op,
            source,
        }
    };
    let mut file = Disk::open_read(path).map_err(io_error(JournalOp::Open))?;
    let len = file.file_len().map_err(io_error(JournalOp::Stat))?;
    if len > MAX_STATE_FILE_BYTES {
        return Err(StateFileError::Corrupt {
            path: path.to_owned(),
            defect: StateFileDefect::Length,
        });
    }
    let mut bytes = vec![0_u8; usize::try_from(len).unwrap_or(usize::MAX)];
    file.read_exact(&mut bytes)
        .map_err(io_error(JournalOp::Read))?;
    decode(kind, path, &bytes).map(Some)
}

/// Replaces the state file at `path` with `value`: writes `temp`, `fsync`s
/// it, renames it over `path` and `fsync`s the directory. Returns the number
/// of `fsync`s. Callers that may write the same file concurrently pass
/// different `temp` paths.
pub(crate) fn write<T: Serialize>(
    kind: StateFileKind,
    path: &Path,
    temp: &Path,
    value: &T,
) -> Result<u64, JournalError> {
    let bytes = encode(kind, value);
    if Disk::exists(temp) {
        Disk::remove_file(temp)
            .map_err(|source| JournalError::io(temp, JournalOp::Remove, source))?;
    }
    let mut file = Disk::open_append(temp)
        .map_err(|source| JournalError::io(temp, JournalOp::Open, source))?;
    file.append(&bytes)
        .map_err(|source| JournalError::io(temp, JournalOp::Append, source))?;
    file.sync_data()
        .map_err(|source| JournalError::io(temp, JournalOp::Sync, source))?;
    drop(file);
    Disk::rename(temp, path).map_err(|source| JournalError::io(path, JournalOp::Rename, source))?;
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(1);
    };
    Disk::sync_dir(parent).map_err(|source| JournalError::io(path, JournalOp::SyncDir, source))?;
    Ok(2)
}

/// These tests read and write real files, so they run on the
/// operating-system disk. Atomicity under power loss is covered on the
/// simulated disk (`ursula-sim`).
#[cfg(all(test, not(madsim)))]
mod tests {
    use std::fs;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use serde::Deserialize;

    use super::JOURNAL_VERSION;
    use super::Path;
    use super::PathBuf;
    use super::StateFileDefect;
    use super::StateFileError;
    use super::StateFileKind;
    use super::decode;
    use super::encode;
    use super::read;
    use super::write;

    #[derive(Debug, PartialEq, Eq, serde::Serialize, Deserialize)]
    struct Sample {
        name: String,
        count: u64,
    }

    fn sample(count: u64) -> Sample {
        Sample {
            name: "core-0".to_owned(),
            count,
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "ursula-raft-state-file-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        crate::tests::remove_test_path(&dir);
        fs::create_dir_all(&dir).expect("create test directory");
        dir
    }

    fn decode_sample(bytes: &[u8]) -> Result<Sample, StateFileError> {
        decode(StateFileKind::RunState, Path::new("state"), bytes)
    }

    #[test]
    fn state_files_round_trip_and_replace_the_previous_version() {
        let dir = temp_dir("round-trip");
        let path = dir.join("state.bin");
        let temp = dir.join("state.bin.tmp");
        assert_eq!(
            read::<Sample>(StateFileKind::RunState, &path).expect("read"),
            None
        );

        assert_eq!(
            write(StateFileKind::RunState, &path, &temp, &sample(1)).expect("write"),
            2,
            "the file and its directory"
        );
        assert_eq!(
            read::<Sample>(StateFileKind::RunState, &path).expect("read"),
            Some(sample(1))
        );
        // A temporary file a crash left behind is replaced, never appended to.
        fs::write(&temp, b"stale").expect("leave a stale temporary file");
        write(StateFileKind::RunState, &path, &temp, &sample(2)).expect("replace");
        assert_eq!(
            read::<Sample>(StateFileKind::RunState, &path).expect("read"),
            Some(sample(2))
        );
        assert!(!temp.exists(), "the temporary file is renamed away");
        crate::tests::remove_test_path(&dir);
    }

    #[test]
    fn state_files_refuse_damage_another_kind_and_another_epoch() {
        let bytes = encode(StateFileKind::RunState, &sample(7));
        assert_eq!(decode_sample(&bytes).expect("decode"), sample(7));

        let err = decode::<Sample>(StateFileKind::CoreMetadata, Path::new("state"), &bytes)
            .expect_err("a run-state file is not a metadata file");
        assert!(matches!(err, StateFileError::WrongKind {
            kind: StateFileKind::CoreMetadata,
            ..
        }));

        for len in [0, 4, 15, 19, bytes.len().saturating_sub(1)] {
            let err = decode_sample(&bytes[..len]).expect_err("a truncated file");
            assert!(
                matches!(err, StateFileError::Corrupt {
                    defect: StateFileDefect::Truncated | StateFileDefect::Length,
                    ..
                }),
                "{len}: {err}"
            );
        }

        let last = bytes.len().saturating_sub(1);
        for offset in [12, 16, last.saturating_sub(4), last] {
            let mut damaged = bytes.clone();
            damaged[offset] ^= 0x40;
            let err = decode_sample(&damaged).expect_err("a damaged byte");
            assert!(
                matches!(err, StateFileError::Corrupt {
                    defect: StateFileDefect::Checksum | StateFileDefect::Length,
                    ..
                }),
                "{offset}: {err}"
            );
        }

        let mut other_epoch = bytes.clone();
        let other = JOURNAL_VERSION.wrapping_sub(1);
        other_epoch[8..10].copy_from_slice(&other.to_le_bytes());
        let err = decode_sample(&other_epoch).expect_err("another format epoch");
        assert!(
            matches!(err, StateFileError::UnsupportedVersion { version, .. } if version == other)
        );
    }
}
