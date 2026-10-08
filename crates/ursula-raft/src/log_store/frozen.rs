//! Immutable payloads referenced by ordinary, checksummed journal records.
//! Publication never authorizes deletion: only a subsequently synced journal
//! reference can replace the original entries. Segment continuity is unchanged.

use std::path::Path;
use std::path::PathBuf;

use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use serde::Deserialize;
use serde::Serialize;

use super::CoreJournalRecord;
use super::RaftGroupLogRecord;
use super::disk::Disk;
use super::disk::JournalDisk;
use super::disk::JournalFile;
use super::journal;
use super::journal::JournalError;
use super::journal::JournalOp;
use super::journal::JournalReplayMode;
use super::journal::JournalWriter;
use super::journal::ReplayTail;
use super::writer::WireCodec;
use crate::types::UrsulaRaftTypeConfig;

type Entry = EntryOf<UrsulaRaftTypeConfig>;
type LogId = LogIdOf<UrsulaRaftTypeConfig>;

/// Derived from immutable source positions, never supplied as a filesystem path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct ArchiveId {
    pub(crate) group_id: u32,
    pub(crate) segment: u64,
    pub(crate) offset: u64,
    pub(crate) first_index: u64,
    pub(crate) last_index: u64,
    pub(crate) content_hash: [u8; 32],
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArchiveRef {
    pub(crate) id: ArchiveId,
    pub(crate) bytes: u64,
    pub(crate) checksum: u32,
}
/// Only these selected log IDs are live. Rewriting a reference must not
/// resurrect entries superseded by a later truncate, append or purge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FrozenEntry {
    pub(crate) log_id: LogId,
    pub(crate) bytes: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FrozenAppend {
    pub(crate) archive: ArchiveRef,
    pub(crate) entries: Vec<FrozenEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ArchiveDefect {
    #[error("length differs from the durable journal reference")]
    Length,
    #[error("checksum differs from the durable journal reference")]
    Checksum,
    #[error("archive does not contain exactly its declared group and entry range")]
    Content,
}

pub(crate) fn path(dir: &Path, id: ArchiveId) -> PathBuf {
    dir.join(format!(
        "frozen-{}-{}-{}-{}-{}-{}.seg",
        id.group_id,
        id.segment,
        id.offset,
        id.first_index,
        id.last_index,
        blake3::Hash::from_bytes(id.content_hash).to_hex()
    ))
}

fn bytes(path: &Path) -> Result<Vec<u8>, JournalError> {
    let mut file =
        Disk::open_read(path).map_err(|source| JournalError::io(path, JournalOp::Open, source))?;
    let len = file
        .file_len()
        .map_err(|source| JournalError::io(path, JournalOp::Stat, source))?;
    // A single frame plus its headers. Refuse corrupt lengths before allocation.
    if len > (journal::MAX_FRAME_PAYLOAD_BYTES as u64).saturating_add(1024) {
        return Err(JournalError::FrozenArchive {
            path: path.to_owned(),
            defect: ArchiveDefect::Length,
        });
    }
    let mut bytes = vec![
        0;
        usize::try_from(len).map_err(|_overflow| JournalError::FrozenArchive {
            path: path.to_owned(),
            defect: ArchiveDefect::Length
        })?
    ];
    file.read_exact(&mut bytes)
        .map_err(|source| JournalError::io(path, JournalOp::Read, source))?;
    Ok(bytes)
}

pub(crate) fn publish(
    dir: &Path,
    mut id: ArchiveId,
    entries: Vec<Entry>,
) -> Result<ArchiveRef, JournalError> {
    let temporary = path(dir, id).with_extension("tmp");
    // A failed attempt is never resumed by appending after its partial frame.
    if Disk::exists(&temporary) {
        Disk::remove_file(&temporary)
            .map_err(|source| JournalError::io(&temporary, JournalOp::Remove, source))?;
    }
    let mut writer = JournalWriter::open(&temporary, id.segment)?;
    writer.append::<WireCodec<CoreJournalRecord>>(&CoreJournalRecord {
        group_id: id.group_id,
        record: RaftGroupLogRecord::Append(entries),
    })?;
    writer.sync_data()?;
    drop(writer);
    let encoded = bytes(&temporary)?;
    id.content_hash = *blake3::hash(&encoded).as_bytes();
    let destination = path(dir, id);
    let reference = ArchiveRef {
        id,
        bytes: u64::try_from(encoded.len()).unwrap_or(u64::MAX),
        checksum: crc32fast::hash(&encoded),
    };
    if Disk::exists(&destination) {
        // A deterministic retry may reuse only byte-identical immutable data.
        if bytes(&destination)? != encoded {
            return Err(JournalError::FrozenArchive {
                path: destination,
                defect: ArchiveDefect::Checksum,
            });
        }
        Disk::remove_file(&temporary)
            .map_err(|source| JournalError::io(&temporary, JournalOp::Remove, source))?;
    } else {
        Disk::rename(&temporary, &destination)
            .map_err(|source| JournalError::io(&destination, JournalOp::Rename, source))?;
    }
    Disk::sync_dir(dir).map_err(|source| JournalError::io(dir, JournalOp::SyncDir, source))?;
    Ok(reference)
}

pub(crate) fn read(
    dir: &Path,
    group: u32,
    reference: &ArchiveRef,
) -> Result<Vec<Entry>, JournalError> {
    let path = path(dir, reference.id);
    let encoded = bytes(&path)?;
    let defect = |defect| JournalError::FrozenArchive {
        path: path.clone(),
        defect,
    };
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX) != reference.bytes {
        return Err(defect(ArchiveDefect::Length));
    }
    if crc32fast::hash(&encoded) != reference.checksum
        || blake3::hash(&encoded).as_bytes() != &reference.id.content_hash
    {
        return Err(defect(ArchiveDefect::Checksum));
    }
    let mut records = Vec::new();
    let replayed = journal::replay::<WireCodec<CoreJournalRecord>>(
        &path,
        JournalReplayMode::Strict,
        |_loc, record| {
            records.push(record);
            Ok(())
        },
    )?;
    if replayed.tail != ReplayTail::Clean
        || replayed.sequence != Some(reference.id.segment)
        || records.len() != 1
        || group != reference.id.group_id
    {
        return Err(defect(ArchiveDefect::Content));
    }
    let Some(record) = records.pop() else {
        return Err(defect(ArchiveDefect::Content));
    };
    let RaftGroupLogRecord::Append(entries) = record.record else {
        return Err(defect(ArchiveDefect::Content));
    };
    if record.group_id != group
        || entries.first().map(|entry| entry.log_id.index) != Some(reference.id.first_index)
        || entries.last().map(|entry| entry.log_id.index) != Some(reference.id.last_index)
    {
        return Err(defect(ArchiveDefect::Content));
    }
    if entries.windows(2).any(|pair| match pair {
        [a, b] => a.log_id.index.checked_add(1) != Some(b.log_id.index),
        _ => false,
    }) {
        return Err(defect(ArchiveDefect::Content));
    }
    Ok(entries)
}

/// Resolve only the exact live subset, validating both term and index.
pub(crate) fn selected(
    dir: &Path,
    group: u32,
    frozen: &FrozenAppend,
) -> Result<Vec<Entry>, JournalError> {
    let defect = || JournalError::FrozenArchive {
        path: path(dir, frozen.archive.id),
        defect: ArchiveDefect::Content,
    };
    if frozen.entries.is_empty()
        || frozen.entries.windows(2).any(|pair| match pair {
            [a, b] => a.log_id.index >= b.log_id.index,
            _ => false,
        })
    {
        return Err(defect());
    }
    let mut entries = read(dir, group, &frozen.archive)?.into_iter().peekable();
    let mut selected = Vec::with_capacity(frozen.entries.len());
    for expected in &frozen.entries {
        while entries
            .peek()
            .is_some_and(|entry| entry.log_id.index < expected.log_id.index)
        {
            entries.next();
        }
        match entries.next() {
            Some(entry)
                if entry.log_id == expected.log_id
                    && u32::try_from(crate::types::entry_log_bytes(&entry)).unwrap_or(u32::MAX)
                        == expected.bytes =>
            {
                selected.push(entry)
            }
            _ => return Err(defect()),
        }
    }
    Ok(selected)
}

/// Account orphan files once at startup without reading their payloads.
pub(crate) fn orphan_bytes(
    dir: &Path,
    referenced: &std::collections::BTreeSet<ArchiveId>,
) -> Result<u64, JournalError> {
    let keep = referenced
        .iter()
        .map(|id| path(dir, *id))
        .collect::<std::collections::BTreeSet<_>>();
    let mut total = 0_u64;
    for path in
        Disk::read_dir(dir).map_err(|source| JournalError::io(dir, JournalOp::Open, source))?
    {
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.starts_with("frozen-") && (name.ends_with(".seg") || name.ends_with(".tmp"))
            })
            && !keep.contains(&path)
        {
            let file = Disk::open_read(&path)
                .map_err(|source| JournalError::io(&path, JournalOp::Open, source))?;
            total = total.saturating_add(
                file.file_len()
                    .map_err(|source| JournalError::io(&path, JournalOp::Stat, source))?,
            );
        }
    }
    Ok(total)
}

/// Remove only payloads absent from every physical journal reference. Make
/// preceding segment deletions durable before deleting their dependencies.
pub(crate) fn collect(
    dir: &Path,
    retained: &std::collections::BTreeSet<ArchiveId>,
) -> Result<(), JournalError> {
    let keep = retained
        .iter()
        .map(|id| path(dir, *id))
        .collect::<std::collections::BTreeSet<_>>();
    let candidates = Disk::read_dir(dir)
        .map_err(|source| JournalError::io(dir, JournalOp::Open, source))?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("frozen-")
                        && (name.ends_with(".seg") || name.ends_with(".tmp"))
                })
                && !keep.contains(path)
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(());
    }
    Disk::sync_dir(dir).map_err(|source| JournalError::io(dir, JournalOp::SyncDir, source))?;
    for path in candidates {
        Disk::remove_file(&path)
            .map_err(|source| JournalError::io(&path, JournalOp::Remove, source))?;
    }
    Disk::sync_dir(dir).map_err(|source| JournalError::io(dir, JournalOp::SyncDir, source))
}
