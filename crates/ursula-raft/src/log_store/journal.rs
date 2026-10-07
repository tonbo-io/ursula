//! Append-only framed journal, format epoch 3.
//!
//! Persistence is kept orthogonal to serialization. The journal moves opaque
//! checksummed frames to and from a file and handles the durability concerns:
//! append, `fsync`, verification on replay, and the torn tail a crash leaves.
//! How a record turns into a payload is the [`FrameCodec`]'s business. Every
//! file operation goes through the [`Disk`] seam.
//!
//! Layout, little-endian:
//!
//! ```text
//! header  magic "URSJWAL\0" | u16 version | u16 header length (32) | u32 zero
//!         | u64 sequence | u32 zero | u32 CRC32 of the 28 bytes before it
//! frame   u32 payload length | u32 header CRC | u32 payload CRC | payload
//! ```
//!
//! The header CRC covers the file's sequence and the payload length, so a
//! damaged length is caught before replay trusts it. The payload CRC covers the
//! sequence, the length and the payload.
//!
//! The sequence names one generation of the file. A new journal starts at
//! [`FIRST_SEQUENCE`], and every rewrite (startup compaction, online reclaim)
//! writes the next generation at the previous sequence plus one. A frame
//! therefore verifies only in the generation that wrote it: bytes of another
//! generation fail verification instead of replaying as current records.
//!
//! Replay never skips a frame that fails verification. [`JournalReplayMode`]
//! decides whether such a frame fails the replay or ends the verified prefix.

use std::fmt;
use std::io;
#[cfg(test)]
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;

use super::disk::Disk;
use super::disk::DiskFile;
use super::disk::JournalDisk;
use super::disk::JournalFile;

const JOURNAL_MAGIC: [u8; 8] = *b"URSJWAL\0";
/// The journal header version is the format epoch (`ursula_stream::FORMAT_EPOCH`).
/// Earlier epochs check their own version exactly, so each refuses the other.
const JOURNAL_VERSION: u16 = ursula_stream::FORMAT_EPOCH as u16;
const _: () = assert!(ursula_stream::FORMAT_EPOCH <= u16::MAX as u32);
const JOURNAL_HEADER_LEN: usize = 32;
const JOURNAL_HEADER_LEN_U16: u16 = 32;
const JOURNAL_HEADER_LEN_U64: u64 = 32;
/// The header bytes its own checksum covers.
const JOURNAL_HEADER_CHECKED_LEN: usize = 28;
const FRAME_HEADER_LEN: usize = 12;
const FRAME_HEADER_LEN_U64: u64 = 12;

/// The sequence of a newly created journal.
pub(crate) const FIRST_SEQUENCE: u64 = 1;

/// Maximum encoded payload accepted from disk or written as one journal frame.
///
/// This is intentionally above Ursula's 256 MiB Raft RPC limit while still
/// preventing a corrupted length field from requesting an unbounded allocation.
pub const MAX_FRAME_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
const MAX_FRAME_PAYLOAD_BYTES_U64: u64 = MAX_FRAME_PAYLOAD_BYTES as u64;

/// Pending frames are written out once they reach this size, so a batch of
/// large records is never copied whole.
pub(crate) const WRITE_BUFFER_BYTES: usize = 1024 * 1024;

/// How replay treats a frame that fails verification.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum JournalReplayMode {
    /// Every write is expected on disk: only an incomplete final frame (a
    /// write a crash cut short) is tolerated and truncated. Any other frame
    /// that fails verification fails the replay.
    #[default]
    Strict,
    /// Writeback may have left holes: keep the frames before the first one
    /// that fails verification and truncate the rest.
    VerifiedPrefix,
}

/// The operation a journal I/O error came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalOp {
    Open,
    Stat,
    Read,
    Append,
    Sync,
    SyncDir,
    Truncate,
    Remove,
    Rename,
}

impl fmt::Display for JournalOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Open => "open",
            Self::Stat => "stat",
            Self::Read => "read",
            Self::Append => "append to",
            Self::Sync => "fsync",
            Self::SyncDir => "fsync the directory of",
            Self::Truncate => "truncate",
            Self::Remove => "remove",
            Self::Rename => "rename",
        })
    }
}

/// Why a file header is not a valid header of this format epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HeaderDefect {
    #[error("the header is incomplete")]
    Torn,
    #[error("the header checksum does not match")]
    Checksum,
    #[error("the header declares length {0}, expected {JOURNAL_HEADER_LEN}")]
    Length(u16),
    #[error("reserved header bytes are not zero")]
    Reserved,
}

/// Why a frame failed verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameDefect {
    /// The length or the generation sequence does not match the header checksum.
    #[error("header checksum mismatch")]
    HeaderChecksum,
    /// The payload does not match its checksum.
    #[error("payload checksum mismatch")]
    PayloadChecksum,
}

/// Failure of the journal file.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("{op} journal '{}': {source}", .path.display())]
    Io {
        path: PathBuf,
        op: JournalOp,
        #[source]
        source: io::Error,
    },
    #[error(
        "'{}' has no Ursula WAL magic; it is not an Ursula journal this binary can read \
         (format epoch {}). Start on an empty raft.wal.path",
        .path.display(),
        ursula_stream::FORMAT_EPOCH
    )]
    NotAJournal { path: PathBuf },
    #[error("{}", version_refusal(.path, *.version))]
    UnsupportedVersion { path: PathBuf, version: u16 },
    #[error("journal '{}' has a corrupt file header: {defect}", .path.display())]
    CorruptHeader { path: PathBuf, defect: HeaderDefect },
    #[error("journal '{}' frame {frame} at offset {offset}: {defect}", .path.display())]
    CorruptFrame {
        path: PathBuf,
        frame: u64,
        offset: u64,
        defect: FrameDefect,
    },
    #[error(
        "journal '{}' frame {frame} at offset {offset} declares {declared} bytes, exceeding the \
         {MAX_FRAME_PAYLOAD_BYTES} byte limit",
        .path.display()
    )]
    OversizedFrame {
        path: PathBuf,
        frame: u64,
        offset: u64,
        declared: u64,
    },
    #[error("journal '{}' frame {frame} at offset {offset} does not decode: {source}", .path.display())]
    Undecodable {
        path: PathBuf,
        frame: u64,
        offset: u64,
        #[source]
        source: io::Error,
    },
    #[error("journal '{}' frame {frame} at offset {offset} cannot be replayed: {source}", .path.display())]
    Rejected {
        path: PathBuf,
        frame: u64,
        offset: u64,
        #[source]
        source: io::Error,
    },
    #[error(transparent)]
    RecordTooLarge(#[from] RecordTooLarge),
    #[error(
        "journal '{}' has {bytes} bytes after its last whole frame at offset {offset} while \
         its writer is running",
        .path.display()
    )]
    UnexpectedTail {
        path: PathBuf,
        offset: u64,
        bytes: u64,
    },
}

impl JournalError {
    pub(crate) fn io(path: &Path, op: JournalOp, source: io::Error) -> Self {
        Self::Io {
            path: path.to_owned(),
            op,
            source,
        }
    }

    /// The `io::ErrorKind` OpenRaft sees for this failure.
    pub(crate) fn kind(&self) -> io::ErrorKind {
        match self {
            Self::Io { source, .. } => source.kind(),
            Self::NotAJournal { .. }
            | Self::UnsupportedVersion { .. }
            | Self::CorruptHeader { .. }
            | Self::CorruptFrame { .. }
            | Self::OversizedFrame { .. }
            | Self::Undecodable { .. }
            | Self::Rejected { .. }
            | Self::UnexpectedTail { .. } => io::ErrorKind::InvalidData,
            Self::RecordTooLarge(_) => io::ErrorKind::InvalidInput,
        }
    }
}

fn version_refusal(path: &Path, version: u16) -> String {
    ursula_stream::format_epoch_refusal(
        &format!("journal '{}'", path.display()),
        &format!("uses Ursula WAL version {version}"),
    )
}

/// A record whose encoding does not fit in one frame. Nothing was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("journal record is {len} bytes, exceeding the {limit} byte frame limit")]
pub struct RecordTooLarge {
    pub len: usize,
    pub limit: usize,
}

/// What a replay verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Replayed {
    /// The generation sequence of the file; `None` when it is missing or empty.
    pub(crate) sequence: Option<u64>,
    /// Frames verified and visited.
    pub(crate) frames: u64,
    /// The length of the header and the verified frames.
    pub(crate) verified_len: u64,
    /// What follows the verified frames.
    pub(crate) tail: ReplayTail,
}

/// What follows the verified frames of a replayed journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayTail {
    /// The file ends after the last verified frame.
    Clean,
    /// The final frame is incomplete: a write a crash cut short.
    Incomplete { bytes: u64 },
    /// [`JournalReplayMode::VerifiedPrefix`] only: frame `frame` failed
    /// verification, so it and the `bytes` from its start are dropped. Frames
    /// after it cannot be counted, because a failed frame's length is not
    /// trusted.
    Unverified {
        frame: u64,
        bytes: u64,
        defect: FrameDefect,
    },
}

impl Replayed {
    const EMPTY: Self = Self {
        sequence: None,
        frames: 0,
        verified_len: 0,
        tail: ReplayTail::Clean,
    };

    /// Bytes after the verified frames.
    pub(crate) fn dropped_bytes(&self) -> u64 {
        match self.tail {
            ReplayTail::Clean => 0,
            ReplayTail::Incomplete { bytes } | ReplayTail::Unverified { bytes, .. } => bytes,
        }
    }

    /// Fails unless the file ends exactly after its last verified frame, as
    /// a journal whose writer is running does.
    pub(crate) fn require_clean(&self, path: &Path) -> Result<(), JournalError> {
        match self.tail {
            ReplayTail::Clean => Ok(()),
            ReplayTail::Incomplete { .. } | ReplayTail::Unverified { .. } => {
                Err(JournalError::UnexpectedTail {
                    path: path.to_owned(),
                    offset: self.verified_len,
                    bytes: self.dropped_bytes(),
                })
            }
        }
    }
}

/// Serialization seam: how one record becomes a frame payload and back.
///
/// `encode_into` is infallible because the codecs we use (MessagePack, JSON
/// over plain owned types) cannot fail in practice.
pub(crate) trait FrameCodec {
    /// The record type carried in each frame.
    type Record;

    /// Serialize a record, appending its payload to `out`.
    fn encode_into(record: &Self::Record, out: &mut Vec<u8>);

    /// Deserialize a frame payload back into a record.
    fn decode(payload: &[u8]) -> io::Result<Self::Record>;
}

/// JSON frame codec for any owned, serde-serializable record.
#[cfg(test)]
pub(crate) struct JsonCodec<T>(PhantomData<T>);

#[cfg(test)]
impl<T> FrameCodec for JsonCodec<T>
where T: serde::Serialize + serde::de::DeserializeOwned
{
    type Record = T;

    fn encode_into(record: &T, out: &mut Vec<u8>) {
        serde_json::to_writer(out, record).expect("journal record serializes to JSON");
    }

    fn decode(payload: &[u8]) -> io::Result<T> {
        serde_json::from_slice(payload)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }
}

/// The checksum of a frame's length within generation `sequence`.
fn header_checksum(sequence: u64, len: u32) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&sequence.to_le_bytes());
    hasher.update(&len.to_le_bytes());
    hasher.finalize()
}

/// The checksum of the sequence, the length and the payload, continued from
/// the header checksum over the first two.
fn payload_checksum(header_checksum: u32, payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new_with_initial(header_checksum);
    hasher.update(payload);
    hasher.finalize()
}

fn encode_header(sequence: u64) -> Vec<u8> {
    let mut header = Vec::with_capacity(JOURNAL_HEADER_LEN);
    header.extend_from_slice(&JOURNAL_MAGIC);
    header.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
    header.extend_from_slice(&JOURNAL_HEADER_LEN_U16.to_le_bytes());
    header.extend_from_slice(&[0; 4]);
    header.extend_from_slice(&sequence.to_le_bytes());
    header.extend_from_slice(&[0; 4]);
    let checksum = crc32fast::hash(&header);
    header.extend_from_slice(&checksum.to_le_bytes());
    header
}

/// Fixed-size fields read from the front of a byte slice.
struct Fields<'a>(&'a [u8]);

impl Fields<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (field, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*field)
    }
}

enum HeaderFault {
    NotAJournal,
    Version(u16),
    Defect(HeaderDefect),
}

impl HeaderFault {
    fn into_error(self, path: &Path) -> JournalError {
        let path = path.to_owned();
        match self {
            Self::NotAJournal => JournalError::NotAJournal { path },
            Self::Version(version) => JournalError::UnsupportedVersion { path, version },
            Self::Defect(defect) => JournalError::CorruptHeader { path, defect },
        }
    }
}

/// Checks a file header and returns its generation sequence. `bytes` holds
/// the file's first bytes, at most a whole header.
fn parse_header(bytes: &[u8]) -> Result<u64, HeaderFault> {
    let magic_len = bytes.len().min(JOURNAL_MAGIC.len());
    if bytes.get(..magic_len) != JOURNAL_MAGIC.get(..magic_len) {
        return Err(HeaderFault::NotAJournal);
    }
    let torn = || HeaderFault::Defect(HeaderDefect::Torn);
    let mut fields = Fields(bytes);
    fields.take::<8>().ok_or_else(torn)?;
    let version = u16::from_le_bytes(fields.take().ok_or_else(torn)?);
    if version != JOURNAL_VERSION {
        return Err(HeaderFault::Version(version));
    }
    let checked = bytes.get(..JOURNAL_HEADER_CHECKED_LEN).ok_or_else(torn)?;
    let header_len = u16::from_le_bytes(fields.take().ok_or_else(torn)?);
    let first_reserved = fields.take::<4>().ok_or_else(torn)?;
    let sequence = u64::from_le_bytes(fields.take().ok_or_else(torn)?);
    let second_reserved = fields.take::<4>().ok_or_else(torn)?;
    let checksum = u32::from_le_bytes(fields.take().ok_or_else(torn)?);
    if crc32fast::hash(checked) != checksum {
        return Err(HeaderFault::Defect(HeaderDefect::Checksum));
    }
    if header_len != JOURNAL_HEADER_LEN_U16 {
        return Err(HeaderFault::Defect(HeaderDefect::Length(header_len)));
    }
    if first_reserved != [0; 4] || second_reserved != [0; 4] {
        return Err(HeaderFault::Defect(HeaderDefect::Reserved));
    }
    Ok(sequence)
}

struct FrameHeader {
    len: u32,
    header_checksum: u32,
    payload_checksum: u32,
}

impl FrameHeader {
    fn parse(bytes: [u8; FRAME_HEADER_LEN]) -> Self {
        let [l0, l1, l2, l3, h0, h1, h2, h3, p0, p1, p2, p3] = bytes;
        Self {
            len: u32::from_le_bytes([l0, l1, l2, l3]),
            header_checksum: u32::from_le_bytes([h0, h1, h2, h3]),
            payload_checksum: u32::from_le_bytes([p0, p1, p2, p3]),
        }
    }
}

/// The single writer of one journal file.
///
/// [`JournalWriter::append`] encodes frames into a buffer that reaches the
/// file on [`JournalWriter::flush`] (or once it is full) and becomes durable
/// on [`JournalWriter::sync`]. After any I/O error the writer must be dropped:
/// the file may end in a partial frame, so nothing may be appended after it.
#[derive(Debug)]
pub(crate) struct JournalWriter {
    path: PathBuf,
    file: DiskFile,
    sequence: u64,
    /// Bytes written to the file, not counting `pending`.
    written: u64,
    pending: Vec<u8>,
    frame_limit: usize,
    /// The file is new, so its directory entry needs an `fsync` of the parent.
    parent_unsynced: bool,
}

impl JournalWriter {
    /// Opens the journal at `path` for appending. A missing or empty file
    /// becomes a new journal of generation `new_sequence`, whose header and
    /// directory entry are durable after the first [`JournalWriter::sync`].
    /// The parent directory must exist.
    pub(crate) fn open(path: &Path, new_sequence: u64) -> Result<Self, JournalError> {
        let mut file = Disk::open_append(path)
            .map_err(|source| JournalError::io(path, JournalOp::Open, source))?;
        let file_len = file
            .file_len()
            .map_err(|source| JournalError::io(path, JournalOp::Stat, source))?;
        let (sequence, pending) = if file_len == 0 {
            (new_sequence, encode_header(new_sequence))
        } else {
            let available = file_len.min(JOURNAL_HEADER_LEN_U64);
            let mut header = vec![0_u8; usize::try_from(available).unwrap_or(JOURNAL_HEADER_LEN)];
            file.read_exact(&mut header)
                .map_err(|source| JournalError::io(path, JournalOp::Read, source))?;
            let sequence = parse_header(&header).map_err(|fault| fault.into_error(path))?;
            (sequence, Vec::new())
        };
        Ok(Self {
            path: path.to_owned(),
            file,
            sequence,
            written: file_len,
            pending,
            frame_limit: MAX_FRAME_PAYLOAD_BYTES,
            parent_unsynced: file_len == 0,
        })
    }

    /// Lowers the frame limit, so tests can exceed it with small records.
    #[cfg(test)]
    pub(crate) fn with_frame_limit(mut self, frame_limit: usize) -> Self {
        self.frame_limit = frame_limit;
        self
    }

    /// The length of the file once the pending frames are written.
    pub(crate) fn len(&self) -> u64 {
        self.written
            .saturating_add(u64::try_from(self.pending.len()).unwrap_or(u64::MAX))
    }

    /// Encodes `record` as the next frame. It reaches the file on the next
    /// [`JournalWriter::flush`]. A record that does not fit in one frame is
    /// refused and leaves the journal unchanged.
    pub(crate) fn append<C: FrameCodec>(
        &mut self,
        record: &C::Record,
    ) -> Result<(), RecordTooLarge> {
        let start = self.pending.len();
        self.pending.extend_from_slice(&[0; FRAME_HEADER_LEN]);
        C::encode_into(record, &mut self.pending);
        let payload_start = start.saturating_add(FRAME_HEADER_LEN);
        let payload_len = self.pending.len().saturating_sub(payload_start);
        let too_large = RecordTooLarge {
            len: payload_len,
            limit: self.frame_limit,
        };
        let len = match u32::try_from(payload_len) {
            Ok(len) if payload_len <= self.frame_limit => len,
            _ => {
                self.pending.truncate(start);
                return Err(too_large);
            }
        };
        let header_checksum = header_checksum(self.sequence, len);
        let payload_checksum = payload_checksum(
            header_checksum,
            self.pending.get(payload_start..).unwrap_or_default(),
        );
        if let Some(header) = self.pending.get_mut(start..payload_start) {
            let [l0, l1, l2, l3] = len.to_le_bytes();
            let [h0, h1, h2, h3] = header_checksum.to_le_bytes();
            let [p0, p1, p2, p3] = payload_checksum.to_le_bytes();
            header.copy_from_slice(&[l0, l1, l2, l3, h0, h1, h2, h3, p0, p1, p2, p3]);
        }
        Ok(())
    }

    /// Bytes of encoded frames not written to the file yet.
    pub(crate) fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    /// Writes the pending frames to the file. They are durable only after
    /// [`JournalWriter::sync`].
    pub(crate) fn flush(&mut self) -> Result<(), JournalError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.file
            .append(&self.pending)
            .map_err(|source| JournalError::io(&self.path, JournalOp::Append, source))?;
        self.written = self.len();
        self.pending.clear();
        if self.pending.capacity() > WRITE_BUFFER_BYTES.saturating_mul(4) {
            self.pending.shrink_to(WRITE_BUFFER_BYTES);
        }
        Ok(())
    }

    /// Writes the pending frames and `fsync`s the file, plus its parent
    /// directory once when the file is new. Returns the number of `fsync`s.
    pub(crate) fn sync(&mut self) -> Result<u64, JournalError> {
        self.flush()?;
        JournalFile::sync_data(&mut self.file)
            .map_err(|source| JournalError::io(&self.path, JournalOp::Sync, source))?;
        if !self.parent_unsynced {
            return Ok(1);
        }
        let Some(parent) = self.path.parent() else {
            self.parent_unsynced = false;
            return Ok(1);
        };
        Disk::sync_dir(parent)
            .map_err(|source| JournalError::io(&self.path, JournalOp::SyncDir, source))?;
        self.parent_unsynced = false;
        Ok(2)
    }
}

/// Reads and verifies every frame of `path` in `mode`, streaming each record
/// through `visit`. The file is not modified: see [`recover`].
pub(crate) fn replay<C: FrameCodec>(
    path: &Path,
    mode: JournalReplayMode,
    visit: impl FnMut(C::Record) -> io::Result<()>,
) -> Result<Replayed, JournalError> {
    if !Disk::exists(path) {
        return Ok(Replayed::EMPTY);
    }
    let mut file =
        Disk::open_read(path).map_err(|source| JournalError::io(path, JournalOp::Open, source))?;
    let file_len = file
        .file_len()
        .map_err(|source| JournalError::io(path, JournalOp::Stat, source))?;
    scan::<C>(path, file_len, mode, |buf| file.read_exact(buf), visit)
}

/// [`replay`], then truncates whatever follows the verified frames, so the
/// file ends at a frame boundary and can be appended to.
pub(crate) fn recover<C: FrameCodec>(
    path: &Path,
    mode: JournalReplayMode,
    visit: impl FnMut(C::Record) -> io::Result<()>,
) -> Result<Replayed, JournalError> {
    let replayed = replay::<C>(path, mode, visit)?;
    if replayed.tail != ReplayTail::Clean {
        Disk::truncate(path, replayed.verified_len)
            .map_err(|source| JournalError::io(path, JournalOp::Truncate, source))?;
    }
    Ok(replayed)
}

/// Reads every record of `path`, truncating what follows the verified frames.
#[cfg(test)]
pub(crate) fn recover_all<C: FrameCodec>(
    path: &Path,
    mode: JournalReplayMode,
) -> Result<(Vec<C::Record>, Replayed), JournalError> {
    let mut records = Vec::new();
    let replayed = recover::<C>(path, mode, |record| {
        records.push(record);
        Ok(())
    })?;
    Ok((records, replayed))
}

/// Decodes the frames of an in-memory journal image in strict mode.
#[cfg(test)]
pub(crate) fn decode_frames<C: FrameCodec>(
    bytes: &[u8],
) -> Result<(Vec<C::Record>, Replayed), JournalError> {
    let mut records = Vec::new();
    let mut rest = bytes;
    let file_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let replayed = scan::<C>(
        Path::new("<in-memory journal>"),
        file_len,
        JournalReplayMode::Strict,
        |buf| io::Read::read_exact(&mut rest, buf),
        |record| {
            records.push(record);
            Ok(())
        },
    )?;
    Ok((records, replayed))
}

/// The verification loop shared by file and in-memory replay.
fn scan<C: FrameCodec>(
    path: &Path,
    file_len: u64,
    mode: JournalReplayMode,
    mut read_exact: impl FnMut(&mut [u8]) -> io::Result<()>,
    mut visit: impl FnMut(C::Record) -> io::Result<()>,
) -> Result<Replayed, JournalError> {
    if file_len == 0 {
        return Ok(Replayed::EMPTY);
    }
    let read_error = |source| JournalError::io(path, JournalOp::Read, source);
    let available = file_len.min(JOURNAL_HEADER_LEN_U64);
    let mut header = vec![0_u8; usize::try_from(available).unwrap_or(JOURNAL_HEADER_LEN)];
    read_exact(&mut header).map_err(read_error)?;
    let sequence = parse_header(&header).map_err(|fault| fault.into_error(path))?;

    let mut offset = JOURNAL_HEADER_LEN_U64;
    let mut frames = 0_u64;
    let mut payload = Vec::new();
    let tail = loop {
        let remaining = file_len.saturating_sub(offset);
        if remaining == 0 {
            break ReplayTail::Clean;
        }
        let frame = frames.saturating_add(1);
        if remaining < FRAME_HEADER_LEN_U64 {
            break ReplayTail::Incomplete { bytes: remaining };
        }
        let failed = |defect| match mode {
            JournalReplayMode::Strict => Err(JournalError::CorruptFrame {
                path: path.to_owned(),
                frame,
                offset,
                defect,
            }),
            JournalReplayMode::VerifiedPrefix => Ok(ReplayTail::Unverified {
                frame,
                bytes: remaining,
                defect,
            }),
        };

        let mut header = [0_u8; FRAME_HEADER_LEN];
        read_exact(&mut header).map_err(read_error)?;
        let header = FrameHeader::parse(header);
        if header.header_checksum != header_checksum(sequence, header.len) {
            break failed(FrameDefect::HeaderChecksum)?;
        }
        let declared = u64::from(header.len);
        let oversized = || JournalError::OversizedFrame {
            path: path.to_owned(),
            frame,
            offset,
            declared,
        };
        if declared > MAX_FRAME_PAYLOAD_BYTES_U64 {
            return Err(oversized());
        }
        if remaining.saturating_sub(FRAME_HEADER_LEN_U64) < declared {
            break ReplayTail::Incomplete { bytes: remaining };
        }
        payload.resize(
            usize::try_from(header.len).map_err(|_overflow| oversized())?,
            0,
        );
        read_exact(&mut payload).map_err(read_error)?;
        if payload_checksum(header.header_checksum, &payload) != header.payload_checksum {
            break failed(FrameDefect::PayloadChecksum)?;
        }
        let record = C::decode(&payload).map_err(|source| JournalError::Undecodable {
            path: path.to_owned(),
            frame,
            offset,
            source,
        })?;
        visit(record).map_err(|source| JournalError::Rejected {
            path: path.to_owned(),
            frame,
            offset,
            source,
        })?;
        offset = offset
            .saturating_add(FRAME_HEADER_LEN_U64)
            .saturating_add(declared);
        frames = frame;
    };
    Ok(Replayed {
        sequence: Some(sequence),
        frames,
        verified_len: offset,
        tail,
    })
}

/// These tests corrupt real files, so they run against the operating-system disk.
#[cfg(all(test, not(madsim)))]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::path::Path;

    use super::FIRST_SEQUENCE;
    use super::FRAME_HEADER_LEN_U64;
    use super::FrameDefect;
    use super::HeaderDefect;
    use super::JOURNAL_HEADER_LEN_U64;
    use super::JournalError;
    use super::JournalReplayMode;
    use super::JournalWriter;
    use super::JsonCodec;
    use super::MAX_FRAME_PAYLOAD_BYTES;
    use super::RecordTooLarge;
    use super::ReplayTail;
    use super::Replayed;
    use super::header_checksum;
    use super::payload_checksum;
    use super::recover_all;
    use super::replay;

    type Codec = JsonCodec<String>;

    fn write_records(path: &Path, sequence: u64, records: &[&str]) {
        let mut writer = JournalWriter::open(path, sequence).expect("open journal");
        for record in records {
            writer
                .append::<Codec>(&(*record).to_owned())
                .expect("append record");
        }
        writer.sync().expect("sync journal");
    }

    fn recover(
        path: &Path,
        mode: JournalReplayMode,
    ) -> Result<(Vec<String>, Replayed), JournalError> {
        recover_all::<Codec>(path, mode)
    }

    /// The offset of frame `index` (1-based) of a journal of one-byte JSON
    /// strings ("a", "b", ...), each a 3-byte payload.
    fn frame_offset(index: u64) -> u64 {
        index
            .checked_sub(1)
            .and_then(|before| before.checked_mul(FRAME_HEADER_LEN_U64 + 3))
            .and_then(|frames| frames.checked_add(JOURNAL_HEADER_LEN_U64))
            .expect("frame offset fits u64")
    }

    fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open journal");
        file.seek(SeekFrom::Start(offset)).expect("seek");
        file.write_all(bytes).expect("overwrite");
        file.sync_data().expect("sync overwrite");
    }

    fn append_raw(path: &Path, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open journal");
        file.write_all(bytes).expect("append raw bytes");
        file.sync_data().expect("sync raw bytes");
    }

    fn file_len(path: &Path) -> u64 {
        fs::metadata(path).expect("journal metadata").len()
    }

    #[test]
    fn replays_appended_records_in_order() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_records(&path, FIRST_SEQUENCE, &["a", "bb", "ccc"]);

        let (records, replayed) = recover(&path, JournalReplayMode::Strict).expect("replay");
        assert_eq!(records, ["a", "bb", "ccc"]);
        assert_eq!(replayed.sequence, Some(FIRST_SEQUENCE));
        assert_eq!(replayed.frames, 3);
        assert_eq!(replayed.verified_len, file_len(&path));
        assert_eq!(replayed.tail, ReplayTail::Clean);
    }

    #[test]
    fn replay_of_missing_file_is_empty() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (records, replayed) =
            recover(&dir.path().join("absent"), JournalReplayMode::Strict).expect("replay");
        assert!(records.is_empty());
        assert_eq!(replayed.sequence, None);
    }

    #[test]
    fn append_reopens_and_extends_existing_journal() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_records(&path, 7, &["first"]);
        // An existing journal keeps its own sequence.
        write_records(&path, FIRST_SEQUENCE, &["second"]);

        let (records, replayed) = recover(&path, JournalReplayMode::Strict).expect("replay");
        assert_eq!(records, ["first", "second"]);
        assert_eq!(replayed.sequence, Some(7));
    }

    #[test]
    fn the_payload_checksum_covers_sequence_length_and_payload() {
        let sequence = 0x0102_0304_0506_0708_u64;
        let payload = b"payload";
        let len = u32::try_from(payload.len()).expect("length fits u32");
        let mut covered = sequence.to_le_bytes().to_vec();
        covered.extend_from_slice(&len.to_le_bytes());
        assert_eq!(header_checksum(sequence, len), crc32fast::hash(&covered));
        covered.extend_from_slice(payload);
        assert_eq!(
            payload_checksum(header_checksum(sequence, len), payload),
            crc32fast::hash(&covered)
        );
    }

    #[test]
    fn both_modes_truncate_an_incomplete_final_frame() {
        for mode in [JournalReplayMode::Strict, JournalReplayMode::VerifiedPrefix] {
            for torn_bytes in [5_usize, 14] {
                let dir = tempfile::tempdir().expect("temp dir");
                let path = dir.path().join("journal");
                write_records(&path, FIRST_SEQUENCE, &["a", "b"]);
                let clean_len = file_len(&path);
                // A frame cut short: within its header, or after a whole header.
                let whole = tempfile::tempdir().expect("temp dir");
                let whole_path = whole.path().join("journal");
                write_records(&whole_path, FIRST_SEQUENCE, &["a", "b", "\"twelve chars\""]);
                let frame = fs::read(&whole_path).expect("read journal");
                let start = usize::try_from(clean_len).expect("length fits usize");
                append_raw(&path, &frame[start..start + torn_bytes]);

                let (records, replayed) = recover(&path, mode).expect("recover a torn tail");
                assert_eq!(records, ["a", "b"], "{mode:?} {torn_bytes}");
                assert_eq!(replayed.tail, ReplayTail::Incomplete {
                    bytes: u64::try_from(torn_bytes).expect("fits u64")
                });
                assert_eq!(file_len(&path), clean_len, "the torn frame is truncated");
                let (records, replayed) = recover(&path, mode).expect("re-replay");
                assert_eq!(records, ["a", "b"]);
                assert_eq!(replayed.tail, ReplayTail::Clean);
            }
        }
    }

    /// Epoch 2 kept the length outside the checksum, so a damaged length read
    /// as a torn tail and replay silently dropped every later frame. The
    /// header checksum now catches it.
    #[test]
    fn a_corrupt_length_is_detected_not_taken_for_a_torn_tail() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_records(&path, FIRST_SEQUENCE, &["a", "b", "c"]);
        let len = file_len(&path);
        overwrite(&path, frame_offset(2), &1_000_u32.to_le_bytes());

        let err = recover(&path, JournalReplayMode::Strict).expect_err("strict fails closed");
        assert!(matches!(err, JournalError::CorruptFrame {
            frame: 2,
            defect: FrameDefect::HeaderChecksum,
            offset,
            ..
        } if offset == frame_offset(2)));
        assert_eq!(file_len(&path), len, "strict replay leaves the file alone");

        let (records, replayed) =
            recover(&path, JournalReplayMode::VerifiedPrefix).expect("keep the verified prefix");
        assert_eq!(records, ["a"]);
        assert_eq!(replayed.tail, ReplayTail::Unverified {
            frame: 2,
            bytes: len - frame_offset(2),
            defect: FrameDefect::HeaderChecksum,
        });
        assert_eq!(file_len(&path), frame_offset(2));
    }

    #[test]
    fn mid_file_payload_corruption_fails_strict_and_ends_the_verified_prefix() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_records(&path, FIRST_SEQUENCE, &["a", "b", "c"]);
        let len = file_len(&path);
        overwrite(&path, frame_offset(2) + FRAME_HEADER_LEN_U64 + 1, b"x");

        let err = recover(&path, JournalReplayMode::Strict).expect_err("strict fails closed");
        assert!(matches!(err, JournalError::CorruptFrame {
            frame: 2,
            defect: FrameDefect::PayloadChecksum,
            ..
        }));
        assert_eq!(file_len(&path), len);

        let (records, replayed) =
            recover(&path, JournalReplayMode::VerifiedPrefix).expect("keep the verified prefix");
        assert_eq!(records, ["a"]);
        assert_eq!(replayed.frames, 1);
        assert_eq!(replayed.dropped_bytes(), len - frame_offset(2));
        assert!(matches!(replayed.tail, ReplayTail::Unverified {
            defect: FrameDefect::PayloadChecksum,
            ..
        }));
    }

    /// A host crash can persist later pages and lose earlier ones, which
    /// leaves a zero-filled hole before intact frames.
    #[test]
    fn a_zero_filled_hole_fails_strict_and_ends_the_verified_prefix() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_records(&path, FIRST_SEQUENCE, &["a", "b", "c", "d"]);
        overwrite(&path, frame_offset(2), &[0; 30]);

        let err = recover(&path, JournalReplayMode::Strict).expect_err("strict fails closed");
        assert!(matches!(err, JournalError::CorruptFrame { frame: 2, .. }));

        let (records, replayed) =
            recover(&path, JournalReplayMode::VerifiedPrefix).expect("keep the verified prefix");
        assert_eq!(records, ["a"]);
        assert_eq!(replayed.verified_len, frame_offset(2));

        // A zero-filled tail is not an incomplete frame either.
        let path = dir.path().join("zero-tail");
        write_records(&path, FIRST_SEQUENCE, &["a"]);
        append_raw(&path, &[0; 64]);
        let err = recover(&path, JournalReplayMode::Strict).expect_err("strict fails closed");
        assert!(matches!(err, JournalError::CorruptFrame { frame: 2, .. }));
        let (records, _) =
            recover(&path, JournalReplayMode::VerifiedPrefix).expect("keep the verified prefix");
        assert_eq!(records, ["a"]);
    }

    #[test]
    fn a_frame_of_another_generation_fails_verification() {
        let dir = tempfile::tempdir().expect("temp dir");
        let old = dir.path().join("old");
        write_records(&old, 4, &["a", "b"]);
        let path = dir.path().join("journal");
        write_records(&path, 5, &["c"]);
        let old_frame = fs::read(&old).expect("read old generation");
        let start = usize::try_from(frame_offset(2)).expect("fits usize");
        append_raw(&path, &old_frame[start..]);

        let err = recover(&path, JournalReplayMode::Strict).expect_err("strict fails closed");
        assert!(matches!(err, JournalError::CorruptFrame {
            frame: 2,
            defect: FrameDefect::HeaderChecksum,
            ..
        }));
        let (records, _) =
            recover(&path, JournalReplayMode::VerifiedPrefix).expect("keep the verified prefix");
        assert_eq!(records, ["c"]);
    }

    #[test]
    fn a_corrupt_file_header_fails_both_modes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_records(&path, FIRST_SEQUENCE, &["a"]);
        // Flip a bit of the sequence.
        overwrite(&path, 16, &[2]);
        for mode in [JournalReplayMode::Strict, JournalReplayMode::VerifiedPrefix] {
            let err = recover(&path, mode).expect_err("a corrupt header fails closed");
            assert!(matches!(err, JournalError::CorruptHeader {
                defect: HeaderDefect::Checksum,
                ..
            }));
        }
    }

    #[test]
    fn an_epoch_2_journal_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        let mut file = File::create(&path).expect("create epoch-2 journal");
        file.write_all(b"URSJWAL\0").expect("magic");
        file.write_all(&2_u16.to_le_bytes()).expect("version");
        file.write_all(&16_u16.to_le_bytes())
            .expect("header length");
        file.write_all(&[0; 4]).expect("reserved");
        let payload = b"\"epoch 2\"";
        file.write_all(&u32::try_from(payload.len()).expect("fits").to_le_bytes())
            .expect("length");
        file.write_all(&crc32fast::hash(payload).to_le_bytes())
            .expect("checksum");
        file.write_all(payload).expect("payload");
        file.sync_data().expect("sync");

        for mode in [JournalReplayMode::Strict, JournalReplayMode::VerifiedPrefix] {
            let err = recover(&path, mode).expect_err("epoch 2 is refused");
            assert!(matches!(err, JournalError::UnsupportedVersion {
                version: 2,
                ..
            }));
            assert!(err.to_string().contains("format epoch 3 only"), "{err}");
        }
        let err = JournalWriter::open(&path, FIRST_SEQUENCE).expect_err("no append to epoch 2");
        assert!(matches!(err, JournalError::UnsupportedVersion {
            version: 2,
            ..
        }));
    }

    #[test]
    fn a_file_without_the_magic_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        fs::write(&path, b"\x05\x00\x00\x00legacy").expect("write a legacy journal");
        for mode in [JournalReplayMode::Strict, JournalReplayMode::VerifiedPrefix] {
            let err = recover(&path, mode).expect_err("a foreign file is refused");
            assert!(matches!(err, JournalError::NotAJournal { .. }));
        }
        assert_eq!(file_len(&path), 10, "a foreign file is never truncated");
    }

    #[test]
    fn an_oversized_frame_fails_before_allocating() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_records(&path, FIRST_SEQUENCE, &["a"]);
        let declared = u32::try_from(MAX_FRAME_PAYLOAD_BYTES + 1).expect("limit fits u32");
        let mut header = declared.to_le_bytes().to_vec();
        header.extend_from_slice(&header_checksum(FIRST_SEQUENCE, declared).to_le_bytes());
        header.extend_from_slice(&[0; 4]);
        append_raw(&path, &header);

        for mode in [JournalReplayMode::Strict, JournalReplayMode::VerifiedPrefix] {
            let err = recover(&path, mode).expect_err("an oversized frame fails closed");
            assert!(matches!(err, JournalError::OversizedFrame { frame: 2, .. }));
        }
    }

    /// A frame whose checksums verify was written whole, so a decode failure
    /// is not a crash artifact and both modes fail closed.
    #[test]
    fn a_verified_frame_that_does_not_decode_fails_both_modes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        let mut writer = JournalWriter::open(&path, FIRST_SEQUENCE).expect("open journal");
        writer.append::<Codec>(&"a".to_owned()).expect("append");
        writer
            .append::<JsonCodec<u32>>(&7)
            .expect("append a record of another type");
        writer.sync().expect("sync");

        for mode in [JournalReplayMode::Strict, JournalReplayMode::VerifiedPrefix] {
            let err = recover(&path, mode).expect_err("an undecodable frame fails closed");
            assert!(matches!(err, JournalError::Undecodable { frame: 2, .. }));
        }
    }

    #[test]
    fn a_record_over_the_frame_limit_is_refused_without_writing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        let mut writer = JournalWriter::open(&path, FIRST_SEQUENCE)
            .expect("open journal")
            .with_frame_limit(8);
        writer
            .append::<Codec>(&"a".to_owned())
            .expect("a small record fits");
        let before = writer.len();
        let err = writer
            .append::<Codec>(&"too long for the limit".to_owned())
            .expect_err("the record exceeds the limit");
        assert_eq!(err, RecordTooLarge { len: 24, limit: 8 });
        assert_eq!(writer.len(), before);
        writer.sync().expect("sync");

        let (records, _) = recover(&path, JournalReplayMode::Strict).expect("replay");
        assert_eq!(records, ["a"]);
    }

    #[test]
    fn replay_does_not_modify_the_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_records(&path, FIRST_SEQUENCE, &["a"]);
        append_raw(&path, &[1, 2, 3]);
        let len = file_len(&path);
        let replayed = replay::<Codec>(&path, JournalReplayMode::Strict, |_| Ok(()))
            .expect("replay a torn tail");
        assert_eq!(replayed.tail, ReplayTail::Incomplete { bytes: 3 });
        assert_eq!(file_len(&path), len);
        let err = replayed
            .require_clean(&path)
            .expect_err("a running writer never leaves a torn tail");
        assert!(matches!(err, JournalError::UnexpectedTail { bytes: 3, .. }));
    }
}
