//! Append-only framed journal.
//!
//! Persistence is kept orthogonal to serialization. The journal moves opaque
//! versioned, checksummed frames to and from a file and handles the durability
//! concerns: append, `fsync`, bounded recovery, and recovery of a torn trailing
//! frame after a crash. How a record turns into a payload is the
//! [`FrameCodec`]'s business. Every file operation goes through the
//! [`Disk`] seam.

use std::io;
#[cfg(test)]
use std::marker::PhantomData;
use std::path::Path;

use super::disk::Disk;
use super::disk::DiskFile;
use super::disk::JournalDisk;
use super::disk::JournalFile;

const JOURNAL_MAGIC: [u8; 8] = *b"URSJWAL\0";
/// The journal header version is the format epoch (`ursula_stream::FORMAT_EPOCH`):
/// Ursula 0.5.x wrote version 1 and refuses this one with its own exact check.
const JOURNAL_VERSION: u16 = ursula_stream::FORMAT_EPOCH as u16;
const _: () = assert!(ursula_stream::FORMAT_EPOCH <= u16::MAX as u32);
const JOURNAL_HEADER_LEN: usize = 16;
const JOURNAL_HEADER_LEN_U16: u16 = JOURNAL_HEADER_LEN as u16;
const JOURNAL_HEADER_LEN_U64: u64 = JOURNAL_HEADER_LEN as u64;
const FRAME_HEADER_LEN: usize = 8;
const FRAME_HEADER_LEN_U64: u64 = FRAME_HEADER_LEN as u64;

/// Maximum encoded payload accepted from disk or written as one journal frame.
///
/// This is intentionally above Ursula's 256 MiB Raft RPC limit while still
/// preventing a corrupted length field from requesting an unbounded allocation.
pub const MAX_FRAME_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
const MAX_FRAME_PAYLOAD_BYTES_U64: u64 = MAX_FRAME_PAYLOAD_BYTES as u64;

fn journal_header() -> [u8; JOURNAL_HEADER_LEN] {
    let [m0, m1, m2, m3, m4, m5, m6, m7] = JOURNAL_MAGIC;
    let [v0, v1] = JOURNAL_VERSION.to_le_bytes();
    let [l0, l1] = JOURNAL_HEADER_LEN_U16.to_le_bytes();
    [m0, m1, m2, m3, m4, m5, m6, m7, v0, v1, l0, l1, 0, 0, 0, 0]
}

/// Serialization seam: how one record becomes a frame payload and back.
///
/// `encode` is infallible because the codecs we use (MessagePack, JSON over
/// plain owned types) cannot fail in practice.
pub(crate) trait FrameCodec {
    /// The record type carried in each frame.
    type Record;

    /// Serialize a record into a frame payload.
    fn encode(record: &Self::Record) -> Vec<u8>;

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

    fn encode(record: &T) -> Vec<u8> {
        serde_json::to_vec(record).expect("journal record serializes to JSON")
    }

    fn decode(payload: &[u8]) -> io::Result<T> {
        serde_json::from_slice(payload)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }
}

/// An append handle over a single journal file.
///
/// The file is opened lazily on first append. The parent directory is `fsync`ed
/// once on the first [`JournalWriter::sync`] when the file may have been freshly
/// created, so the file's existence survives a crash.
#[derive(Debug)]
pub(crate) struct JournalWriter {
    file: Option<DiskFile>,
    parent_unsynced: bool,
}

impl JournalWriter {
    /// Create a writer. Set `needs_parent_sync` when the file may not exist yet, so
    /// the parent directory is `fsync`ed once the file is created.
    pub(crate) fn new(needs_parent_sync: bool) -> Self {
        Self {
            file: None,
            parent_unsynced: needs_parent_sync,
        }
    }

    /// Create and initialize the journal file even when there are no records.
    pub(crate) fn ensure_created(&mut self, path: &Path) -> io::Result<()> {
        self.file_mut(path).map(|_| ())
    }

    /// Append one record as a framed payload. Does not durably flush; pair with
    /// [`JournalWriter::sync`] once per batch.
    pub(crate) fn append<C: FrameCodec>(
        &mut self,
        path: &Path,
        record: &C::Record,
    ) -> io::Result<()> {
        let payload = C::encode(record);
        if payload.len() > MAX_FRAME_PAYLOAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "journal record is {} bytes, exceeding the {} byte limit",
                    payload.len(),
                    MAX_FRAME_PAYLOAD_BYTES
                ),
            ));
        }
        let len = u32::try_from(payload.len()).map_err(|_overflow| {
            io::Error::new(io::ErrorKind::InvalidData, "journal record too large")
        })?;
        let [l0, l1, l2, l3] = len.to_le_bytes();
        let [c0, c1, c2, c3] = crc32fast::hash(&payload).to_le_bytes();
        let file = self.file_mut(path)?;
        file.append(&[l0, l1, l2, l3, c0, c1, c2, c3])?;
        file.append(&payload)
    }

    /// `fsync` the file data, plus the parent directory once if it was freshly created.
    pub(crate) fn sync(&mut self, path: &Path) -> io::Result<()> {
        let file = self.file_mut(path)?;
        file.sync_data()?;
        if self.parent_unsynced
            && let Some(parent) = path.parent()
        {
            Disk::sync_dir(parent)?;
            self.parent_unsynced = false;
        }
        Ok(())
    }

    fn file_mut(&mut self, path: &Path) -> io::Result<&mut DiskFile> {
        let file = match self.file.take() {
            Some(file) => file,
            None => open_journal_for_append(path)?,
        };
        Ok(self.file.insert(file))
    }
}

fn open_journal_for_append(path: &Path) -> io::Result<DiskFile> {
    if let Some(parent) = path.parent() {
        Disk::create_dir_all(parent)?;
    }
    let mut file = Disk::open_append(path)?;
    let file_len = file.file_len()?;
    if file_len == 0 {
        file.append(&journal_header())?;
    } else {
        validate_file_header(&mut file, path, file_len)?;
    }
    Ok(file)
}

/// Read every record from `path`, decoding with `C`. A torn trailing frame left by a
/// crash mid-write is truncated away and ignored, leaving the file at its last clean
/// record boundary.
#[cfg(test)]
pub(crate) fn replay<C: FrameCodec>(path: &Path) -> io::Result<Vec<C::Record>> {
    let mut records = Vec::new();
    replay_each::<C>(path, |record| {
        records.push(record);
        Ok(())
    })?;
    Ok(records)
}

/// Stream every valid record from `path` through `visit` without retaining the
/// entire journal in memory. A torn trailing frame is truncated away, leaving
/// the file at its last clean record boundary.
pub(crate) fn replay_each<C: FrameCodec>(
    path: &Path,
    mut visit: impl FnMut(C::Record) -> io::Result<()>,
) -> io::Result<()> {
    if !Disk::exists(path) {
        return Ok(());
    }

    let mut file = Disk::open_read(path)?;
    let file_len = file.file_len()?;
    if file_len == 0 {
        return Ok(());
    }
    if file_len < JOURNAL_HEADER_LEN_U64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "journal '{}' has no complete Ursula WAL header; it is not an Ursula journal \
                 this binary can read (format epoch {}). Start on an empty raft.wal.path",
                path.display(),
                ursula_stream::FORMAT_EPOCH
            ),
        ));
    }
    validate_file_header(&mut file, path, file_len)?;
    let mut valid_len = JOURNAL_HEADER_LEN_U64;
    let mut frame_index = 0_u64;
    while valid_len < file_len {
        let remaining = file_len.saturating_sub(valid_len);
        if remaining < FRAME_HEADER_LEN_U64 {
            break;
        }
        let frame_number = frame_index.saturating_add(1);

        let mut len_bytes = [0_u8; 4];
        file.read_exact(&mut len_bytes)?;
        let payload_len = u64::from(u32::from_le_bytes(len_bytes));
        let mut checksum_bytes = [0_u8; 4];
        file.read_exact(&mut checksum_bytes)?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);
        if payload_len > MAX_FRAME_PAYLOAD_BYTES_U64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "journal '{}' frame {} declares {} bytes, exceeding the {} byte limit",
                    path.display(),
                    frame_number,
                    payload_len,
                    MAX_FRAME_PAYLOAD_BYTES
                ),
            ));
        }
        if remaining.saturating_sub(FRAME_HEADER_LEN_U64) < payload_len {
            break;
        }

        let mut payload = vec![
            0_u8;
            usize::try_from(payload_len).map_err(|_overflow| {
                io::Error::new(io::ErrorKind::InvalidData, "journal frame exceeds usize")
            })?
        ];
        file.read_exact(&mut payload)?;
        let actual_checksum = crc32fast::hash(&payload);
        if actual_checksum != expected_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "journal '{}' frame {} checksum mismatch: expected {expected_checksum:#010x}, got {actual_checksum:#010x}",
                    path.display(),
                    frame_number
                ),
            ));
        }
        let record = C::decode(&payload).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "journal '{}' frame {} decode failed: {err}",
                    path.display(),
                    frame_number
                ),
            )
        })?;
        visit(record)?;
        valid_len = valid_len
            .checked_add(FRAME_HEADER_LEN_U64.saturating_add(payload_len))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "journal offset overflow"))?;
        frame_index = frame_index.saturating_add(1);
    }

    if valid_len < file_len {
        Disk::truncate(path, valid_len)?;
    }
    Ok(())
}

/// Decode framed records from an in-memory buffer, returning the records and the byte
/// length of the valid (fully-written) prefix. A torn trailing frame ends the scan.
#[cfg(test)]
pub(crate) fn decode_frames<C: FrameCodec>(bytes: &[u8]) -> io::Result<(Vec<C::Record>, usize)> {
    let mut records = Vec::new();
    if bytes.is_empty() {
        return Ok((records, 0));
    }
    let Some(header) = bytes.first_chunk::<JOURNAL_HEADER_LEN>() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "in-memory journal has no complete Ursula WAL header",
        ));
    };
    validate_header_bytes(header, "in-memory journal")?;
    let mut offset = JOURNAL_HEADER_LEN;
    let mut frame_index = 0_usize;
    while offset < bytes.len() {
        let Some(&[l0, l1, l2, l3, c0, c1, c2, c3]) = bytes
            .get(offset..)
            .and_then(<[u8]>::first_chunk::<FRAME_HEADER_LEN>)
        else {
            return Ok((records, offset)); // torn length prefix
        };
        let len = usize::try_from(u32::from_le_bytes([l0, l1, l2, l3])).map_err(|_overflow| {
            io::Error::new(io::ErrorKind::InvalidData, "journal frame exceeds usize")
        })?;
        if len > MAX_FRAME_PAYLOAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "in-memory journal frame {} declares {len} bytes, exceeding the {MAX_FRAME_PAYLOAD_BYTES} byte limit",
                    frame_index.saturating_add(1)
                ),
            ));
        }
        let expected_checksum = u32::from_le_bytes([c0, c1, c2, c3]);
        let start = offset.saturating_add(FRAME_HEADER_LEN);
        let end = start.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "journal frame length overflow")
        })?;
        let Some(payload) = bytes.get(start..end) else {
            return Ok((records, offset)); // torn payload
        };
        let actual_checksum = crc32fast::hash(payload);
        if actual_checksum != expected_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "in-memory journal frame {} checksum mismatch: expected {expected_checksum:#010x}, got {actual_checksum:#010x}",
                    frame_index.saturating_add(1)
                ),
            ));
        }
        records.push(C::decode(payload)?);
        offset = end;
        frame_index = frame_index.saturating_add(1);
    }
    Ok((records, bytes.len()))
}

fn validate_file_header(file: &mut DiskFile, path: &Path, file_len: u64) -> io::Result<()> {
    if file_len < JOURNAL_HEADER_LEN_U64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("journal '{}' has a torn file header", path.display()),
        ));
    }
    let mut header = [0_u8; JOURNAL_HEADER_LEN];
    file.read_exact(&mut header)?;
    validate_header_bytes(&header, &format!("journal '{}'", path.display()))
}

fn validate_header_bytes(header: &[u8; JOURNAL_HEADER_LEN], description: &str) -> io::Result<()> {
    let [magic @ .., v0, v1, l0, l1, r0, r1, r2, r3] = *header;
    if magic != JOURNAL_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{description} has no Ursula WAL magic; it is not an Ursula journal this binary \
                 can read (format epoch {}). Start on an empty raft.wal.path",
                ursula_stream::FORMAT_EPOCH
            ),
        ));
    }
    let version = u16::from_le_bytes([v0, v1]);
    if version != JOURNAL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{description} uses Ursula WAL version {version}; this binary reads version \
                 {JOURNAL_VERSION} (format epoch {}) only. There is no in-place upgrade from \
                 Ursula 0.5.x",
                ursula_stream::FORMAT_EPOCH
            ),
        ));
    }
    let header_len = usize::from(u16::from_le_bytes([l0, l1]));
    if header_len != JOURNAL_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{description} declares unsupported header length {header_len}; expected {JOURNAL_HEADER_LEN}"
            ),
        ));
    }
    if [r0, r1, r2, r3] != [0; 4] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{description} has non-zero reserved header bytes"),
        ));
    }
    Ok(())
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

    use super::*;

    fn write_all(path: &Path, records: &[String]) {
        let mut writer = JournalWriter::new(true);
        for record in records {
            writer
                .append::<JsonCodec<String>>(path, record)
                .expect("append record");
        }
        writer.sync(path).expect("sync journal");
    }

    #[test]
    fn replays_appended_records_in_order() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        let records = vec!["a".to_owned(), "bb".to_owned(), "ccc".to_owned()];
        write_all(&path, &records);

        let replayed = replay::<JsonCodec<String>>(&path).expect("replay");
        assert_eq!(replayed, records);
    }

    #[test]
    fn replay_of_missing_file_is_empty() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("absent");
        let replayed = replay::<JsonCodec<String>>(&path).expect("replay");
        assert!(replayed.is_empty());
    }

    #[test]
    fn append_reopens_and_extends_existing_journal() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_all(&path, &["first".to_owned()]);
        write_all(&path, &["second".to_owned()]);

        let replayed = replay::<JsonCodec<String>>(&path).expect("replay");
        assert_eq!(replayed, vec!["first".to_owned(), "second".to_owned()]);
    }

    #[test]
    fn replay_truncates_a_torn_trailing_frame() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_all(&path, &["clean".to_owned()]);

        // Append a frame whose length header promises more bytes than follow.
        let mut file = OpenOptions::new().append(true).open(&path).expect("reopen");
        file.write_all(&64_u32.to_le_bytes()).expect("torn length");
        file.write_all(b"torn").expect("torn payload");
        file.sync_data().expect("sync torn tail");
        let torn_len = fs::metadata(&path).expect("metadata").len();

        let replayed = replay::<JsonCodec<String>>(&path).expect("replay");
        assert_eq!(replayed, vec!["clean".to_owned()]);

        // The torn tail was truncated away, so a re-read is clean and shorter.
        let healed_len = fs::metadata(&path).expect("metadata").len();
        assert!(healed_len < torn_len);
        let reread = replay::<JsonCodec<String>>(&path).expect("re-replay");
        assert_eq!(reread, vec!["clean".to_owned()]);
    }

    #[test]
    fn replay_each_visits_records_without_collecting_them() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_all(&path, &[
            "first".to_owned(),
            "second".to_owned(),
            "third".to_owned(),
        ]);

        let mut replayed = Vec::new();
        replay_each::<JsonCodec<String>>(&path, |record| {
            replayed.push(record);
            Ok(())
        })
        .expect("stream replay");

        assert_eq!(replayed, vec!["first", "second", "third"]);
    }

    #[test]
    fn replay_rejects_checksum_corruption() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_all(&path, &["clean".to_owned()]);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open journal");
        file.seek(SeekFrom::End(-1))
            .expect("seek final payload byte");
        file.write_all(b"x").expect("corrupt payload");
        file.sync_data().expect("sync corruption");

        let err = replay::<JsonCodec<String>>(&path).expect_err("checksum must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("frame 1 checksum mismatch"));
    }

    #[test]
    fn replay_rejects_legacy_unversioned_journal() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        let payload = serde_json::to_vec("legacy").expect("encode legacy payload");
        let mut file = File::create(&path).expect("create legacy journal");
        file.write_all(
            &u32::try_from(payload.len())
                .expect("payload length fits u32")
                .to_le_bytes(),
        )
        .expect("write legacy length");
        file.write_all(&payload).expect("write legacy payload");
        file.sync_data().expect("sync legacy journal");

        let err = replay::<JsonCodec<String>>(&path).expect_err("legacy format must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .contains("not an Ursula journal this binary can read")
        );
    }

    #[test]
    fn replay_rejects_unsupported_version() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_all(&path, &["clean".to_owned()]);

        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open journal");
        file.seek(SeekFrom::Start(8)).expect("seek version");
        file.write_all(&1_u16.to_le_bytes())
            .expect("write unsupported version");
        file.sync_data().expect("sync unsupported version");

        // E7: a format-epoch-1 (Ursula 0.5.x) journal is refused.
        let err = replay::<JsonCodec<String>>(&path).expect_err("version must fail closed");
        assert!(err.to_string().contains("uses Ursula WAL version 1"));
    }

    #[test]
    fn replay_rejects_oversized_frame_before_allocating() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        write_all(&path, &["clean".to_owned()]);

        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open journal");
        let oversized =
            u32::try_from(MAX_FRAME_PAYLOAD_BYTES + 1).expect("configured frame limit fits u32");
        file.write_all(&oversized.to_le_bytes())
            .expect("write oversized length");
        file.write_all(&0_u32.to_le_bytes())
            .expect("write placeholder checksum");
        file.sync_data().expect("sync oversized frame");

        let err = replay::<JsonCodec<String>>(&path).expect_err("oversized frame must fail closed");
        assert!(
            err.to_string()
                .contains("exceeding the 536870912 byte limit")
        );
    }
}
