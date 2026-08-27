//! Append-only framed journal.
//!
//! Persistence is kept orthogonal to serialization. The journal moves opaque
//! versioned, checksummed frames to and from a file and handles the durability
//! concerns — append, `fsync`, bounded recovery, and recovery of a torn trailing
//! frame after a crash.
//! How a record turns into a payload is entirely the [`FrameCodec`]'s business, so
//! the Raft log store can frame protobuf while the WAL engine frames JSON over the
//! exact same code.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;

const JOURNAL_MAGIC: [u8; 8] = *b"URSJWAL\0";
const JOURNAL_VERSION: u16 = 1;
const JOURNAL_HEADER_LEN: usize = 16;
const FRAME_HEADER_LEN: usize = 8;

/// Maximum encoded payload accepted from disk or written as one journal frame.
///
/// This is intentionally above Ursula's 256 MiB Raft RPC limit while still
/// preventing a corrupted length field from requesting an unbounded allocation.
pub const MAX_FRAME_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;

fn journal_header() -> [u8; JOURNAL_HEADER_LEN] {
    let mut header = [0_u8; JOURNAL_HEADER_LEN];
    header[..JOURNAL_MAGIC.len()].copy_from_slice(&JOURNAL_MAGIC);
    header[8..10].copy_from_slice(&JOURNAL_VERSION.to_le_bytes());
    header[10..12].copy_from_slice(
        &u16::try_from(JOURNAL_HEADER_LEN)
            .expect("journal header length fits u16")
            .to_le_bytes(),
    );
    header
}

/// Serialization seam: how one record becomes a frame payload and back.
///
/// `encode` is infallible because the codecs we use (protobuf, JSON over plain
/// owned types) cannot fail in practice; a codec with fallible encoding should
/// surface that as an `io::Error` from a panic-documented invariant instead.
pub trait FrameCodec {
    /// The record type carried in each frame.
    type Record;

    /// Serialize a record into a frame payload.
    fn encode(record: &Self::Record) -> Vec<u8>;

    /// Deserialize a frame payload back into a record.
    fn decode(payload: &[u8]) -> io::Result<Self::Record>;
}

/// JSON frame codec for any owned, serde-serializable record.
pub struct JsonCodec<T>(PhantomData<T>);

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
pub struct JournalWriter {
    file: Option<File>,
    parent_unsynced: bool,
}

impl JournalWriter {
    /// Create a writer. Set `needs_parent_sync` when the file may not exist yet, so
    /// the parent directory is `fsync`ed once the file is created.
    pub fn new(needs_parent_sync: bool) -> Self {
        Self {
            file: None,
            parent_unsynced: needs_parent_sync,
        }
    }

    /// Create and initialize the journal file even when there are no records.
    pub fn ensure_created(&mut self, path: &Path) -> io::Result<()> {
        let _ = self.file_mut(path)?;
        Ok(())
    }

    /// Append one record as a framed payload. Does not durably flush; pair with
    /// [`JournalWriter::sync`] once per batch.
    pub fn append<C: FrameCodec>(&mut self, path: &Path, record: &C::Record) -> io::Result<()> {
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
        let len = u32::try_from(payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "journal record too large"))?;
        let checksum = crc32fast::hash(&payload);
        let file = self.file_mut(path)?;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&checksum.to_le_bytes())?;
        file.write_all(&payload)
    }

    /// `fsync` the file data, plus the parent directory once if it was freshly created.
    pub fn sync(&mut self, path: &Path) -> io::Result<()> {
        let file = self.file.as_mut().expect("file opened before sync");
        file.sync_data()?;
        if self.parent_unsynced
            && let Some(parent) = path.parent()
            && let Ok(dir) = File::open(parent)
        {
            dir.sync_all()?;
            self.parent_unsynced = false;
        }
        Ok(())
    }

    fn file_mut(&mut self, path: &Path) -> io::Result<&mut File> {
        if self.file.is_none() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(path)?;
            let file_len = file.metadata()?.len();
            if file_len == 0 {
                file.write_all(&journal_header())?;
            } else {
                validate_file_header(&mut file, path, file_len)?;
            }
            self.file = Some(file);
        }
        Ok(self.file.as_mut().expect("file opened above"))
    }
}

/// Read every record from `path`, decoding with `C`. A torn trailing frame left by a
/// crash mid-write is truncated away and ignored, leaving the file at its last clean
/// record boundary.
pub fn replay<C: FrameCodec>(path: &Path) -> io::Result<Vec<C::Record>> {
    let mut records = Vec::new();
    replay_each::<C>(path, |record| {
        records.push(record);
        Ok(())
    })?;
    Ok(records)
}

/// Stream every valid record from `path` through `visit` without retaining the
/// entire journal in memory. A torn trailing frame is truncated with the same
/// recovery semantics as [`replay`].
pub fn replay_each<C: FrameCodec>(
    path: &Path,
    mut visit: impl FnMut(C::Record) -> io::Result<()>,
) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        return Ok(());
    }
    if file_len < u64::try_from(JOURNAL_HEADER_LEN).expect("header length fits u64") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "journal '{}' has no complete Ursula WAL header; legacy unversioned journals require an explicit reset or migration",
                path.display()
            ),
        ));
    }
    validate_file_header(&mut file, path, file_len)?;
    let mut valid_len = u64::try_from(JOURNAL_HEADER_LEN).expect("header length fits u64");
    let mut frame_index = 0_u64;
    while valid_len < file_len {
        let remaining = file_len.saturating_sub(valid_len);
        if remaining < u64::try_from(FRAME_HEADER_LEN).expect("frame header length fits u64") {
            break;
        }

        let mut len_bytes = [0_u8; 4];
        file.read_exact(&mut len_bytes)?;
        let payload_len = u64::from(u32::from_le_bytes(len_bytes));
        let mut checksum_bytes = [0_u8; 4];
        file.read_exact(&mut checksum_bytes)?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);
        if payload_len > u64::try_from(MAX_FRAME_PAYLOAD_BYTES).expect("frame limit fits u64") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "journal '{}' frame {} declares {} bytes, exceeding the {} byte limit",
                    path.display(),
                    frame_index + 1,
                    payload_len,
                    MAX_FRAME_PAYLOAD_BYTES
                ),
            ));
        }
        if remaining
            .saturating_sub(u64::try_from(FRAME_HEADER_LEN).expect("frame header length fits u64"))
            < payload_len
        {
            break;
        }

        let payload_len = usize::try_from(payload_len).expect("u32 fits usize");
        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)?;
        let actual_checksum = crc32fast::hash(&payload);
        if actual_checksum != expected_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "journal '{}' frame {} checksum mismatch: expected {expected_checksum:#010x}, got {actual_checksum:#010x}",
                    path.display(),
                    frame_index + 1
                ),
            ));
        }
        let record = C::decode(&payload).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "journal '{}' frame {} decode failed: {err}",
                    path.display(),
                    frame_index + 1
                ),
            )
        })?;
        visit(record)?;
        valid_len = valid_len
            .checked_add(
                u64::try_from(FRAME_HEADER_LEN)
                    .expect("frame header length fits u64")
                    .saturating_add(u64::try_from(payload_len).expect("usize fits u64")),
            )
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "journal offset overflow"))?;
        frame_index = frame_index.saturating_add(1);
    }

    if valid_len < file_len {
        truncate_to(
            path,
            usize::try_from(valid_len).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "journal offset exceeds usize")
            })?,
        )?;
    }
    Ok(())
}

/// Migrate the legacy `[length][payload]` journal format to the current
/// checksummed format, retaining a hard-linked `.v0.bak` rollback copy.
///
/// A current-format or empty journal is left untouched. Unsupported future
/// versions are also left for [`replay`] to reject explicitly.
pub fn migrate_legacy<C: FrameCodec>(path: &Path) -> io::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let mut source = File::open(path)?;
    let file_len = source.metadata()?.len();
    if file_len == 0 {
        return Ok(false);
    }
    let mut prefix = [0_u8; JOURNAL_MAGIC.len()];
    let prefix_len = source.read(&mut prefix)?;
    source.rewind()?;
    if prefix_len == JOURNAL_MAGIC.len() && prefix == JOURNAL_MAGIC {
        return Ok(false);
    }

    let migrate_path = suffixed_path(path, ".migrate-v1");
    let backup_path = suffixed_path(path, ".v0.bak");
    if migrate_path.exists() {
        fs::remove_file(&migrate_path)?;
    }
    let mut writer = JournalWriter::new(true);
    let mut valid_len = 0_u64;
    let mut frame_index = 0_u64;
    while valid_len < file_len {
        let remaining = file_len.saturating_sub(valid_len);
        if remaining < 4 {
            break;
        }
        let mut len_bytes = [0_u8; 4];
        source.read_exact(&mut len_bytes)?;
        let payload_len = u64::from(u32::from_le_bytes(len_bytes));
        if payload_len > u64::try_from(MAX_FRAME_PAYLOAD_BYTES).expect("frame limit fits u64") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "legacy journal '{}' frame {} declares {} bytes, exceeding the {} byte limit",
                    path.display(),
                    frame_index + 1,
                    payload_len,
                    MAX_FRAME_PAYLOAD_BYTES
                ),
            ));
        }
        if remaining.saturating_sub(4) < payload_len {
            break;
        }
        let payload_len = usize::try_from(payload_len).expect("u32 fits usize");
        let mut payload = vec![0_u8; payload_len];
        source.read_exact(&mut payload)?;
        let record = C::decode(&payload).map_err(|err| {
            io::Error::new(
                err.kind(),
                format!(
                    "legacy journal '{}' frame {} decode failed: {err}",
                    path.display(),
                    frame_index + 1
                ),
            )
        })?;
        writer.append::<C>(&migrate_path, &record)?;
        valid_len = valid_len
            .checked_add(4_u64.saturating_add(u64::try_from(payload_len).expect("usize fits u64")))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "journal offset overflow"))?;
        frame_index = frame_index.saturating_add(1);
    }
    if frame_index == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "journal '{}' has neither the Ursula WAL header nor a complete legacy frame",
                path.display()
            ),
        ));
    }
    if valid_len < file_len {
        tracing::warn!(
            path = %path.display(),
            valid_bytes = valid_len,
            discarded_torn_bytes = file_len.saturating_sub(valid_len),
            "discarding a torn legacy WAL tail during format migration"
        );
    }
    writer.sync(&migrate_path)?;
    drop(writer);

    if backup_path.exists() {
        fs::remove_file(&backup_path)?;
    }
    fs::hard_link(path, &backup_path)?;
    sync_parent(path)?;
    fs::rename(&migrate_path, path)?;
    sync_parent(path)?;
    Ok(true)
}

fn suffixed_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn sync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Decode framed records from an in-memory buffer, returning the records and the byte
/// length of the valid (fully-written) prefix. A torn trailing frame ends the scan.
pub fn decode_frames<C: FrameCodec>(bytes: &[u8]) -> io::Result<(Vec<C::Record>, usize)> {
    let mut records = Vec::new();
    if bytes.is_empty() {
        return Ok((records, 0));
    }
    if bytes.len() < JOURNAL_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "in-memory journal has no complete Ursula WAL header",
        ));
    }
    validate_header_bytes(&bytes[..JOURNAL_HEADER_LEN], "in-memory journal")?;
    let mut offset = JOURNAL_HEADER_LEN;
    let mut frame_index = 0_usize;
    while offset < bytes.len() {
        let Some(frame_header) = bytes.get(offset..offset.saturating_add(FRAME_HEADER_LEN)) else {
            return Ok((records, offset)); // torn length prefix
        };
        let len = usize::try_from(u32::from_le_bytes(
            frame_header[..4]
                .try_into()
                .expect("slice is exactly four bytes"),
        ))
        .expect("u32 fits usize");
        if len > MAX_FRAME_PAYLOAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "in-memory journal frame {} declares {len} bytes, exceeding the {MAX_FRAME_PAYLOAD_BYTES} byte limit",
                    frame_index + 1
                ),
            ));
        }
        let expected_checksum = u32::from_le_bytes(
            frame_header[4..8]
                .try_into()
                .expect("slice is exactly four bytes"),
        );
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
                    frame_index + 1
                ),
            ));
        }
        records.push(C::decode(payload)?);
        offset = end;
        frame_index = frame_index.saturating_add(1);
    }
    Ok((records, bytes.len()))
}

fn validate_file_header(file: &mut File, path: &Path, file_len: u64) -> io::Result<()> {
    if file_len < u64::try_from(JOURNAL_HEADER_LEN).expect("header length fits u64") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("journal '{}' has a torn file header", path.display()),
        ));
    }
    file.rewind()?;
    let mut header = [0_u8; JOURNAL_HEADER_LEN];
    file.read_exact(&mut header)?;
    validate_header_bytes(&header, &format!("journal '{}'", path.display()))
}

fn validate_header_bytes(header: &[u8], description: &str) -> io::Result<()> {
    if header.get(..JOURNAL_MAGIC.len()) != Some(JOURNAL_MAGIC.as_slice()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{description} has no Ursula WAL magic; legacy unversioned journals require an explicit reset or migration"
            ),
        ));
    }
    let version = u16::from_le_bytes(
        header[8..10]
            .try_into()
            .expect("validated journal header has version bytes"),
    );
    if version != JOURNAL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{description} uses unsupported Ursula WAL version {version}; this binary supports version {JOURNAL_VERSION}"
            ),
        ));
    }
    let header_len = usize::from(u16::from_le_bytes(
        header[10..12]
            .try_into()
            .expect("validated journal header has length bytes"),
    ));
    if header_len != JOURNAL_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{description} declares unsupported header length {header_len}; expected {JOURNAL_HEADER_LEN}"
            ),
        ));
    }
    if header[12..].iter().any(|byte| *byte != 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{description} has non-zero reserved header bytes"),
        ));
    }
    Ok(())
}

/// Truncate `path` to `valid_len` bytes, dropping a torn trailing frame, then `fsync`.
pub fn truncate_to(path: &Path, valid_len: usize) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(u64::try_from(valid_len).expect("valid frame offset fits u64"))?;
    file.sync_data()
}

#[cfg(test)]
mod tests {
    use std::io::SeekFrom;

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
        assert!(err.to_string().contains("explicit reset or migration"));
    }

    #[test]
    fn legacy_migration_preserves_records_and_a_rollback_copy() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal");
        let records = ["first".to_owned(), "second".to_owned()];
        let mut legacy = File::create(&path).expect("create legacy journal");
        for record in &records {
            let payload = serde_json::to_vec(record).expect("encode legacy payload");
            legacy
                .write_all(
                    &u32::try_from(payload.len())
                        .expect("payload length fits u32")
                        .to_le_bytes(),
                )
                .expect("write legacy length");
            legacy.write_all(&payload).expect("write legacy payload");
        }
        legacy.sync_data().expect("sync legacy journal");
        drop(legacy);

        assert!(migrate_legacy::<JsonCodec<String>>(&path).expect("migrate legacy journal"));
        assert_eq!(
            replay::<JsonCodec<String>>(&path).expect("replay migrated journal"),
            records
        );
        assert!(suffixed_path(&path, ".v0.bak").exists());
        assert!(!migrate_legacy::<JsonCodec<String>>(&path).expect("migration is idempotent"));
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
        file.write_all(&2_u16.to_le_bytes())
            .expect("write unsupported version");
        file.sync_data().expect("sync unsupported version");

        let err = replay::<JsonCodec<String>>(&path).expect_err("version must fail closed");
        assert!(err.to_string().contains("unsupported Ursula WAL version 2"));
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
