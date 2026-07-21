//! Append-only framed journal.
//!
//! Persistence is kept orthogonal to serialization. The journal moves opaque
//! `[u32-LE length][payload]` frames to and from a file and handles the durability
//! concerns — append, `fsync`, and recovery of a torn trailing frame after a crash.
//! How a record turns into a payload is entirely the [`FrameCodec`]'s business, so
//! the Raft log store can frame protobuf while the WAL engine frames JSON over the
//! exact same code.

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;

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

    /// Append one record as a framed payload. Does not durably flush; pair with
    /// [`JournalWriter::sync`] once per batch.
    pub fn append<C: FrameCodec>(&mut self, path: &Path, record: &C::Record) -> io::Result<()> {
        let payload = C::encode(record);
        let len = u32::try_from(payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "journal record too large"))?;
        let file = self.file_mut(path)?;
        file.write_all(&len.to_le_bytes())?;
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
            self.file = Some(OpenOptions::new().create(true).append(true).open(path)?);
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
    let mut valid_len = 0_u64;
    while valid_len < file_len {
        let remaining = file_len.saturating_sub(valid_len);
        if remaining < 4 {
            break;
        }

        let mut len_bytes = [0_u8; 4];
        file.read_exact(&mut len_bytes)?;
        let payload_len = u64::from(u32::from_le_bytes(len_bytes));
        if remaining.saturating_sub(4) < payload_len {
            break;
        }

        let payload_len = usize::try_from(payload_len).expect("u32 fits usize");
        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)?;
        visit(C::decode(&payload)?)?;
        valid_len = valid_len
            .checked_add(4_u64.saturating_add(u64::try_from(payload_len).expect("usize fits u64")))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "journal offset overflow"))?;
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

/// Decode framed records from an in-memory buffer, returning the records and the byte
/// length of the valid (fully-written) prefix. A torn trailing frame ends the scan.
pub fn decode_frames<C: FrameCodec>(bytes: &[u8]) -> io::Result<(Vec<C::Record>, usize)> {
    let mut records = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let Some(len_bytes) = bytes.get(offset..offset.saturating_add(4)) else {
            return Ok((records, offset)); // torn length prefix
        };
        let len = usize::try_from(u32::from_le_bytes(
            len_bytes.try_into().expect("slice is exactly four bytes"),
        ))
        .expect("u32 fits usize");
        let start = offset.saturating_add(4);
        let end = start.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "journal frame length overflow")
        })?;
        let Some(payload) = bytes.get(start..end) else {
            return Ok((records, offset)); // torn payload
        };
        records.push(C::decode(payload)?);
        offset = end;
    }
    Ok((records, bytes.len()))
}

/// Truncate `path` to `valid_len` bytes, dropping a torn trailing frame, then `fsync`.
pub fn truncate_to(path: &Path, valid_len: usize) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(u64::try_from(valid_len).expect("valid frame offset fits u64"))?;
    file.sync_data()
}

#[cfg(test)]
mod tests {
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
}
