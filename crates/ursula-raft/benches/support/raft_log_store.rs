//! Benchmark-only adapter based on OpenRaft's `examples/log-wal` store.

use std::any::type_name;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::fs;
use std::io;
use std::marker::PhantomData;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::LogIdOptionExt;
use openraft::OptionalSend;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftVote;
use raft_log::Config;
use raft_log::RaftLog;
use raft_log::api::raft_log_writer::RaftLogWriter;
use raft_log::codeq::Decode;
use raft_log::codeq::Encode;
use raft_log::codeq::OffsetWriter;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;
use tokio::sync::oneshot;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct MsgPack<T>(T);

impl<T: Serialize> MsgPack<T> {
    fn encoded_len(&self) -> u64 {
        encode_msgpack(&self.0, io::sink())
            .map(|len| u64::try_from(len).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

impl<T: Serialize> Encode for MsgPack<T> {
    fn encode<W: io::Write>(&self, writer: W) -> Result<usize, io::Error> {
        encode_msgpack(&self.0, writer)
    }
}

impl<T: DeserializeOwned> Decode for MsgPack<T> {
    fn decode<R: io::Read>(reader: R) -> Result<Self, io::Error> {
        decode_msgpack(reader).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MsgPackVote<C: RaftTypeConfig>(VoteOf<C>);

impl<C: RaftTypeConfig> PartialOrd for MsgPackVote<C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        RaftVote::partial_cmp(&self.0, &other.0)
    }
}

impl<C: RaftTypeConfig> Encode for MsgPackVote<C> {
    fn encode<W: io::Write>(&self, writer: W) -> Result<usize, io::Error> {
        encode_msgpack(&self.0, writer)
    }
}

impl<C: RaftTypeConfig> Decode for MsgPackVote<C> {
    fn decode<R: io::Read>(reader: R) -> Result<Self, io::Error> {
        decode_msgpack(reader).map(Self)
    }
}

fn encode_msgpack<T: Serialize>(value: &T, mut writer: impl io::Write) -> io::Result<usize> {
    let mut offset = OffsetWriter::new(&mut writer);
    rmp_serde::encode::write_named(&mut offset, value).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{err}; encoding {}", type_name::<T>()),
        )
    })?;
    Ok(offset.offset())
}

fn decode_msgpack<T: DeserializeOwned>(reader: impl io::Read) -> io::Result<T> {
    rmp_serde::decode::from_read(reader).map_err(|err| {
        let kind = match &err {
            rmp_serde::decode::Error::InvalidMarkerRead(io_err)
            | rmp_serde::decode::Error::InvalidDataRead(io_err) => io_err.kind(),
            _ => io::ErrorKind::InvalidData,
        };
        io::Error::new(kind, format!("{err}; decoding {}", type_name::<T>()))
    })
}

enum Callback<C: RaftTypeConfig> {
    IOFlushed(IOFlushed<C>),
    Oneshot(oneshot::Sender<Result<(), io::Error>>),
}

impl<C: RaftTypeConfig> raft_log::Callback for Callback<C> {
    fn send(self, result: Result<(), io::Error>) {
        match self {
            Self::IOFlushed(callback) => callback.io_completed(result),
            Self::Oneshot(tx) => {
                let _ = tx.send(result);
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct WalTypes<C>(PhantomData<C>);

impl<C> raft_log::Types for WalTypes<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    type LogId = MsgPack<LogIdOf<C>>;
    type LogPayload = MsgPack<EntryOf<C>>;
    type Vote = MsgPackVote<C>;
    type Callback = Callback<C>;
    type UserData = MsgPack<()>;

    fn log_index(log_id: &Self::LogId) -> u64 {
        log_id.0.index
    }

    fn payload_size(payload: &Self::LogPayload) -> u64 {
        payload.encoded_len()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BenchmarkRaftLogStore<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    inner: Arc<RwLock<RaftLog<WalTypes<C>>>>,
}

impl<C> BenchmarkRaftLogStore<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    pub(crate) fn open(dir: impl ToString) -> io::Result<Self> {
        let mut config = Config::new(dir);
        // The upstream default is 1 GiB per store. Ursula may host hundreds of
        // groups on one core, so the comparison uses an explicit small budget.
        config.log_cache_max_items = Some(4_096);
        config.log_cache_capacity = Some(4 * 1024 * 1024);
        fs::create_dir_all(&config.wal.dir)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(RaftLog::open(Arc::new(config))?)),
        })
    }

    pub(crate) async fn append_durable(&self, entries: Vec<EntryOf<C>>) -> io::Result<()> {
        let entries = entries.into_iter().map(|entry| {
            let log_id = entry.log_id().clone();
            (MsgPack(log_id), MsgPack(entry))
        });
        let (tx, rx) = oneshot::channel();
        {
            let mut log = self.inner.write().await;
            log.append(entries)?;
            log.flush(true, Some(Callback::Oneshot(tx)))?;
        }
        rx.await.map_err(io::Error::other)??;
        Ok(())
    }
}

impl<C> RaftLogReader<C> for BenchmarkRaftLogStore<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<EntryOf<C>>> {
        let (start, end) = range_boundary(range);
        self.inner
            .read()
            .await
            .read(start, end)
            .map(|result| result.map(|(_, payload)| payload.0))
            .collect()
    }

    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<C>>> {
        Ok(self
            .inner
            .read()
            .await
            .log_state()
            .vote()
            .map(|vote| vote.0.clone()))
    }
}

impl<C> RaftLogStorage<C> for BenchmarkRaftLogStore<C>
where
    C: RaftTypeConfig,
    EntryOf<C>: Clone,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<C>> {
        let log = self.inner.read().await;
        Ok(LogState {
            last_purged_log_id: log.log_state().purged().map(|id| id.0.clone()),
            last_log_id: log.log_state().last().map(|id| id.0.clone()),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut log = self.inner.write().await;
            log.save_vote(MsgPackVote(vote.clone()))?;
            log.flush(true, Some(Callback::Oneshot(tx)))?;
        }
        rx.await.map_err(io::Error::other)??;
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> io::Result<()> {
        if let Some(committed) = committed {
            self.inner.write().await.commit(MsgPack(committed))?;
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<C>>> {
        Ok(self
            .inner
            .read()
            .await
            .log_state()
            .committed()
            .map(|id| id.0.clone()))
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> io::Result<()>
    where
        I: IntoIterator<Item = EntryOf<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().map(|entry| {
            let log_id = entry.log_id().clone();
            (MsgPack(log_id), MsgPack(entry))
        });
        let mut log = self.inner.write().await;
        log.append(entries)?;
        log.flush(true, Some(Callback::IOFlushed(callback)))
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> io::Result<()> {
        let truncate_at = last_log_id.next_index();
        let mut log = self.inner.write().await;
        if truncate_at < log.log_state().last().map(|id| id.0.clone()).next_index() {
            log.truncate(truncate_at)?;
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> io::Result<()> {
        let mut log = self.inner.write().await;
        if log_id.index >= log.log_state().purged().map(|id| id.0.clone()).next_index() {
            log.purge(MsgPack(log_id))?;
            log.flush(true, None)?;
        }
        Ok(())
    }
}

fn range_boundary(range: impl RangeBounds<u64>) -> (u64, u64) {
    let start = match range.start_bound() {
        Bound::Included(&index) => index,
        Bound::Excluded(&index) => index.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&index) => index.saturating_add(1),
        Bound::Excluded(&index) => index,
        Bound::Unbounded => u64::MAX,
    };
    (start, end)
}
