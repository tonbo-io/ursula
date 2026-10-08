use std::future::Future;
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Sub;
use std::ops::SubAssign;
use std::pin::pin;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use futures_util::TryFutureExt;
use openraft_rt::AsyncRuntime;
use openraft_rt::Instant;
use openraft_rt::Mpsc;
use openraft_rt::MpscReceiver;
use openraft_rt::MpscSender;
use openraft_rt::MpscWeakSender;
use openraft_rt::Mutex;
use openraft_rt::Oneshot;
use openraft_rt::OneshotSender;
use openraft_rt::OptionalSend;
use openraft_rt::OptionalSync;
use openraft_rt::RecvError;
use openraft_rt::SendError;
use openraft_rt::TryRecvError;
use openraft_rt::Watch;
use openraft_rt::WatchReceiver;
use openraft_rt::WatchSender;
use sim_tokio::sync::Notify;
use sim_tokio::sync::mpsc;

pub type MadsimOpenRaftRuntime = openraft_rt::deterministic_rng::DeterministicRng<MadsimRuntime>;

pin_project_lite::pin_project! {
    pub struct MadsimTimeout<T> {
        #[pin]
        future: T,
        #[pin]
        sleep: sim_tokio::time::Sleep,
    }
}

impl<T> MadsimTimeout<T> {
    fn new(duration: Duration, future: T) -> Self {
        Self {
            future,
            sleep: sim_tokio::time::sleep(duration),
        }
    }
}

impl<T> Future for MadsimTimeout<T>
where T: Future
{
    type Output = Result<T::Output, sim_tokio::time::error::Elapsed>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(output) = this.future.poll(cx) {
            return Poll::Ready(Ok(output));
        }
        if this.sleep.poll(cx).is_ready() {
            return Poll::Ready(Err(sim_tokio::time::error::Elapsed));
        }
        Poll::Pending
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MadsimRuntime;

impl AsyncRuntime for MadsimRuntime {
    type JoinError = sim_tokio::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = sim_tokio::task::JoinHandle<T>;
    type Sleep = sim_tokio::time::Sleep;
    type Instant = MadsimInstant;
    type TimeoutError = sim_tokio::time::error::Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = MadsimTimeout<T>;
    type ThreadLocalRng = rand::rngs::ThreadRng;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        sim_tokio::spawn(future)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        sim_tokio::time::sleep(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        sim_tokio::time::sleep_until(deadline.0)
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(
        duration: Duration,
        future: F,
    ) -> Self::Timeout<R, F> {
        MadsimTimeout::new(duration, future)
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(
        deadline: Self::Instant,
        future: F,
    ) -> Self::Timeout<R, F> {
        let duration = deadline
            .0
            .saturating_duration_since(sim_tokio::time::Instant::now());
        MadsimTimeout::new(duration, future)
    }

    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        rand::rng()
    }

    type Mpsc = MadsimMpsc;
    type Watch = MadsimWatch;
    type Oneshot = MadsimOneshot;
    type Mutex<T: OptionalSend + 'static> = MadsimMutex<T>;

    fn new(_threads: usize) -> Self {
        Self
    }

    fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T>,
        T: OptionalSend,
    {
        madsim::runtime::Runtime::new().block_on(future)
    }

    #[allow(clippy::manual_async_fn)]
    fn spawn_blocking<F, T>(f: F) -> impl Future<Output = Result<T, std::io::Error>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        async move { Ok(f()) }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct MadsimInstant(sim_tokio::time::Instant);

impl Add<Duration> for MadsimInstant {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0.add(rhs))
    }
}

impl AddAssign<Duration> for MadsimInstant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0.add_assign(rhs)
    }
}

impl Sub<Duration> for MadsimInstant {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}

impl Sub<Self> for MadsimInstant {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0.sub(rhs.0)
    }
}

impl SubAssign<Duration> for MadsimInstant {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0.sub_assign(rhs)
    }
}

impl Instant for MadsimInstant {
    fn now() -> Self {
        Self(sim_tokio::time::Instant::now())
    }

    fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

pub struct MadsimMpsc;

pub struct MadsimMpscSender<T>(mpsc::Sender<T>);
pub struct MadsimMpscReceiver<T>(mpsc::Receiver<T>);
pub struct MadsimMpscWeakSender<T>(mpsc::WeakSender<T>);

impl<T> Clone for MadsimMpscSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for MadsimMpscWeakSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Mpsc for MadsimMpsc {
    type Sender<T: OptionalSend> = MadsimMpscSender<T>;
    type Receiver<T: OptionalSend> = MadsimMpscReceiver<T>;
    type WeakSender<T: OptionalSend> = MadsimMpscWeakSender<T>;

    fn channel<T: OptionalSend>(buffer: usize) -> (Self::Sender<T>, Self::Receiver<T>) {
        let (tx, rx) = mpsc::channel(buffer);
        (MadsimMpscSender(tx), MadsimMpscReceiver(rx))
    }
}

impl<T> MpscSender<MadsimMpsc, T> for MadsimMpscSender<T>
where T: OptionalSend
{
    fn send(&self, msg: T) -> impl Future<Output = Result<(), SendError<T>>> + OptionalSend {
        self.0.send(msg).map_err(|err| SendError(err.0))
    }

    fn downgrade(&self) -> <MadsimMpsc as Mpsc>::WeakSender<T> {
        MadsimMpscWeakSender(self.0.downgrade())
    }
}

impl<T> MpscReceiver<T> for MadsimMpscReceiver<T>
where T: OptionalSend
{
    fn recv(&mut self) -> impl Future<Output = Option<T>> + OptionalSend {
        self.0.recv()
    }

    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.0.try_recv().map_err(|err| match err {
            mpsc::error::TryRecvError::Empty => TryRecvError::Empty,
            mpsc::error::TryRecvError::Disconnected => TryRecvError::Disconnected,
        })
    }
}

impl<T> MpscWeakSender<MadsimMpsc, T> for MadsimMpscWeakSender<T>
where T: OptionalSend
{
    fn upgrade(&self) -> Option<<MadsimMpsc as Mpsc>::Sender<T>> {
        self.0.upgrade().map(MadsimMpscSender)
    }
}

pub struct MadsimOneshot;

pub struct MadsimOneshotSender<T>(sim_tokio::sync::oneshot::Sender<T>);

impl Oneshot for MadsimOneshot {
    type Sender<T: OptionalSend> = MadsimOneshotSender<T>;
    type Receiver<T: OptionalSend> = sim_tokio::sync::oneshot::Receiver<T>;
    type ReceiverError = sim_tokio::sync::oneshot::error::RecvError;

    fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>)
    where T: OptionalSend {
        let (tx, rx) = sim_tokio::sync::oneshot::channel();
        (MadsimOneshotSender(tx), rx)
    }
}

impl<T> OneshotSender<T> for MadsimOneshotSender<T>
where T: OptionalSend
{
    fn send(self, t: T) -> Result<(), T> {
        self.0.send(t)
    }
}

pub struct MadsimMutex<T>(sim_tokio::sync::Mutex<T>);

impl<T> Mutex<T> for MadsimMutex<T>
where T: OptionalSend + 'static
{
    type Guard<'a> = sim_tokio::sync::MutexGuard<'a, T>;

    fn new(value: T) -> Self {
        Self(sim_tokio::sync::Mutex::new(value))
    }

    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + OptionalSend {
        self.0.lock()
    }
}

/// OpenRaft's watch channel under madsim.
///
/// `tokio::sync::watch` parks each waiting receiver on one of eight `Notify`s
/// picked by tokio's thread-local RNG and wakes them slot by slot. tokio seeds
/// that RNG from a process-wide counter, so when one send wakes several
/// receivers (a leader's commit reaching every replication stream) the wake
/// order, and with it the simulated schedule, differs between two runs of one
/// seed in the same process, as `Runtime::check_determinism` runs them. This
/// channel parks every receiver on one `Notify`, which wakes them in the order
/// they started waiting.
pub struct MadsimWatch;

struct WatchShared<T> {
    value: RwLock<T>,
    /// Bumped by every notifying change, under the value's write lock.
    version: AtomicU64,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    changed: Notify,
}

impl<T> WatchShared<T> {
    fn read(&self) -> RwLockReadGuard<'_, T> {
        self.value.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.value.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Bumps the version while `value` is still locked, then wakes every
    /// waiting receiver.
    fn publish(&self, value: RwLockWriteGuard<'_, T>) {
        self.version.fetch_add(1, Ordering::SeqCst);
        drop(value);
        self.changed.notify_waiters();
    }

    fn subscribe(self: &Arc<Self>) -> MadsimWatchReceiver<T> {
        self.receivers.fetch_add(1, Ordering::SeqCst);
        MadsimWatchReceiver {
            shared: Arc::clone(self),
            seen: self.version.load(Ordering::SeqCst),
        }
    }
}

pub struct MadsimWatchSender<T>(Arc<WatchShared<T>>);

pub struct MadsimWatchReceiver<T> {
    shared: Arc<WatchShared<T>>,
    /// The version this receiver last marked seen.
    seen: u64,
}

impl<T> Clone for MadsimWatchSender<T> {
    fn clone(&self) -> Self {
        self.0.senders.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(&self.0))
    }
}

impl<T> Drop for MadsimWatchSender<T> {
    fn drop(&mut self) {
        if self.0.senders.fetch_sub(1, Ordering::SeqCst) == 1 {
            // The last sender is gone: waiting receivers see the channel closed.
            self.0.changed.notify_waiters();
        }
    }
}

impl<T> Clone for MadsimWatchReceiver<T> {
    fn clone(&self) -> Self {
        self.shared.receivers.fetch_add(1, Ordering::SeqCst);
        Self {
            shared: Arc::clone(&self.shared),
            seen: self.seen,
        }
    }
}

impl<T> Drop for MadsimWatchReceiver<T> {
    fn drop(&mut self) {
        self.shared.receivers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Watch for MadsimWatch {
    type Sender<T: OptionalSend + OptionalSync> = MadsimWatchSender<T>;
    type Receiver<T: OptionalSend + OptionalSync> = MadsimWatchReceiver<T>;
    type Ref<'a, T: OptionalSend + 'a> = RwLockReadGuard<'a, T>;

    fn channel<T: OptionalSend + OptionalSync>(init: T) -> (Self::Sender<T>, Self::Receiver<T>) {
        let shared = Arc::new(WatchShared {
            value: RwLock::new(init),
            version: AtomicU64::new(0),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(0),
            changed: Notify::new(),
        });
        let rx = shared.subscribe();
        (MadsimWatchSender(shared), rx)
    }
}

impl<T> WatchSender<MadsimWatch, T> for MadsimWatchSender<T>
where T: OptionalSend + OptionalSync
{
    fn send(&self, value: T) -> Result<(), openraft_rt::watch::SendError<T>> {
        if self.0.receivers.load(Ordering::SeqCst) == 0 {
            return Err(openraft_rt::watch::SendError(value));
        }
        let mut current = self.0.write();
        *current = value;
        self.0.publish(current);
        Ok(())
    }

    fn send_if_modified<F>(&self, modify: F) -> bool
    where F: FnOnce(&mut T) -> bool {
        let mut current = self.0.write();
        let modified = modify(&mut current);
        if modified {
            self.0.publish(current);
        }
        modified
    }

    fn borrow_watched(&self) -> <MadsimWatch as Watch>::Ref<'_, T> {
        self.0.read()
    }

    fn subscribe(&self) -> <MadsimWatch as Watch>::Receiver<T> {
        self.0.subscribe()
    }
}

impl<T> WatchReceiver<MadsimWatch, T> for MadsimWatchReceiver<T>
where T: OptionalSend + OptionalSync
{
    async fn changed(&mut self) -> Result<(), RecvError> {
        loop {
            // Register before checking, so a send between the check and the
            // wait still wakes this receiver.
            let mut notified = pin!(self.shared.changed.notified());
            notified.as_mut().enable();
            let version = self.shared.version.load(Ordering::SeqCst);
            if version != self.seen {
                self.seen = version;
                return Ok(());
            }
            if self.shared.senders.load(Ordering::SeqCst) == 0 {
                return Err(RecvError(()));
            }
            notified.await;
        }
    }

    fn borrow_watched(&self) -> <MadsimWatch as Watch>::Ref<'_, T> {
        self.shared.read()
    }

    fn borrow_and_update(&mut self) -> <MadsimWatch as Watch>::Ref<'_, T> {
        // Publishers advance the version under this same value lock. Mark
        // exactly the value being returned seen, including sends after changed().
        let value = self.shared.read();
        self.seen = self.shared.version.load(Ordering::SeqCst);
        value
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;
    use openraft_rt::RecvError;
    use openraft_rt::Watch;
    use openraft_rt::WatchReceiver;
    use openraft_rt::WatchSender;

    use super::MadsimWatch;

    #[test]
    fn borrowed_watch_update_is_not_delivered_twice() {
        madsim::runtime::Runtime::new().block_on(async {
            let (tx, mut rx) = MadsimWatch::channel(0);
            tx.send(1).unwrap();
            rx.changed().await.unwrap();
            // A new command arrives after changed() but before borrowing it.
            tx.send(2).unwrap();
            assert_eq!(*rx.borrow_and_update(), 2);
            assert!(rx.changed().now_or_never().is_none());
            tx.send(3).unwrap();
            rx.changed().await.unwrap();
            assert_eq!(*rx.borrow_and_update(), 3);
            drop(tx);
            assert!(matches!(rx.changed().await, Err(RecvError(()))));
        });
    }
}
