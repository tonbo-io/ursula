use std::future::Future;
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Sub;
use std::ops::SubAssign;
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
use sim_tokio::sync::mpsc;

use crate::sim_watch as watch;

pub type MadsimOpenRaftRuntime = openraft_rt::deterministic_rng::DeterministicRng<MadsimRuntime>;

/// The stack of a simulation's thread: the 8 MiB of the main thread the
/// `ursula-sim` subcommands ran their seeds on before each run got a thread
/// of its own.
const SIM_THREAD_STACK_BYTES: usize = 8 << 20;

/// Runs the future `workload` builds under a fresh madsim runtime for
/// `seed`, on a fresh OS thread, then runs `after` on that thread, whether
/// the workload returned or panicked. Returns both to the caller, the
/// workload's panic as `Err`.
///
/// Every simulation goes through here (`ursula-sim`'s `run_with_madsim`,
/// this crate's madsim tests, as the `nondeterminism` audit enforces), so no
/// run starts on a thread an earlier run used. A run leaves state in its
/// thread's thread-locals that madsim does not reset: lazily seeded ones
/// such as `rand`'s thread RNG and std's `RandomState` keys draw from
/// madsim's intercepted `getrandom` only on their first use on a thread, so
/// a second run on that thread would not spend those draws and every later
/// draw of madsim's RNG would shift (#638). `after` reads the run's own
/// thread-locals before the thread ends.
pub fn run_isolated<W, Fut, A, R>(
    seed: u64,
    time_limit: Option<Duration>,
    workload: W,
    after: A,
) -> (std::thread::Result<Fut::Output>, R)
where
    W: FnOnce() -> Fut + Send,
    Fut: Future,
    Fut::Output: Send,
    A: FnOnce() -> R + Send,
    R: Send,
{
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name(format!("madsim-seed-{seed}"))
            .stack_size(SIM_THREAD_STACK_BYTES)
            .spawn_scoped(scope, || {
                // The runtime comes first: nothing on this thread may draw
                // a lazily seeded thread-local before madsim intercepts it.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut runtime =
                        madsim::runtime::Runtime::with_seed_and_config(seed, Default::default());
                    if let Some(limit) = time_limit {
                        runtime.set_time_limit(limit);
                    }
                    runtime.block_on(workload())
                }));
                (result, after())
            })
            .expect("spawn the simulation thread")
            .join()
            .expect("the simulation thread catches the run's panic")
    })
}

/// [`run_isolated`] for a test: the workload's output, its panic resumed on
/// the caller.
#[cfg(test)]
pub(crate) fn run_seeded<W, Fut>(
    seed: u64,
    time_limit: Option<Duration>,
    workload: W,
) -> Fut::Output
where
    W: FnOnce() -> Fut + Send,
    Fut: Future,
    Fut::Output: Send,
{
    let (result, ()) = run_isolated(seed, time_limit, workload, || ());
    result.unwrap_or_else(|payload| std::panic::resume_unwind(payload))
}

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
    where F: Future<Output = T> {
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

/// OpenRaft's watch channel under madsim: [`crate::sim_watch`], which wakes
/// its receivers in the order they started waiting instead of in an order
/// tokio's process-seeded RNG picks. The wrappers expose only OpenRaft's
/// traits.
pub struct MadsimWatch;

pub struct MadsimWatchSender<T>(watch::Sender<T>);

pub struct MadsimWatchReceiver<T>(watch::Receiver<T>);

impl<T> Clone for MadsimWatchSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for MadsimWatchReceiver<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Watch for MadsimWatch {
    type Sender<T: OptionalSend + OptionalSync> = MadsimWatchSender<T>;
    type Receiver<T: OptionalSend + OptionalSync> = MadsimWatchReceiver<T>;
    type Ref<'a, T: OptionalSend + 'a> = watch::Ref<'a, T>;

    fn channel<T: OptionalSend + OptionalSync>(init: T) -> (Self::Sender<T>, Self::Receiver<T>) {
        let (tx, rx) = watch::channel(init);
        (MadsimWatchSender(tx), MadsimWatchReceiver(rx))
    }
}

impl<T> WatchSender<MadsimWatch, T> for MadsimWatchSender<T>
where T: OptionalSend + OptionalSync
{
    fn send(&self, value: T) -> Result<(), openraft_rt::watch::SendError<T>> {
        self.0
            .send(value)
            .map_err(|watch::error::SendError(value)| openraft_rt::watch::SendError(value))
    }

    fn send_if_modified<F>(&self, modify: F) -> bool
    where F: FnOnce(&mut T) -> bool {
        self.0.send_if_modified(modify)
    }

    fn borrow_watched(&self) -> <MadsimWatch as Watch>::Ref<'_, T> {
        self.0.borrow()
    }

    fn subscribe(&self) -> <MadsimWatch as Watch>::Receiver<T> {
        MadsimWatchReceiver(self.0.subscribe())
    }
}

impl<T> WatchReceiver<MadsimWatch, T> for MadsimWatchReceiver<T>
where T: OptionalSend + OptionalSync
{
    async fn changed(&mut self) -> Result<(), RecvError> {
        self.0.changed().await.map_err(|_closed| RecvError(()))
    }

    fn borrow_watched(&self) -> <MadsimWatch as Watch>::Ref<'_, T> {
        self.0.borrow()
    }

    fn borrow_and_update(&mut self) -> <MadsimWatch as Watch>::Ref<'_, T> {
        self.0.borrow_and_update()
    }
}
