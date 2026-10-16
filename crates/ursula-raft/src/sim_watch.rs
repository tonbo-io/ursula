//! A watch channel with tokio's API that wakes its receivers in a fixed
//! order, for `cfg(madsim)` builds.
//!
//! `tokio::sync::watch` parks each waiting receiver on one of eight
//! `Notify`s picked by tokio's thread-local RNG and wakes them slot by slot.
//! tokio seeds that RNG from a process-wide counter, so when one send wakes
//! several receivers (every snapshot permit waiter, every replication stream
//! of a leader's commit) the wake order, and with it the simulated schedule,
//! depends on what ran earlier in the process (#638). This channel parks
//! every receiver on one `Notify`, which wakes them in the order they started
//! waiting. `crate::rt::sync::watch` is this module under `cfg(madsim)`, and
//! OpenRaft's watch (`MadsimWatch`) is built on it.
//!
//! It implements the part of tokio's API the crate uses.

use std::ops::Deref;
use std::pin::pin;
use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use sim_tokio::sync::Notify;

pub mod error {
    //! The errors of [`super`], as in `tokio::sync::watch::error`.

    /// Every sender is gone.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
    #[error("channel closed")]
    pub struct RecvError(pub(super) ());

    /// Every receiver is gone: the value comes back.
    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    #[error("channel closed")]
    pub struct SendError<T>(pub T);
}

struct Shared<T> {
    value: RwLock<T>,
    /// Bumped by every notifying change, under the value's write lock.
    version: AtomicU64,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    changed: Notify,
}

impl<T> Shared<T> {
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

    fn subscribe(self: &Arc<Self>) -> Receiver<T> {
        self.receivers.fetch_add(1, Ordering::SeqCst);
        Receiver {
            shared: Arc::clone(self),
            seen: self.version.load(Ordering::SeqCst),
        }
    }

    fn closed(&self) -> bool {
        self.senders.load(Ordering::SeqCst) == 0
    }
}

/// A borrowed value, as `tokio::sync::watch::Ref`.
pub struct Ref<'a, T>(RwLockReadGuard<'a, T>);

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// A channel holding `init`, with one receiver.
pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let sender = Sender::new(init);
    let receiver = sender.subscribe();
    (sender, receiver)
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Sender<T> {
    /// A channel holding `init`, with no receiver yet.
    pub fn new(init: T) -> Self {
        Self {
            shared: Arc::new(Shared {
                value: RwLock::new(init),
                version: AtomicU64::new(0),
                senders: AtomicUsize::new(1),
                receivers: AtomicUsize::new(0),
                changed: Notify::new(),
            }),
        }
    }

    /// Replaces the value and notifies, or gives it back when no receiver
    /// is left.
    pub fn send(&self, value: T) -> Result<(), error::SendError<T>> {
        if self.shared.receivers.load(Ordering::SeqCst) == 0 {
            return Err(error::SendError(value));
        }
        self.send_replace(value);
        Ok(())
    }

    /// Replaces the value, notifies and returns the old one, whether or not
    /// a receiver is left.
    pub fn send_replace(&self, value: T) -> T {
        let mut current = self.shared.write();
        let old = std::mem::replace(&mut *current, value);
        self.shared.publish(current);
        old
    }

    /// Modifies the value in place and notifies.
    pub fn send_modify<F>(&self, modify: F)
    where F: FnOnce(&mut T) {
        self.send_if_modified(|value| {
            modify(value);
            true
        });
    }

    /// Modifies the value in place and notifies when `modify` says it
    /// changed it.
    pub fn send_if_modified<F>(&self, modify: F) -> bool
    where F: FnOnce(&mut T) -> bool {
        let mut current = self.shared.write();
        let modified = modify(&mut current);
        if modified {
            self.shared.publish(current);
        }
        modified
    }

    pub fn borrow(&self) -> Ref<'_, T> {
        Ref(self.shared.read())
    }

    /// A receiver that has seen the current value.
    pub fn subscribe(&self) -> Receiver<T> {
        self.shared.subscribe()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::SeqCst);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::SeqCst) == 1 {
            // The last sender is gone: waiting receivers see the channel closed.
            self.shared.changed.notify_waiters();
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender")
            .field("value", &*self.shared.read())
            .finish()
    }
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The version this receiver last marked seen.
    seen: u64,
}

impl<T> Receiver<T> {
    /// Waits for a value this receiver has not seen, or fails once every
    /// sender is gone.
    pub async fn changed(&mut self) -> Result<(), error::RecvError> {
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
            if self.shared.closed() {
                return Err(error::RecvError(()));
            }
            notified.await;
        }
    }

    pub fn borrow(&self) -> Ref<'_, T> {
        Ref(self.shared.read())
    }

    /// Borrows the value and marks it seen.
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        // Publishers advance the version under this same value lock, so this
        // marks exactly the value returned seen.
        let value = self.shared.read();
        self.seen = self.shared.version.load(Ordering::SeqCst);
        Ref(value)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.receivers.fetch_add(1, Ordering::SeqCst);
        Self {
            shared: Arc::clone(&self.shared),
            seen: self.seen,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receivers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver")
            .field("seen", &self.seen)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;

    use super::channel;
    use super::error::RecvError;

    #[test]
    fn a_value_borrowed_after_changed_is_not_delivered_twice() {
        crate::sim_runtime::run_seeded(0, None, || async {
            let (tx, mut rx) = channel(0);
            tx.send(1).unwrap();
            rx.changed().await.unwrap();
            // A send after changed() but before the borrow.
            tx.send(2).unwrap();
            assert_eq!(*rx.borrow_and_update(), 2);
            assert!(rx.changed().now_or_never().is_none());
            tx.send_replace(3);
            rx.changed().await.unwrap();
            assert_eq!(*rx.borrow_and_update(), 3);
            drop(tx);
            assert!(matches!(rx.changed().await, Err(RecvError(()))));
        });
    }
}
