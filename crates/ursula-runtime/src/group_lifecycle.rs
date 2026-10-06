//! Process-local close/drain barriers for detached per-group work.
//! Durable assignment/generation authorization remains with the receiver.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use crate::rt::sync::watch;

#[derive(Debug, Default)]
struct State {
    closed: bool,
    active: usize,
}

#[derive(Debug)]
/// A process-local, permanently closeable lifecycle for detached replica work.
/// It supplements durable receiver authorization; it does not grant hosting.
pub struct GroupActivity {
    state: Mutex<State>,
    changes: watch::Sender<usize>,
}

impl Default for GroupActivity {
    fn default() -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            state: Mutex::default(),
            changes,
        }
    }
}

#[derive(Debug)]
/// Keeps admitted replica work in the retirement drain until dropped.
pub struct GroupActivityGuard {
    activity: Arc<GroupActivity>,
}

impl GroupActivity {
    pub fn enter(self: &Arc<Self>) -> Result<GroupActivityGuard, String> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return Err("group lifecycle is retired".to_owned());
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or("group activity counter exhausted")?;
        self.changes.send_replace(state.active);
        Ok(GroupActivityGuard {
            activity: self.clone(),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed
    }

    pub fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed = true;
    }

    /// Wait after `close`; callers cannot reopen this lifecycle.
    pub async fn drain(&self) {
        let mut changes = self.changes.subscribe();
        loop {
            if *changes.borrow_and_update() == 0 {
                return;
            }
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for GroupActivityGuard {
    fn drop(&mut self) {
        let mut state = self
            .activity
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
        self.activity.changes.send_replace(state.active);
    }
}
