//! [`KeyHolds`]: in-memory holds on object keys. A key is held while any
//! [`KeyHold`] on it stands, and a deleter that reads a set's keys after
//! listing the store leaves every held key alone.
//!
//! Each set chooses, by its type, what dropping a hold does ([`DropPolicy`]).
//! A lease on a snapshot object ends when its owner drops it ([`Release`]).
//! An upload's hold may have to outlive the future that took it, so in such
//! a set only [`KeyHold::release`] ends a hold, and whatever stays held ends
//! with the set ([`Keep`]). Only a releasing hold can be cloned: a clone of a
//! kept hold would hold its key until the set ends.

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

/// What dropping a [`KeyHold`] does.
pub trait DropPolicy: Send + Sync + 'static {
    /// Whether dropping a hold releases it.
    const RELEASES_ON_DROP: bool;
}

/// Dropping a hold releases it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Release;

/// A hold stands until [`KeyHold::release`] or until its set is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Keep;

impl DropPolicy for Release {
    const RELEASES_ON_DROP: bool = true;
}

impl DropPolicy for Keep {
    const RELEASES_ON_DROP: bool = false;
}

type HeldKeys<V> = Arc<Mutex<BTreeMap<String, Held<V>>>>;

/// A set of held keys, each with the value of its first hold and the
/// number of holds that stand on it.
pub struct KeyHolds<V, P: DropPolicy> {
    held: HeldKeys<V>,
    policy: PhantomData<P>,
}

#[derive(Debug)]
struct Held<V> {
    value: V,
    holds: usize,
}

impl<V, P: DropPolicy> Default for KeyHolds<V, P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V, P: DropPolicy> KeyHolds<V, P> {
    /// An empty set.
    pub fn new() -> Self {
        Self {
            held: Arc::new(Mutex::new(BTreeMap::new())),
            policy: PhantomData,
        }
    }

    /// Holds `key`. The first hold of a key records `value`, and a further
    /// hold of a held key keeps the recorded one.
    pub fn hold(&self, key: impl Into<String>, value: V) -> KeyHold<V, P> {
        let key = key.into();
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = held.entry(key.clone()).or_insert(Held { value, holds: 0 });
        entry.holds = entry.holds.saturating_add(1);
        KeyHold {
            stand: Some(self.held.clone()),
            key,
            policy: PhantomData,
        }
    }

    /// Whether a hold on `key` stands.
    pub fn contains(&self, key: &str) -> bool {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(key)
    }

    /// The held keys, in key order.
    pub fn keys(&self) -> Vec<String> {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }

    /// The recorded value of every held key, in key order.
    pub fn values(&self) -> Vec<V>
    where V: Clone {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|held| held.value.clone())
            .collect()
    }

    /// How many keys are held.
    pub fn len(&self) -> usize {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether no key is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<V, P: DropPolicy> fmt::Debug for KeyHolds<V, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyHolds")
            .field("keys", &self.keys())
            .field("releases_on_drop", &P::RELEASES_ON_DROP)
            .finish()
    }
}

/// One hold on a key of a [`KeyHolds`].
#[must_use = "a hold ends when it is dropped or released"]
pub struct KeyHold<V, P: DropPolicy> {
    /// The set, while the hold stands.
    stand: Option<HeldKeys<V>>,
    key: String,
    policy: PhantomData<P>,
}

fn release_one<V>(held: &HeldKeys<V>, key: &str) {
    let mut held = held.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(entry) = held.get_mut(key) {
        entry.holds = entry.holds.saturating_sub(1);
        if entry.holds == 0 {
            held.remove(key);
        }
    }
}

impl<V, P: DropPolicy> KeyHold<V, P> {
    /// The held key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Ends this hold, whatever the set's policy.
    pub fn release(mut self) {
        if let Some(held) = self.stand.take() {
            release_one(&held, &self.key);
        }
    }
}

/// A clone is another hold on the same key, which its drop releases.
impl<V> Clone for KeyHold<V, Release> {
    fn clone(&self) -> Self {
        if let Some(held) = &self.stand
            && let Some(entry) = held
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_mut(&self.key)
        {
            entry.holds = entry.holds.saturating_add(1);
        }
        Self {
            stand: self.stand.clone(),
            key: self.key.clone(),
            policy: PhantomData,
        }
    }
}

impl<V, P: DropPolicy> Drop for KeyHold<V, P> {
    fn drop(&mut self) {
        if P::RELEASES_ON_DROP
            && let Some(held) = self.stand.take()
        {
            release_one(&held, &self.key);
        }
    }
}

impl<V, P: DropPolicy> fmt::Debug for KeyHold<V, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyHold")
            .field("key", &self.key)
            .field("releases_on_drop", &P::RELEASES_ON_DROP)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Keep;
    use super::KeyHolds;
    use super::Release;

    /// A releasing set ends a hold when it drops, and a key stays held
    /// while any of its holds stands.
    #[test]
    fn a_releasing_set_ends_a_hold_on_drop() {
        let holds = KeyHolds::<_, Release>::new();
        let first = holds.hold("a", 1);
        let second = holds.hold("a", 2);
        let clone = first.clone();
        assert_eq!(holds.values(), vec![1]);
        assert_eq!(clone.key(), "a");
        drop(first);
        drop(second);
        assert!(holds.contains("a"));
        drop(clone);
        assert!(!holds.contains("a"));
        assert!(holds.is_empty());
    }

    /// A keeping set ends a hold only on release: a dropped hold stays with
    /// the set.
    #[test]
    fn a_keeping_set_ends_a_hold_only_on_release() {
        let holds = KeyHolds::<(), Keep>::new();
        let released = holds.hold("released", ());
        drop(holds.hold("dropped", ()));
        assert_eq!(holds.keys(), vec![
            "dropped".to_owned(),
            "released".to_owned()
        ]);
        released.release();
        assert_eq!(holds.keys(), vec!["dropped".to_owned()]);
        assert_eq!(holds.len(), 1);
    }
}
