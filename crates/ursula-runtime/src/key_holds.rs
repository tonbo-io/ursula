//! [`KeyHolds`]: in-memory holds on object keys. A key is held while any
//! [`KeyHold`] on it stands, and a deleter that reads a set's keys after
//! listing the store leaves every held key alone.
//!
//! Each set chooses what dropping a hold does ([`HoldDrop`]). A lease on a
//! snapshot object ends when its owner drops it. An upload's hold may have
//! to outlive the future that took it, so in such a set only
//! [`KeyHold::release`] ends a hold, and whatever stays held ends with the
//! set.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

/// What dropping a [`KeyHold`] does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldDrop {
    /// Dropping the hold releases it.
    Release,
    /// The hold stands until [`KeyHold::release`] or until the set is
    /// dropped. A hold dropped without a release stays with the set.
    Keep,
}

/// A set of held keys, each with the value of its first hold and the
/// number of holds that stand on it.
pub struct KeyHolds<V> {
    held: Arc<Mutex<BTreeMap<String, Held<V>>>>,
    on_drop: HoldDrop,
}

#[derive(Debug)]
struct Held<V> {
    value: V,
    holds: usize,
}

impl<V> KeyHolds<V> {
    /// An empty set whose holds behave as `on_drop` says when dropped.
    pub fn new(on_drop: HoldDrop) -> Self {
        Self {
            held: Arc::new(Mutex::new(BTreeMap::new())),
            on_drop,
        }
    }

    /// Holds `key`. The first hold of a key records `value`, and a further
    /// hold of a held key keeps the recorded one.
    pub fn hold(&self, key: impl Into<String>, value: V) -> KeyHold<V> {
        let key = key.into();
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = held.entry(key.clone()).or_insert(Held { value, holds: 0 });
        entry.holds = entry.holds.saturating_add(1);
        KeyHold {
            stand: Some(Stand {
                held: self.held.clone(),
                key,
            }),
            on_drop: self.on_drop,
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

impl<V> fmt::Debug for KeyHolds<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyHolds")
            .field("keys", &self.keys())
            .field("on_drop", &self.on_drop)
            .finish()
    }
}

/// One hold on a key of a [`KeyHolds`]. A clone is another hold on the same
/// key.
pub struct KeyHold<V> {
    /// `None` once the hold was released.
    stand: Option<Stand<V>>,
    on_drop: HoldDrop,
}

struct Stand<V> {
    held: Arc<Mutex<BTreeMap<String, Held<V>>>>,
    key: String,
}

impl<V> Stand<V> {
    fn release(self) {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = held.get_mut(&self.key) {
            entry.holds = entry.holds.saturating_sub(1);
            if entry.holds == 0 {
                held.remove(&self.key);
            }
        }
    }
}

impl<V> KeyHold<V> {
    /// The held key.
    pub fn key(&self) -> Option<&str> {
        self.stand.as_ref().map(|stand| stand.key.as_str())
    }

    /// Ends this hold, whatever the set's [`HoldDrop`].
    pub fn release(mut self) {
        if let Some(stand) = self.stand.take() {
            stand.release();
        }
    }
}

impl<V> Clone for KeyHold<V> {
    fn clone(&self) -> Self {
        let stand = self.stand.as_ref().map(|stand| {
            let mut held = stand.held.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(entry) = held.get_mut(&stand.key) {
                entry.holds = entry.holds.saturating_add(1);
            }
            Stand {
                held: stand.held.clone(),
                key: stand.key.clone(),
            }
        });
        Self {
            stand,
            on_drop: self.on_drop,
        }
    }
}

impl<V> Drop for KeyHold<V> {
    fn drop(&mut self) {
        if self.on_drop == HoldDrop::Release
            && let Some(stand) = self.stand.take()
        {
            stand.release();
        }
    }
}

impl<V> fmt::Debug for KeyHold<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyHold")
            .field("key", &self.key())
            .field("on_drop", &self.on_drop)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::HoldDrop;
    use super::KeyHolds;

    /// A releasing set ends a hold when it drops, and a key stays held
    /// while any of its holds stands.
    #[test]
    fn a_releasing_set_ends_a_hold_on_drop() {
        let holds = KeyHolds::new(HoldDrop::Release);
        let first = holds.hold("a", 1);
        let second = holds.hold("a", 2);
        let clone = first.clone();
        assert_eq!(holds.values(), vec![1]);
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
        let holds = KeyHolds::new(HoldDrop::Keep);
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
