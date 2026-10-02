//! Reference model of `state(D)` (design §4.1) and of keyed-state range
//! reads (P3 normative 2, 3 and 5).
//!
//! This is the executable definition the keyed engine, the TypeScript owner
//! overlay and the vectors are checked against (I27). It favours clarity over
//! speed: a `BTreeMap` of visible rows, ordered by unsigned octet order.

use std::collections::BTreeMap;
use std::ops::Bound;

use serde_json::value::RawValue;

use super::batch::InvalidMessage;
use super::batch::KeyedBatch;
use super::batch::KeyedBatchError;
use super::batch::KeyedOp;
use super::batch::encode_key;
use super::batch::parse_batch;

/// Response budget of one keyed-state page: 4 MiB of uncompressed body.
pub const KEYED_STATE_RESPONSE_BUDGET: usize = 4 * 1024 * 1024;

/// One visible key's row.
#[derive(Debug, Clone)]
pub struct Row {
    /// Ordinal of the record containing the last put applied to the key.
    pub record: u64,
    /// The value's stored text.
    pub value: Box<RawValue>,
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.record == other.record && self.value.get() == other.value.get()
    }
}

impl Eq for Row {}

/// `state(D)`: the fold of records `0 .. D`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyedState {
    rows: BTreeMap<Vec<u8>, Row>,
    through: u64,
}

/// Lower bound of a range read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Lower {
    /// From the first key (the default).
    #[default]
    First,
    /// `start=k`: inclusive.
    Start(Vec<u8>),
    /// `after=k`: exclusive.
    After(Vec<u8>),
}

/// A range read: `[lo, hi)`, at most `limit` rows, optionally bounded by a
/// response budget in body bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeQuery {
    /// Lower bound.
    pub lower: Lower,
    /// Exclusive upper bound; `None` means past the last key.
    pub end: Option<Vec<u8>>,
    /// Maximum number of rows.
    pub limit: usize,
    /// Maximum body bytes (rendered lines including their LF); the first row
    /// is always returned whole. `None` means unbounded.
    pub budget: Option<usize>,
}

impl Default for RangeQuery {
    fn default() -> Self {
        Self {
            lower: Lower::First,
            end: None,
            limit: 100,
            budget: Some(KEYED_STATE_RESPONSE_BUDGET),
        }
    }
}

/// One page of a range read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePage<'s> {
    /// Rows in ascending unsigned octet order of key.
    pub rows: Vec<(&'s [u8], &'s Row)>,
    /// The last returned key, present exactly when the read stopped because
    /// of `limit` or the budget while rows remained in the range
    /// (`Stream-Keyed-After`).
    pub after: Option<Vec<u8>>,
}

impl RangePage<'_> {
    /// Renders the page as the `application/vnd.durable-stream-keyed-rows+ndjson` body.
    pub fn body(&self) -> String {
        let mut out = String::new();
        for (key, row) in &self.rows {
            out.push_str(&row_line(key, row));
        }
        out
    }
}

/// Renders one row as `{"key":"<k>","record":<r>,"value":<v>}` plus LF.
pub fn row_line(key: &[u8], row: &Row) -> String {
    format!(
        "{{\"key\":\"{}\",\"record\":{},\"value\":{}}}\n",
        encode_key(key),
        row.record,
        row.value.get()
    )
}

impl KeyedState {
    /// `state(0)`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds messages `0 .. n` into `state(n)`, validating each.
    pub fn fold<'a, I>(messages: I) -> Result<Self, InvalidMessage>
    where I: IntoIterator<Item = &'a str> {
        let mut state = Self::new();
        for (index, message) in messages.into_iter().enumerate() {
            state
                .apply_message(message)
                .map_err(|error| InvalidMessage { index, error })?;
        }
        Ok(state)
    }

    /// `D`: the number of records applied.
    pub fn through(&self) -> u64 {
        self.through
    }

    /// Visible rows in ascending unsigned octet order.
    pub fn rows(&self) -> &BTreeMap<Vec<u8>, Row> {
        &self.rows
    }

    /// Point read.
    pub fn get(&self, key: &[u8]) -> Option<&Row> {
        self.rows.get(key)
    }

    /// Validates and applies the next record.
    pub fn apply_message(&mut self, text: &str) -> Result<(), KeyedBatchError> {
        let batch = parse_batch(text)?;
        self.apply(&batch);
        Ok(())
    }

    /// Applies an already parsed batch as record `D`, then advances `D`.
    pub fn apply(&mut self, batch: &KeyedBatch<'_>) {
        let record = self.through;
        for op in &batch.ops {
            match op {
                KeyedOp::Put { key, value } => {
                    self.rows.insert(key.clone(), Row {
                        record,
                        value: (*value).to_owned(),
                    });
                }
                KeyedOp::Delete { key } => {
                    self.rows.remove(key.as_slice());
                }
                KeyedOp::DeleteRange { start, end } => {
                    let mut middle = self.rows.split_off(start.as_slice());
                    let mut tail = middle.split_off(end.as_slice());
                    self.rows.append(&mut tail);
                }
            }
        }
        self.through = self.through.saturating_add(1);
    }

    /// Range read per P3 normative 2, 3 and 5.
    pub fn range(&self, query: &RangeQuery) -> RangePage<'_> {
        let empty = RangePage {
            rows: Vec::new(),
            after: None,
        };
        let lower = match &query.lower {
            Lower::First => Bound::Unbounded,
            Lower::Start(key) => Bound::Included(key.as_slice()),
            Lower::After(key) => Bound::Excluded(key.as_slice()),
        };
        let upper = match &query.end {
            Some(end) => {
                // `lo >= hi` selects nothing (and would panic in `range`).
                let below = match &query.lower {
                    Lower::First => true,
                    Lower::Start(key) | Lower::After(key) => key < end,
                };
                if !below {
                    return empty;
                }
                Bound::Excluded(end.as_slice())
            }
            None => Bound::Unbounded,
        };
        let mut rows = Vec::new();
        let mut used = 0_usize;
        let mut stopped = false;
        let mut iter = self
            .rows
            .range::<[u8], _>((lower, upper))
            .map(|(key, row)| (key.as_slice(), row))
            .peekable();
        while let Some(&(key, row)) = iter.peek() {
            if rows.len() >= query.limit {
                stopped = true;
                break;
            }
            if let Some(budget) = query.budget {
                let size = row_line(key, row).len();
                let next = used.saturating_add(size);
                if !rows.is_empty() && next > budget {
                    stopped = true;
                    break;
                }
                used = next;
            }
            rows.push((key, row));
            iter.next();
        }
        let after = if stopped {
            rows.last().map(|(key, _row)| key.to_vec())
        } else {
            None
        };
        RangePage { rows, after }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_delete_range() {
        let state = KeyedState::fold([
            r#"{"ops":[["p","AQ",1],["p","Ag",2],["p","Aw",3]]}"#,
            r#"{"ops":[["x","Ag","BA"],["p","Aw",null]]}"#,
        ])
        .unwrap();
        assert_eq!(state.through(), 2);
        let keys: Vec<_> = state.rows().keys().cloned().collect();
        assert_eq!(keys, vec![vec![1], vec![3]]);
        assert_eq!(state.get(&[3]).unwrap().record, 1);
        assert_eq!(state.get(&[3]).unwrap().value.get(), "null");
    }

    #[test]
    fn lo_not_below_hi_is_empty() {
        let state = KeyedState::fold([r#"{"ops":[["p","AQ",1]]}"#]).unwrap();
        let page = state.range(&RangeQuery {
            lower: Lower::After(vec![1]),
            end: Some(vec![1]),
            ..RangeQuery::default()
        });
        assert!(page.rows.is_empty());
        assert_eq!(page.after, None);
    }

    #[test]
    fn budget_returns_first_row_whole() {
        let state = KeyedState::fold([r#"{"ops":[["p","AQ","xxxxxxxx"],["p","Ag",2]]}"#]).unwrap();
        let page = state.range(&RangeQuery {
            budget: Some(1),
            ..RangeQuery::default()
        });
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.after, Some(vec![1]));
        assert_eq!(
            page.body(),
            "{\"key\":\"AQ\",\"record\":0,\"value\":\"xxxxxxxx\"}\n"
        );
    }
}
