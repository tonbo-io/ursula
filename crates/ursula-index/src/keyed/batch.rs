//! `keyed-batch-v1` validator and zero-copy parser (design §4.1, §5.2).
//!
//! The input is one message as stored under JSON Message Text (P1): a
//! complete JSON text. The parser borrows values from it as [`RawValue`]s, so
//! a put's value is exactly the stored text. Keys are decoded from their
//! canonical unpadded base64url spelling.
//!
//! Classification: when a message is rejected, the error reason names the
//! first grammar violation met in document order. A message that is not JSON
//! text at all is always [`KeyedBatchErrorReason::InvalidJson`], even when a
//! grammar violation precedes the syntax error, which keeps the HTTP
//! precedence 400 (JSON) before 422 (grammar) for callers that use this
//! module alone.
//!
//! Nesting depth is not checked here: P1 rejects messages deeper than 127
//! before they reach this parser.

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserializer as _;
use serde::de;
use serde::de::DeserializeSeed;
use serde::de::MapAccess;
use serde::de::SeqAccess;
use serde::de::Visitor;
use serde_json::value::RawValue;

/// Profile token and content-type `profile` parameter value of the format.
pub const KEYED_BATCH_PROFILE: &str = "keyed-batch-v1";

/// Largest key, in octets.
pub const MAX_KEY_OCTETS: usize = 4096;

/// Longest canonical unpadded base64url spelling of a key of
/// [`MAX_KEY_OCTETS`] octets: `ceil(4096 * 4 / 3)`.
pub const MAX_KEY_CHARS: usize = 5462;

/// One parsed `keyed-batch-v1` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedBatch<'a> {
    /// The ops of the `ops` member, in array order.
    pub ops: Vec<KeyedOp<'a>>,
}

/// One op of a keyed batch.
#[derive(Debug, Clone)]
pub enum KeyedOp<'a> {
    /// `["p", key, value]`: sets `key` to `value`.
    Put {
        /// Decoded key octets.
        key: Vec<u8>,
        /// The value's text exactly as stored.
        value: &'a RawValue,
    },
    /// `["d", key]`: removes `key`.
    Delete {
        /// Decoded key octets.
        key: Vec<u8>,
    },
    /// `["x", start, end]`: removes every key in `[start, end)`.
    DeleteRange {
        /// Inclusive lower bound.
        start: Vec<u8>,
        /// Exclusive upper bound; strictly greater than `start`.
        end: Vec<u8>,
    },
}

impl PartialEq for KeyedOp<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Put { key: a, value: x }, Self::Put { key: b, value: y }) => {
                a == b && x.get() == y.get()
            }
            (Self::Delete { key: a }, Self::Delete { key: b }) => a == b,
            (Self::DeleteRange { start: a, end: b }, Self::DeleteRange { start: c, end: d }) => {
                a == c && b == d
            }
            _ => false,
        }
    }
}

impl Eq for KeyedOp<'_> {}

/// Why a message is not a valid `keyed-batch-v1` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyedBatchErrorReason {
    /// Not RFC 8259 JSON text (HTTP 400 under P1, not 422).
    InvalidJson,
    /// The message is JSON but not an object.
    NotObject,
    /// One of the message's own member names contains an unpaired surrogate
    /// escape.
    InvalidMemberName,
    /// No member is named `ops`.
    MissingOps,
    /// More than one member is named `ops` (after unescaping).
    DuplicateOps,
    /// The `ops` member is not an array.
    OpsNotArray,
    /// An op is not an array, is empty, or has the wrong number of elements.
    InvalidOp,
    /// An op's first element is not one of the strings `p`, `d`, `x`.
    UnknownOpCode,
    /// A key is not a canonical unpadded base64url string of 1..=4096 octets.
    InvalidKey,
    /// A range delete whose start is not below its end.
    EmptyRange,
}

impl KeyedBatchErrorReason {
    /// Stable snake_case name, used by the vectors.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid_json",
            Self::NotObject => "not_object",
            Self::InvalidMemberName => "invalid_member_name",
            Self::MissingOps => "missing_ops",
            Self::DuplicateOps => "duplicate_ops",
            Self::OpsNotArray => "ops_not_array",
            Self::InvalidOp => "invalid_op",
            Self::UnknownOpCode => "unknown_op_code",
            Self::InvalidKey => "invalid_key",
            Self::EmptyRange => "empty_range",
        }
    }

    /// Parses [`Self::as_str`] output.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "invalid_json" => Self::InvalidJson,
            "not_object" => Self::NotObject,
            "invalid_member_name" => Self::InvalidMemberName,
            "missing_ops" => Self::MissingOps,
            "duplicate_ops" => Self::DuplicateOps,
            "ops_not_array" => Self::OpsNotArray,
            "invalid_op" => Self::InvalidOp,
            "unknown_op_code" => Self::UnknownOpCode,
            "invalid_key" => Self::InvalidKey,
            "empty_range" => Self::EmptyRange,
            _ => return None,
        })
    }

    /// Whether this is a JSON syntax error rather than a grammar error.
    pub fn is_json_syntax(self) -> bool {
        matches!(self, Self::InvalidJson)
    }
}

impl fmt::Display for KeyedBatchErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A rejected message: a reason class plus human-readable text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{detail}")]
pub struct KeyedBatchError {
    reason: KeyedBatchErrorReason,
    detail: String,
}

impl KeyedBatchError {
    fn new(reason: KeyedBatchErrorReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }

    /// The reason class.
    pub fn reason(&self) -> KeyedBatchErrorReason {
        self.reason
    }

    /// The human-readable text, e.g. `op 2: key is not canonical base64url`.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// The first invalid message of a sequence.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid keyed batch at message {index}: {error}")]
pub struct InvalidMessage {
    /// Zero-based index of the message after P1 flattening.
    pub index: usize,
    /// Why it is invalid.
    pub error: KeyedBatchError,
}

/// Why a key spelling is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// The spelling is empty (a key has at least one octet).
    #[error("key is empty")]
    Empty,
    /// The key exceeds [`MAX_KEY_OCTETS`].
    #[error("key exceeds 4096 octets")]
    TooLong,
    /// The spelling is not canonical unpadded base64url.
    #[error("key is not canonical unpadded base64url")]
    NonCanonical,
}

/// Encodes key octets as canonical unpadded base64url.
pub fn encode_key(key: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(key)
}

/// Decodes a key from its canonical unpadded base64url characters (no
/// quotes, no escapes). Every accepted spelling is the unique canonical
/// encoding of its octets, and the octets have length 1..=4096.
pub fn decode_key(text: &str) -> Result<Vec<u8>, KeyError> {
    if text.is_empty() {
        return Err(KeyError::Empty);
    }
    if text.len() > MAX_KEY_CHARS {
        return Err(KeyError::TooLong);
    }
    if !text
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(KeyError::NonCanonical);
    }
    // The engine rejects padding, lengths of 1 mod 4 and non-zero pad bits,
    // which together with the alphabet check makes the spelling canonical.
    let key = URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_error| KeyError::NonCanonical)?;
    if key.is_empty() {
        return Err(KeyError::Empty);
    }
    if key.len() > MAX_KEY_OCTETS {
        return Err(KeyError::TooLong);
    }
    Ok(key)
}

/// Parses and validates one message.
pub fn parse_batch(text: &str) -> Result<KeyedBatch<'_>, KeyedBatchError> {
    let mut ctx = Ctx {
        position: Position::Top,
        recorded: None,
    };
    let parsed = parse_inner(text, &mut ctx);
    match parsed {
        Ok(batch) => Ok(batch),
        Err(error) => {
            // A grammar violation may precede a syntax error later in the
            // text; syntax errors take precedence.
            if serde_json::from_str::<&RawValue>(text).is_err() {
                return Err(KeyedBatchError::new(
                    KeyedBatchErrorReason::InvalidJson,
                    format!("invalid JSON: {error}"),
                ));
            }
            Err(ctx.recorded.unwrap_or_else(|| ctx.position.error()))
        }
    }
}

/// Validates one message without keeping the parse.
pub fn validate_batch(text: &str) -> Result<(), KeyedBatchError> {
    parse_batch(text).map(|_batch| ())
}

/// Validates every message of a request; returns the first invalid one.
pub fn validate_messages<'a, I>(messages: I) -> Result<(), InvalidMessage>
where I: IntoIterator<Item = &'a str> {
    for (index, message) in messages.into_iter().enumerate() {
        validate_batch(message).map_err(|error| InvalidMessage { index, error })?;
    }
    Ok(())
}

fn parse_inner<'a>(text: &'a str, ctx: &mut Ctx) -> Result<KeyedBatch<'a>, serde_json::Error> {
    let mut de = serde_json::Deserializer::from_str(text);
    let first = text
        .bytes()
        .find(|b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'));
    if first != Some(b'{') {
        return Err(ctx.fail(KeyedBatchError::new(
            KeyedBatchErrorReason::NotObject,
            "message is not a JSON object",
        )));
    }
    let batch = de.deserialize_map(BatchVisitor { ctx: &mut *ctx })?;
    de.end()?;
    Ok(batch)
}

/// Where the parser is; names the error when serde rejects a value whose
/// type does not fit the grammar.
#[derive(Clone, Copy)]
enum Position {
    Top,
    OpsValue,
    Op(usize),
}

impl Position {
    fn error(self) -> KeyedBatchError {
        match self {
            Self::Top => KeyedBatchError::new(
                KeyedBatchErrorReason::NotObject,
                "message is not a JSON object",
            ),
            Self::OpsValue => KeyedBatchError::new(
                KeyedBatchErrorReason::OpsNotArray,
                "\"ops\" is not an array",
            ),
            Self::Op(index) => KeyedBatchError::new(
                KeyedBatchErrorReason::InvalidOp,
                format!("op {index}: not an array"),
            ),
        }
    }
}

struct Ctx {
    position: Position,
    recorded: Option<KeyedBatchError>,
}

impl Ctx {
    fn fail<E: de::Error>(&mut self, error: KeyedBatchError) -> E {
        let message = E::custom(error.detail());
        self.recorded = Some(error);
        message
    }
}

struct BatchVisitor<'c> {
    ctx: &'c mut Ctx,
}

impl<'de> Visitor<'de> for BatchVisitor<'_> {
    type Value = KeyedBatch<'de>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a keyed-batch-v1 object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where A: MapAccess<'de> {
        let mut ops = None;
        while let Some(name) = map.next_key_seed(NameSeed)? {
            match name {
                Name::Invalid => {
                    return Err(self.ctx.fail(KeyedBatchError::new(
                        KeyedBatchErrorReason::InvalidMemberName,
                        "member name contains an unpaired surrogate escape",
                    )));
                }
                Name::Ops => {
                    if ops.is_some() {
                        return Err(self.ctx.fail(KeyedBatchError::new(
                            KeyedBatchErrorReason::DuplicateOps,
                            "duplicate \"ops\" member",
                        )));
                    }
                    self.ctx.position = Position::OpsValue;
                    ops = Some(map.next_value_seed(OpsSeed {
                        ctx: &mut *self.ctx,
                    })?);
                    self.ctx.position = Position::Top;
                }
                Name::Other => {
                    let _value: &'de RawValue = map.next_value()?;
                }
            }
        }
        match ops {
            Some(ops) => Ok(KeyedBatch { ops }),
            None => Err(self.ctx.fail(KeyedBatchError::new(
                KeyedBatchErrorReason::MissingOps,
                "missing \"ops\" member",
            ))),
        }
    }
}

enum Name {
    Ops,
    Other,
    Invalid,
}

struct NameSeed;

impl<'de> DeserializeSeed<'de> for NameSeed {
    type Value = Name;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: de::Deserializer<'de> {
        // `deserialize_bytes` yields the unescaped name without validating
        // surrogates; an unpaired surrogate comes back as non-UTF-8 bytes.
        deserializer.deserialize_bytes(NameVisitor)
    }
}

struct NameVisitor;

impl<'de> Visitor<'de> for NameVisitor {
    type Value = Name;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a member name")
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        Ok(if v == b"ops" {
            Name::Ops
        } else if std::str::from_utf8(v).is_ok() {
            Name::Other
        } else {
            Name::Invalid
        })
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        self.visit_bytes(v.as_bytes())
    }
}

struct OpsSeed<'c> {
    ctx: &'c mut Ctx,
}

impl<'de> DeserializeSeed<'de> for OpsSeed<'_> {
    type Value = Vec<KeyedOp<'de>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: de::Deserializer<'de> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for OpsSeed<'_> {
    type Value = Vec<KeyedOp<'de>>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an array of ops")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where A: SeqAccess<'de> {
        let mut ops = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        let mut index = 0_usize;
        loop {
            self.ctx.position = Position::Op(index);
            let op = seq.next_element_seed(OpSeed {
                ctx: &mut *self.ctx,
                index,
            })?;
            match op {
                Some(op) => ops.push(op),
                None => break,
            }
            index = index.saturating_add(1);
        }
        self.ctx.position = Position::OpsValue;
        Ok(ops)
    }
}

struct OpSeed<'c> {
    ctx: &'c mut Ctx,
    index: usize,
}

impl<'de> DeserializeSeed<'de> for OpSeed<'_> {
    type Value = KeyedOp<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: de::Deserializer<'de> {
        deserializer.deserialize_seq(self)
    }
}

#[derive(Clone, Copy)]
enum OpCode {
    Put,
    Delete,
    DeleteRange,
}

fn op_code(raw: &RawValue) -> Option<OpCode> {
    let text = raw.get();
    let code = match text {
        "\"p\"" => return Some(OpCode::Put),
        "\"d\"" => return Some(OpCode::Delete),
        "\"x\"" => return Some(OpCode::DeleteRange),
        _ if !text.starts_with('"') || !text.contains('\\') => return None,
        // An escaped spelling; compare after unescaping. A string with an
        // unpaired surrogate fails here and is simply not an op code.
        _ => serde_json::from_str::<String>(text).ok()?,
    };
    match code.as_str() {
        "p" => Some(OpCode::Put),
        "d" => Some(OpCode::Delete),
        "x" => Some(OpCode::DeleteRange),
        _ => None,
    }
}

impl OpSeed<'_> {
    fn arity<E: de::Error>(&mut self, expected: usize) -> E {
        self.ctx.fail(KeyedBatchError::new(
            KeyedBatchErrorReason::InvalidOp,
            format!("op {}: expected {expected} elements", self.index),
        ))
    }

    fn key<'de, A>(&mut self, seq: &mut A, expected: usize) -> Result<Vec<u8>, A::Error>
    where A: SeqAccess<'de> {
        let Some(raw) = seq.next_element::<&'de RawValue>()? else {
            return Err(self.arity(expected));
        };
        let text = raw.get();
        let inner = text
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'));
        let Some(inner) = inner else {
            return Err(self.ctx.fail(KeyedBatchError::new(
                KeyedBatchErrorReason::InvalidKey,
                format!("op {}: key is not a string", self.index),
            )));
        };
        decode_key(inner).map_err(|error| {
            self.ctx.fail(KeyedBatchError::new(
                KeyedBatchErrorReason::InvalidKey,
                format!("op {}: {error}", self.index),
            ))
        })
    }
}

impl<'de> Visitor<'de> for OpSeed<'_> {
    type Value = KeyedOp<'de>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an op array")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, A::Error>
    where A: SeqAccess<'de> {
        let Some(code) = seq.next_element::<&'de RawValue>()? else {
            return Err(self.ctx.fail(KeyedBatchError::new(
                KeyedBatchErrorReason::InvalidOp,
                format!("op {}: empty op", self.index),
            )));
        };
        let Some(code) = op_code(code) else {
            return Err(self.ctx.fail(KeyedBatchError::new(
                KeyedBatchErrorReason::UnknownOpCode,
                format!("op {}: unknown op code", self.index),
            )));
        };
        let (op, expected) = match code {
            OpCode::Put => {
                let key = self.key(&mut seq, 3)?;
                let Some(value) = seq.next_element::<&'de RawValue>()? else {
                    return Err(self.arity(3));
                };
                (KeyedOp::Put { key, value }, 3)
            }
            OpCode::Delete => (
                KeyedOp::Delete {
                    key: self.key(&mut seq, 2)?,
                },
                2,
            ),
            OpCode::DeleteRange => {
                let start = self.key(&mut seq, 3)?;
                let end = self.key(&mut seq, 3)?;
                if start >= end {
                    return Err(self.ctx.fail(KeyedBatchError::new(
                        KeyedBatchErrorReason::EmptyRange,
                        format!("op {}: range start is not below its end", self.index),
                    )));
                }
                (KeyedOp::DeleteRange { start, end }, 3)
            }
        };
        if seq.next_element::<&'de RawValue>()?.is_some() {
            return Err(self.arity(expected));
        }
        Ok(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reason(text: &str) -> KeyedBatchErrorReason {
        parse_batch(text).unwrap_err().reason()
    }

    #[test]
    fn parses_all_op_shapes() {
        let batch =
            parse_batch(r#"{"m":1,"ops":[["p","AQ",{"a":1.50e3}],["d","Ag"],["x","AA","AQ"]]}"#)
                .unwrap();
        assert_eq!(batch.ops.len(), 3);
        match &batch.ops[0] {
            KeyedOp::Put { key, value } => {
                assert_eq!(key, &[1]);
                assert_eq!(value.get(), r#"{"a":1.50e3}"#);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn classifies_errors() {
        assert_eq!(reason("[]"), KeyedBatchErrorReason::NotObject);
        assert_eq!(reason("{}"), KeyedBatchErrorReason::MissingOps);
        assert_eq!(
            reason(r#"{"ops":[],"ops":[]}"#),
            KeyedBatchErrorReason::DuplicateOps
        );
        assert_eq!(
            reason(r#"{"\ud800":1,"ops":[]}"#),
            KeyedBatchErrorReason::InvalidMemberName
        );
        assert_eq!(reason(r#"{"ops":{}}"#), KeyedBatchErrorReason::OpsNotArray);
        assert_eq!(reason(r#"{"ops":[1]}"#), KeyedBatchErrorReason::InvalidOp);
        assert_eq!(
            reason(r#"{"ops":[1e400]}"#),
            KeyedBatchErrorReason::InvalidOp
        );
        assert_eq!(
            reason(r#"{"ops":[["q","AA"]]}"#),
            KeyedBatchErrorReason::UnknownOpCode
        );
        assert_eq!(
            reason(r#"{"ops":[["d","AB"]]}"#),
            KeyedBatchErrorReason::InvalidKey
        );
        assert_eq!(
            reason(r#"{"ops":[["x","AQ","AQ"]]}"#),
            KeyedBatchErrorReason::EmptyRange
        );
        assert_eq!(
            reason(r#"{"ops":[["d","AB"]],"#),
            KeyedBatchErrorReason::InvalidJson
        );
        assert_eq!(
            reason(r#"{"ops":[]} x"#),
            KeyedBatchErrorReason::InvalidJson
        );
        assert_eq!(reason("[1,"), KeyedBatchErrorReason::InvalidJson);
    }

    #[test]
    fn sequence_reports_first_invalid_index() {
        let error = validate_messages([r#"{"ops":[]}"#, r#"{"ops":[]}"#, "{}", "["]).unwrap_err();
        assert_eq!(error.index, 2);
        assert_eq!(
            error.to_string(),
            "invalid keyed batch at message 2: missing \"ops\" member"
        );
    }

    #[test]
    fn key_limits() {
        assert_eq!(
            decode_key(&encode_key(&[7; MAX_KEY_OCTETS])).unwrap().len(),
            MAX_KEY_OCTETS
        );
        assert_eq!(
            decode_key(&encode_key(&[7; MAX_KEY_OCTETS + 1])),
            Err(KeyError::TooLong)
        );
        assert_eq!(decode_key(""), Err(KeyError::Empty));
        assert_eq!(decode_key("AA=="), Err(KeyError::NonCanonical));
        assert_eq!(decode_key("AB"), Err(KeyError::NonCanonical));
        assert_eq!(decode_key("+/"), Err(KeyError::NonCanonical));
        assert_eq!(decode_key("A"), Err(KeyError::NonCanonical));
    }
}
