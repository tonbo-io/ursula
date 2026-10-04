//! Event-time extraction from one source message.
//!
//! A message is walked lexically as [`RawValue`]s: only the members a pointer
//! names are decoded, so an escape of an unpaired surrogate (allowed by JSON
//! Message Text) elsewhere in the message cannot fail extraction.

use chrono::DateTime;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde_json::value::RawValue;

use crate::IndexError;
use crate::store::SkipKind;

/// The unit of a numeric time value.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    /// An RFC 3339 string, or an integer number of milliseconds.
    #[default]
    Auto,
    Rfc3339,
    S,
    Ms,
    Us,
    Ns,
}

/// Registration form of an extractor:
/// `{each?, time:[ptr…], end?:[ptr…], unit}`.
///
/// Pointers are RFC 6901 JSON pointers in which a `*` segment matches every
/// member of an object or element of an array. `each` selects the elements of
/// a message (default: the message itself). `time` and `end` are fallback
/// lists evaluated per element: the first pointer that yields a value wins.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractorConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub each: Option<String>,
    pub time: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub end: Vec<String>,
    #[serde(default)]
    pub unit: TimeUnit,
}

impl ExtractorConfig {
    /// The legacy `{"timestamp_field": "x"}` form: one top-level member, as
    /// an RFC 3339 string or integer milliseconds.
    pub fn timestamp_field(field: &str) -> Self {
        Self {
            each: None,
            time: vec![format!("/{}", field.replace('~', "~0").replace('/', "~1"))],
            end: Vec::new(),
            unit: TimeUnit::Auto,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Segment {
    Key(String),
    Wildcard,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Pointer(Vec<Segment>);

impl Pointer {
    fn parse(text: &str) -> Result<Self, IndexError> {
        if text.is_empty() {
            return Ok(Self(Vec::new()));
        }
        let Some(rest) = text.strip_prefix('/') else {
            return Err(IndexError::InvalidExtractor(format!(
                "pointer `{text}` must be empty or start with '/'"
            )));
        };
        let mut segments = Vec::new();
        for raw in rest.split('/') {
            if raw == "*" {
                segments.push(Segment::Wildcard);
                continue;
            }
            let mut key = String::with_capacity(raw.len());
            let mut chars = raw.chars();
            while let Some(character) = chars.next() {
                if character != '~' {
                    key.push(character);
                    continue;
                }
                match chars.next() {
                    Some('0') => key.push('~'),
                    Some('1') => key.push('/'),
                    _ => {
                        return Err(IndexError::InvalidExtractor(format!(
                            "pointer `{text}` has an invalid '~' escape"
                        )));
                    }
                }
            }
            segments.push(Segment::Key(key));
        }
        Ok(Self(segments))
    }

    fn select<'a>(&self, root: &'a RawValue) -> Vec<&'a RawValue> {
        let mut current = vec![root];
        for segment in &self.0 {
            let mut next = Vec::new();
            for value in current {
                children(value, segment, &mut next);
            }
            if next.is_empty() {
                return next;
            }
            current = next;
        }
        current
    }
}

/// Compiled extractor plus its digest, which names the index namespace.
#[derive(Clone, Debug)]
pub struct Extractor {
    config: ExtractorConfig,
    each: Pointer,
    time: Vec<Pointer>,
    end: Vec<Pointer>,
    digest: String,
}

/// The event time of one message, or why it has none.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Extraction {
    Event { t_ms: i64, t_end_ms: i64 },
    Skip(SkipKind),
}

impl Extractor {
    pub fn new(config: ExtractorConfig) -> Result<Self, IndexError> {
        if config.time.is_empty() {
            return Err(IndexError::InvalidExtractor(
                "`time` needs at least one pointer".to_owned(),
            ));
        }
        let each = Pointer::parse(config.each.as_deref().unwrap_or(""))?;
        let time = config
            .time
            .iter()
            .map(|pointer| Pointer::parse(pointer))
            .collect::<Result<Vec<_>, _>>()?;
        let end = config
            .end
            .iter()
            .map(|pointer| Pointer::parse(pointer))
            .collect::<Result<Vec<_>, _>>()?;
        let digest = blake3::hash(&serde_json::to_vec(&config)?)
            .to_hex()
            .to_string();
        Ok(Self {
            config,
            each,
            time,
            end,
            digest,
        })
    }

    pub fn timestamp_field(field: &str) -> Result<Self, IndexError> {
        if field.is_empty() {
            return Err(IndexError::InvalidExtractor(
                "timestamp field must not be empty".to_owned(),
            ));
        }
        Self::new(ExtractorConfig::timestamp_field(field))
    }

    pub fn config(&self) -> &ExtractorConfig {
        &self.config
    }

    /// BLAKE3 of the canonical configuration, in hex.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Extract one message. `message` may include its trailing LF (and CR).
    pub fn extract(&self, message: &[u8]) -> Extraction {
        let Ok(text) = std::str::from_utf8(message) else {
            return Extraction::Skip(SkipKind::Unparseable);
        };
        if text.trim().is_empty() {
            return Extraction::Skip(SkipKind::Blank);
        }
        let Ok(root) = serde_json::from_str::<&RawValue>(text) else {
            return Extraction::Skip(SkipKind::Unparseable);
        };
        let mut start: Option<i64> = None;
        let mut end: Option<i64> = None;
        let mut saw_invalid = false;
        for element in self.each.select(root) {
            let time = match first_value(&self.time, element, self.config.unit, false) {
                Value::Time(time) => time,
                Value::Missing => continue,
                Value::Invalid => {
                    saw_invalid = true;
                    continue;
                }
            };
            let element_end = match first_value(&self.end, element, self.config.unit, true) {
                Value::Time(value) => value.max(time),
                Value::Missing | Value::Invalid => time,
            };
            start = Some(start.map_or(time, |current| current.min(time)));
            end = Some(end.map_or(element_end, |current| current.max(element_end)));
        }
        match (start, end) {
            (Some(t_ms), Some(t_end_ms)) => Extraction::Event { t_ms, t_end_ms },
            _ if saw_invalid => Extraction::Skip(SkipKind::Invalid),
            _ => Extraction::Skip(SkipKind::Missing),
        }
    }
}

/// Whether `message` is one complete JSON value.
pub(crate) fn is_json_message(message: &[u8]) -> bool {
    std::str::from_utf8(message).is_ok_and(|text| serde_json::from_str::<&RawValue>(text).is_ok())
}

enum Value {
    Time(i64),
    Missing,
    Invalid,
}

/// Evaluate a fallback list on one element: the first pointer that selects a
/// present value decides. A pointer with a wildcard contributes the minimum
/// of its valid values, or the maximum when `latest` (for `end`).
fn first_value(pointers: &[Pointer], element: &RawValue, unit: TimeUnit, latest: bool) -> Value {
    for pointer in pointers {
        let mut best: Option<i64> = None;
        let mut invalid = false;
        for value in pointer.select(element) {
            match parse_time(value, unit) {
                Value::Time(time) => {
                    best = Some(best.map_or(time, |current| {
                        if latest {
                            current.max(time)
                        } else {
                            current.min(time)
                        }
                    }))
                }
                Value::Invalid => invalid = true,
                Value::Missing => {}
            }
        }
        if let Some(time) = best {
            return Value::Time(time);
        }
        if invalid {
            return Value::Invalid;
        }
    }
    Value::Missing
}

fn parse_time(raw: &RawValue, unit: TimeUnit) -> Value {
    let text = raw.get().trim();
    if text == "null" {
        return Value::Missing;
    }
    if text.starts_with('"') {
        let Ok(decoded) = serde_json::from_str::<String>(text) else {
            return Value::Invalid;
        };
        return parse_string_time(&decoded, unit);
    }
    if text.starts_with('-') || text.starts_with(|character: char| character.is_ascii_digit()) {
        return match unit {
            TimeUnit::Rfc3339 => Value::Invalid,
            TimeUnit::Auto => scale_integer(text, TimeUnit::Ms),
            TimeUnit::S => parse_seconds(text),
            TimeUnit::Ms | TimeUnit::Us | TimeUnit::Ns => scale_integer(text, unit),
        };
    }
    Value::Invalid
}

fn parse_string_time(text: &str, unit: TimeUnit) -> Value {
    if text.is_empty() {
        return Value::Missing;
    }
    match unit {
        TimeUnit::Rfc3339 => rfc3339(text),
        TimeUnit::Auto => match rfc3339(text) {
            Value::Time(time) => Value::Time(time),
            Value::Missing | Value::Invalid => scale_integer(text, TimeUnit::Ms),
        },
        TimeUnit::S => parse_seconds(text),
        TimeUnit::Ms | TimeUnit::Us | TimeUnit::Ns => scale_integer(text, unit),
    }
}

fn rfc3339(text: &str) -> Value {
    DateTime::parse_from_rfc3339(text).map_or(Value::Invalid, |time| {
        nonzero(i128::from(time.timestamp_millis()))
    })
}

/// A decimal integer (digits with an optional leading '-') in `unit`, which
/// is `Ms`, `Us` or `Ns` (seconds go through `parse_seconds`).
fn scale_integer(text: &str, unit: TimeUnit) -> Value {
    let Some(value) = parse_decimal_integer(text) else {
        return Value::Invalid;
    };
    let millis = match unit {
        TimeUnit::Us => value.checked_div_euclid(1_000),
        TimeUnit::Ns => value.checked_div_euclid(1_000_000),
        _ => Some(value),
    };
    millis.map_or(Value::Invalid, nonzero)
}

/// Seconds as a decimal integer or a plain decimal fraction; sub-millisecond
/// digits are truncated.
fn parse_seconds(text: &str) -> Value {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || (digits.contains('.') && fraction.is_empty())
    {
        return Value::Invalid;
    }
    let Ok(whole) = whole.parse::<i128>() else {
        return Value::Invalid;
    };
    let mut millis_text = fraction.chars().take(3).collect::<String>();
    while millis_text.len() < 3 {
        millis_text.push('0');
    }
    let Ok(fraction) = millis_text.parse::<i128>() else {
        return Value::Invalid;
    };
    let Some(magnitude) = whole
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(fraction))
    else {
        return Value::Invalid;
    };
    let millis = if negative {
        magnitude.checked_neg()
    } else {
        Some(magnitude)
    };
    millis.map_or(Value::Invalid, nonzero)
}

fn parse_decimal_integer(text: &str) -> Option<i128> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// 0 counts as missing (OTLP leaves unset times at 0); values outside `i64`
/// milliseconds are invalid.
fn nonzero(millis: i128) -> Value {
    if millis == 0 {
        return Value::Missing;
    }
    i64::try_from(millis).map_or(Value::Invalid, Value::Time)
}

/// Append the children of `value` that `segment` selects.
fn children<'a>(value: &'a RawValue, segment: &Segment, out: &mut Vec<&'a RawValue>) {
    let text = value.get().trim_start();
    if text.starts_with('{') {
        let Ok(Members(members)) = serde_json::from_str::<Members<'a>>(value.get()) else {
            return;
        };
        match segment {
            Segment::Wildcard => out.extend(members.into_iter().map(|(_, member)| member)),
            Segment::Key(key) => {
                // With duplicate members the last one wins, as in JSON.parse.
                if let Some((_, member)) = members
                    .into_iter()
                    .rev()
                    .find(|(name, _)| key_matches(name.get(), key))
                {
                    out.push(member);
                }
            }
        }
    } else if text.starts_with('[') {
        let Ok(elements) = serde_json::from_str::<Vec<&'a RawValue>>(value.get()) else {
            return;
        };
        match segment {
            Segment::Wildcard => out.extend(elements),
            Segment::Key(key) => {
                let canonical = key == "0" || (!key.starts_with('0') && !key.starts_with('+'));
                if let Some(element) = key
                    .parse::<usize>()
                    .ok()
                    .filter(|_| canonical)
                    .and_then(|index| elements.get(index).copied())
                {
                    out.push(element);
                }
            }
        }
    }
}

/// An object's members as raw `(key, value)` pairs, in document order.
struct Members<'a>(Vec<(&'a RawValue, &'a RawValue)>);

impl<'de: 'a, 'a> Deserialize<'de> for Members<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        deserializer.deserialize_map(MembersVisitor(std::marker::PhantomData))
    }
}

struct MembersVisitor<'a>(std::marker::PhantomData<&'a ()>);

impl<'de: 'a, 'a> Visitor<'de> for MembersVisitor<'a> {
    type Value = Members<'a>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where A: MapAccess<'de> {
        let mut members = Vec::new();
        while let Some(key) = map.next_key::<&'de RawValue>()? {
            members.push((key, map.next_value::<&'de RawValue>()?));
        }
        Ok(Members(members))
    }
}

/// Compare a raw JSON string literal (quotes included) with `field`.
fn key_matches(raw: &str, field: &str) -> bool {
    let inner = raw
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or(raw);
    if !inner.contains('\\') {
        return inner == field;
    }
    // Escaped keys are decoded; one that encodes a lone surrogate cannot
    // equal a Rust string and fails to decode, which is a non-match.
    serde_json::from_str::<String>(raw).is_ok_and(|key| key == field)
}

#[cfg(test)]
mod tests {
    use super::Extraction;
    use super::Extractor;
    use super::ExtractorConfig;
    use super::TimeUnit;
    use crate::store::SkipKind;

    fn extractor(each: Option<&str>, time: &[&str], end: &[&str], unit: TimeUnit) -> Extractor {
        Extractor::new(ExtractorConfig {
            each: each.map(str::to_owned),
            time: time.iter().map(|pointer| (*pointer).to_owned()).collect(),
            end: end.iter().map(|pointer| (*pointer).to_owned()).collect(),
            unit,
        })
        .expect("valid extractor")
    }

    fn event(t_ms: i64, t_end_ms: i64) -> Extraction {
        Extraction::Event { t_ms, t_end_ms }
    }

    #[test]
    fn legacy_timestamp_field_keeps_its_lexical_top_level_semantics() {
        let legacy = Extractor::timestamp_field("captured_at").expect("valid field");
        assert_eq!(
            legacy.extract(
                br#"{"note":"\ud800 lone","\udc00":1,"captured_at":"2026-01-02T03:04:05Z"}"#
            ),
            event(1_767_323_045_000, 1_767_323_045_000)
        );
        assert_eq!(
            legacy.extract(br#"{"captured_at":1,"nested":{"captured_at":2},"captured_at":3}"#),
            event(3, 3)
        );
        assert_eq!(legacy.extract(br#"{"captured\u005fat":42}"#), event(42, 42));
        // A deep sibling does not hit serde_json's recursion limit.
        let deep = format!(
            r#"{{"d":{}{},"captured_at":7}}"#,
            "[".repeat(200),
            "]".repeat(200)
        );
        assert_eq!(legacy.extract(deep.as_bytes()), event(7, 7));
        assert_eq!(legacy.extract(b"{\"captured_at\":9}\n"), event(9, 9));
    }

    #[test]
    fn bad_values_are_classified_instead_of_failing() {
        let legacy = Extractor::timestamp_field("captured_at").expect("valid field");
        for (text, kind) in [
            (r#"{"other":1}"#, SkipKind::Missing),
            (r#"{"captured_at":null}"#, SkipKind::Missing),
            (r#"{"captured_at":0}"#, SkipKind::Missing),
            (r#"[{"captured_at":1}]"#, SkipKind::Missing),
            ("12", SkipKind::Missing),
            (r#"{"captured_at":"\ud800"}"#, SkipKind::Invalid),
            (r#"{"captured_at":"not-a-time"}"#, SkipKind::Invalid),
            (r#"{"captured_at":1.5}"#, SkipKind::Invalid),
            (r#"{"captured_at":true}"#, SkipKind::Invalid),
            ("{not json", SkipKind::Unparseable),
            ("  \r\n", SkipKind::Blank),
        ] {
            assert_eq!(
                legacy.extract(text.as_bytes()),
                Extraction::Skip(kind),
                "{text}"
            );
        }
        assert_eq!(
            legacy.extract(b"\xff\xfe"),
            Extraction::Skip(SkipKind::Unparseable)
        );
    }

    #[test]
    fn otlp_spans_use_wildcards_units_and_min_max_aggregation() {
        let spans = extractor(
            Some("/resourceSpans/*/scopeSpans/*/spans/*"),
            &["/startTimeUnixNano"],
            &["/endTimeUnixNano"],
            TimeUnit::Ns,
        );
        let message = br#"{"resourceSpans":[{"scopeSpans":[{"spans":[
            {"startTimeUnixNano":"1759482001000000000","endTimeUnixNano":"1759482004000000000"},
            {"startTimeUnixNano":"1759482000500000000","endTimeUnixNano":"1759482002000000000"},
            {"startTimeUnixNano":"0"}
        ]}]}]}"#;
        assert_eq!(
            spans.extract(message),
            event(1_759_482_000_500, 1_759_482_004_000)
        );
        let logs = extractor(
            Some("/resourceLogs/*/scopeLogs/*/logRecords/*"),
            &["/timeUnixNano", "/observedTimeUnixNano"],
            &[],
            TimeUnit::Ns,
        );
        assert_eq!(
            logs.extract(
                br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"0","observedTimeUnixNano":"1759482000000000000"}]}]}]}"#
            ),
            event(1_759_482_000_000, 1_759_482_000_000)
        );
        assert_eq!(
            logs.extract(br#"{"resourceLogs":[]}"#),
            Extraction::Skip(SkipKind::Missing)
        );
        // Without `each`, a wildcard `end` keeps its latest value.
        let flat = extractor(None, &["/spans/*/s"], &["/spans/*/e"], TimeUnit::Ms);
        assert_eq!(
            flat.extract(br#"{"spans":[{"s":1,"e":5},{"s":2,"e":9}]}"#),
            event(1, 9)
        );
    }

    #[test]
    fn nested_pointers_fall_back_and_honor_declared_units() {
        let envelope = extractor(
            None,
            &["/entry/timestamp", "/timestamp"],
            &[],
            TimeUnit::Auto,
        );
        assert_eq!(
            envelope.extract(br#"{"entry":{"timestamp":"2026-01-02T03:04:05.250Z"}}"#),
            event(1_767_323_045_250, 1_767_323_045_250)
        );
        assert_eq!(
            envelope.extract(br#"{"timestamp":"1767323045000"}"#),
            event(1_767_323_045_000, 1_767_323_045_000)
        );
        let seconds = extractor(None, &["/t"], &[], TimeUnit::S);
        assert_eq!(seconds.extract(br#"{"t":1.5}"#), event(1_500, 1_500));
        assert_eq!(seconds.extract(br#"{"t":"2.0019"}"#), event(2_001, 2_001));
        assert_eq!(seconds.extract(br#"{"t":3}"#), event(3_000, 3_000));
        assert_eq!(
            seconds.extract(br#"{"t":"1e3"}"#),
            Extraction::Skip(SkipKind::Invalid)
        );
        let micros = extractor(None, &["/t"], &[], TimeUnit::Us);
        assert_eq!(micros.extract(br#"{"t":1500000}"#), event(1_500, 1_500));
        let rfc = extractor(None, &["/t"], &[], TimeUnit::Rfc3339);
        assert_eq!(
            rfc.extract(br#"{"t":1500}"#),
            Extraction::Skip(SkipKind::Invalid)
        );
        let indexed = extractor(None, &["/a/1/t", "/a/~0k~1/t"], &[], TimeUnit::Ms);
        assert_eq!(indexed.extract(br#"{"a":[{"t":1},{"t":2}]}"#), event(2, 2));
        assert_eq!(indexed.extract(br#"{"a":{"~k/":{"t":5}}}"#), event(5, 5));
    }

    #[test]
    fn invalid_configurations_are_rejected() {
        for config in [
            ExtractorConfig {
                each: None,
                time: Vec::new(),
                end: Vec::new(),
                unit: TimeUnit::Auto,
            },
            ExtractorConfig {
                each: Some("no-slash".to_owned()),
                time: vec!["/t".to_owned()],
                end: Vec::new(),
                unit: TimeUnit::Auto,
            },
            ExtractorConfig {
                each: None,
                time: vec!["/bad~2".to_owned()],
                end: Vec::new(),
                unit: TimeUnit::Auto,
            },
        ] {
            assert!(matches!(
                Extractor::new(config),
                Err(crate::IndexError::InvalidExtractor(_))
            ));
        }
        let first = Extractor::timestamp_field("captured_at").expect("valid");
        let second = Extractor::new(ExtractorConfig {
            each: None,
            time: vec!["/captured_at".to_owned()],
            end: Vec::new(),
            unit: TimeUnit::Auto,
        })
        .expect("valid");
        assert_eq!(first.digest(), second.digest());
    }
}
