//! JSON Message Text (P1): the storage rule for `application/json` streams.
//!
//! A JSON write body is validated as RFC 8259 text without building a value
//! tree, a top-level array is flattened exactly once, and each message is
//! stored as the writer's text with insignificant whitespace removed, followed
//! by one LF. Member order, duplicate members, number text and string escapes
//! (including escapes of unpaired surrogates) are kept byte for byte.

use serde_json::value::RawValue;

/// Deepest nesting a stored message may have. A scalar has depth 0; an array
/// or object has depth 1 plus the deepest of its elements, and an empty one
/// has depth 1. Depth is measured per message, after flattening.
pub const MAX_JSON_MESSAGE_DEPTH: usize = 127;

/// Why a JSON write body was refused. Every variant maps to HTTP 400.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonTextError {
    InvalidUtf8(String),
    InvalidJson(String),
    EmptyArray,
    TooDeep,
}

impl std::fmt::Display for JsonTextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUtf8(err) => write!(f, "invalid JSON payload: invalid UTF-8: {err}"),
            Self::InvalidJson(err) => write!(f, "invalid JSON payload: {err}"),
            Self::EmptyArray => f.write_str("JSON append array must not be empty"),
            Self::TooDeep => write!(
                f,
                "invalid JSON payload: message nesting depth exceeds {MAX_JSON_MESSAGE_DEPTH}"
            ),
        }
    }
}

impl std::error::Error for JsonTextError {}

/// Validate a JSON write body and return its stored form: one minified
/// message per LF-terminated line. A top-level array contributes one message
/// per element; an empty top-level array yields no messages when
/// `allow_empty_array` is set and is an error otherwise.
pub fn normalize_json_messages(
    body: &[u8],
    allow_empty_array: bool,
) -> Result<Vec<u8>, JsonTextError> {
    let text =
        std::str::from_utf8(body).map_err(|err| JsonTextError::InvalidUtf8(err.to_string()))?;
    // `RawValue` validates the grammar without building a tree or limiting
    // depth, and accepts `\u` escapes of unpaired surrogates.
    serde_json::from_str::<&RawValue>(text)
        .map_err(|err| JsonTextError::InvalidJson(err.to_string()))?;
    minify_validated(text.as_bytes(), allow_empty_array)
}

/// Lexical pass over text already known to be valid JSON: strips whitespace
/// outside strings, splits a top-level array into messages and enforces the
/// per-message depth limit.
fn minify_validated(input: &[u8], allow_empty_array: bool) -> Result<Vec<u8>, JsonTextError> {
    let first = input
        .iter()
        .copied()
        .find(|byte| !is_json_whitespace(*byte));
    let flatten = first == Some(b'[');
    // Nesting level of the outer array that is being flattened, if any.
    let base = usize::from(flatten);

    let mut out = Vec::with_capacity(input.len().saturating_add(1));
    let mut level = 0usize;
    let mut message_open = false;
    let mut messages = 0usize;
    let mut index = 0usize;
    while let Some(&byte) = input.get(index) {
        match byte {
            b' ' | b'\t' | b'\n' | b'\r' => {}
            b'"' => {
                let end = string_end(input, index);
                out.extend_from_slice(input.get(index..end).unwrap_or_default());
                message_open = true;
                index = end;
                continue;
            }
            b'[' | b'{' => {
                level = level.saturating_add(1);
                if flatten && level == 1 {
                    index = index.saturating_add(1);
                    continue;
                }
                if level.saturating_sub(base) > MAX_JSON_MESSAGE_DEPTH {
                    return Err(JsonTextError::TooDeep);
                }
                out.push(byte);
                message_open = true;
            }
            b']' | b'}' => {
                if flatten && level == 1 {
                    if message_open {
                        out.push(b'\n');
                        messages = messages.saturating_add(1);
                        message_open = false;
                    }
                    level = 0;
                } else {
                    level = level.saturating_sub(1);
                    out.push(byte);
                }
            }
            b',' if flatten && level == 1 => {
                out.push(b'\n');
                messages = messages.saturating_add(1);
                message_open = false;
            }
            _ => {
                out.push(byte);
                message_open = true;
            }
        }
        index = index.saturating_add(1);
    }
    if flatten {
        if messages == 0 && !allow_empty_array {
            return Err(JsonTextError::EmptyArray);
        }
    } else {
        out.push(b'\n');
    }
    Ok(out)
}

/// Index one past the closing quote of the string that opens at `start`.
/// The input is valid JSON, so every string is terminated.
fn string_end(input: &[u8], start: usize) -> usize {
    let mut index = start.saturating_add(1);
    loop {
        let Some(rest) = input.get(index..) else {
            return input.len();
        };
        match memchr::memchr2(b'"', b'\\', rest) {
            Some(offset) => {
                let at = index.saturating_add(offset);
                if input.get(at) == Some(&b'"') {
                    return at.saturating_add(1);
                }
                // Skip the backslash and the escaped byte; `\uXXXX` digits are
                // plain bytes the next search passes over.
                index = at.saturating_add(2);
            }
            None => return input.len(),
        }
    }
}

fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

/// Remove insignificant whitespace from valid JSON text. Exposed for tests
/// and benchmarks as the reference `minify(input)` of a single message.
pub fn minify(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    for ch in input.chars() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
        } else if ch == '"' {
            in_string = true;
            out.push(ch);
        } else if !matches!(ch, ' ' | '\t' | '\n' | '\r') {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(body: &str) -> Result<String, JsonTextError> {
        normalize_json_messages(body.as_bytes(), false)
            .map(|bytes| String::from_utf8(bytes).unwrap())
    }

    fn nested(depth: usize) -> String {
        format!("{}{}", "[".repeat(depth), "]".repeat(depth))
    }

    #[test]
    fn keeps_member_order_duplicates_numbers_and_escapes() {
        let body = r#" { "b" : 1.50e3 , "a" : -0 , "a" : 1e400, "s" : "\ud800 \" \\ \/ é" } "#;
        assert_eq!(
            stored(body).unwrap(),
            "{\"b\":1.50e3,\"a\":-0,\"a\":1e400,\"s\":\"\\ud800 \\\" \\\\ \\/ é\"}\n"
        );
    }

    #[test]
    fn flattens_top_level_array_exactly_once() {
        assert_eq!(
            stored("[ [1, 2] , {\"a\" : [ ]} , \"x , ]\" , 3 ]").unwrap(),
            "[1,2]\n{\"a\":[]}\n\"x , ]\"\n3\n"
        );
        assert_eq!(stored("[[]]").unwrap(), "[]\n");
        assert_eq!(stored("\"[1]\"").unwrap(), "\"[1]\"\n");
        assert_eq!(stored(" 7 ").unwrap(), "7\n");
    }

    #[test]
    fn empty_array_is_refused_unless_allowed() {
        assert_eq!(stored(" [ ] "), Err(JsonTextError::EmptyArray));
        assert_eq!(normalize_json_messages(b"[]", true).unwrap(), b"");
    }

    #[test]
    fn depth_limit_is_per_message_after_flattening() {
        // Bare (non-array) messages: 127 accepted, 128 refused.
        let bare = |depth: usize| format!("{{\"k\":{}}}", nested(depth - 1));
        assert_eq!(stored(&bare(127)).unwrap(), format!("{}\n", bare(127)));
        assert_eq!(stored(&bare(128)), Err(JsonTextError::TooDeep));
        // A bare array body is flattened: its elements are the messages.
        assert_eq!(stored(&nested(128)).unwrap(), format!("{}\n", nested(127)));
        assert_eq!(stored(&nested(129)), Err(JsonTextError::TooDeep));
        // Wrapped: the outer array is flattened, so its message keeps 127.
        let wrapped = format!("[{}]", nested(127));
        assert_eq!(stored(&wrapped).unwrap(), format!("{}\n", nested(127)));
        let wrapped = format!("[1,{}]", nested(128));
        assert_eq!(stored(&wrapped), Err(JsonTextError::TooDeep));
        // Objects count like arrays.
        let objects = format!("{}1{}", "{\"k\":".repeat(127), "}".repeat(127));
        assert!(stored(&objects).is_ok());
        let objects = format!("{}1{}", "{\"k\":".repeat(128), "}".repeat(128));
        assert_eq!(stored(&objects), Err(JsonTextError::TooDeep));
        // A bracket inside a string is not nesting.
        let in_string = format!("[\"{}\"]", "[".repeat(500));
        assert!(stored(&in_string).is_ok());
    }

    #[test]
    fn refuses_invalid_json_and_utf8() {
        for body in [
            "{",
            "[1,]",
            "{\"a\":1,}",
            "01",
            "\"\\x\"",
            "1 2",
            "  ",
            "\"\u{1}\"",
        ] {
            assert!(
                matches!(stored(body), Err(JsonTextError::InvalidJson(_))),
                "{body:?}"
            );
        }
        assert!(matches!(
            normalize_json_messages(b"\"\xff\"", false),
            Err(JsonTextError::InvalidUtf8(_))
        ));
        // A truncated `\u` escape is invalid.
        assert!(matches!(
            stored("\"\\ud8\""),
            Err(JsonTextError::InvalidJson(_))
        ));
    }

    #[test]
    fn deep_invalid_body_does_not_overflow_the_stack() {
        let body = "[".repeat(1_000_000);
        assert!(matches!(stored(&body), Err(JsonTextError::InvalidJson(_))));
        let body = nested(1_000_000);
        assert_eq!(stored(&body), Err(JsonTextError::TooDeep));
    }

    mod prop {
        use proptest::prelude::*;

        use super::super::*;

        #[derive(Debug, Clone)]
        enum Json {
            Scalar(String),
            Array(Vec<Json>),
            Object(Vec<(String, Json)>),
        }

        fn string_literal() -> impl Strategy<Value = String> {
            let piece = prop_oneof![
                Just("\\ud800".to_owned()),
                Just("\\udfff".to_owned()),
                Just("\\ud83d\\ude00".to_owned()),
                Just("\\uDBFF".to_owned()),
                Just("\\\"".to_owned()),
                Just("\\\\".to_owned()),
                Just("\\/".to_owned()),
                Just("\\b\\f\\n\\r\\t".to_owned()),
                Just("\\u0000".to_owned()),
                Just("  ".to_owned()),
                Just("[{,:}]".to_owned()),
                Just("é中😀".to_owned()),
                "[a-z0-9 ]{0,6}",
            ];
            proptest::collection::vec(piece, 0..5)
                .prop_map(|pieces| format!("\"{}\"", pieces.concat()))
        }

        fn number_literal() -> impl Strategy<Value = String> {
            prop_oneof![
                Just("-0".to_owned()),
                Just("1e400".to_owned()),
                Just("1.50e3".to_owned()),
                Just("-1E-400".to_owned()),
                Just("123456789012345678901234567890".to_owned()),
                "-?(0|[1-9][0-9]{0,5})(\\.[0-9]{1,4})?([eE][+-]?[0-9]{1,3})?",
            ]
        }

        fn scalar() -> impl Strategy<Value = Json> {
            prop_oneof![
                string_literal(),
                number_literal(),
                Just("true".to_owned()),
                Just("false".to_owned()),
                Just("null".to_owned()),
            ]
            .prop_map(Json::Scalar)
        }

        fn json() -> impl Strategy<Value = Json> {
            scalar().prop_recursive(6, 64, 6, |inner| {
                let key = prop_oneof![
                    Just("\"a\"".to_owned()),
                    Just("\"b\"".to_owned()),
                    Just("\"\\ud800\"".to_owned()),
                    string_literal(),
                ];
                prop_oneof![
                    proptest::collection::vec(inner.clone(), 0..6).prop_map(Json::Array),
                    proptest::collection::vec((key, inner), 0..6).prop_map(Json::Object),
                ]
            })
        }

        fn whitespace() -> impl Strategy<Value = Vec<String>> {
            proptest::collection::vec("[ \t\n\r]{0,3}", 1..16)
        }

        struct Ws<'a> {
            pieces: &'a [String],
            next: usize,
        }

        impl Ws<'_> {
            fn take(&mut self) -> &str {
                let piece = &self.pieces[self.next % self.pieces.len()];
                self.next += 1;
                piece
            }
        }

        fn render(value: &Json, ws: &mut Ws<'_>, out: &mut String) {
            match value {
                Json::Scalar(text) => out.push_str(text),
                Json::Array(items) => {
                    out.push('[');
                    out.push_str(ws.take());
                    for (index, item) in items.iter().enumerate() {
                        if index > 0 {
                            out.push_str(ws.take());
                            out.push(',');
                            out.push_str(ws.take());
                        }
                        render(item, ws, out);
                    }
                    out.push_str(ws.take());
                    out.push(']');
                }
                Json::Object(members) => {
                    out.push('{');
                    out.push_str(ws.take());
                    for (index, (key, item)) in members.iter().enumerate() {
                        if index > 0 {
                            out.push(',');
                            out.push_str(ws.take());
                        }
                        out.push_str(key);
                        out.push_str(ws.take());
                        out.push(':');
                        out.push_str(ws.take());
                        render(item, ws, out);
                        out.push_str(ws.take());
                    }
                    out.push('}');
                }
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(4096))]

            #[test]
            fn stored_is_the_minified_writer_text(value in json(), pieces in whitespace()) {
                let mut ws = Ws { pieces: &pieces, next: 0 };
                let mut body = ws.take().to_owned();
                render(&value, &mut ws, &mut body);
                body.push_str(ws.take());

                let expected = match &value {
                    Json::Array(items) => items
                        .iter()
                        .map(|item| {
                            let mut text = String::new();
                            render(item, &mut Ws { pieces: &pieces, next: 0 }, &mut text);
                            format!("{}\n", minify(&text))
                        })
                        .collect::<String>(),
                    _ => format!("{}\n", minify(&body)),
                };
                let result = normalize_json_messages(body.as_bytes(), true);
                prop_assert_eq!(
                    result.as_ref().map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
                    Ok(expected),
                    "body: {:?}",
                    body
                );
                // Every stored line is itself valid JSON text.
                for line in result.unwrap().split(|byte| *byte == b'\n').filter(|line| !line.is_empty()) {
                    let text = std::str::from_utf8(line).unwrap();
                    prop_assert!(serde_json::from_str::<&RawValue>(text).is_ok(), "{:?}", text);
                }
            }

            #[test]
            fn arbitrary_bytes_never_panic(body in proptest::collection::vec(any::<u8>(), 0..64)) {
                let _result = normalize_json_messages(&body, false);
            }
        }
    }
}
