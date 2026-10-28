//! The flat JSON objects the extension reads and writes: `ursula_open`'s `options_json`, the
//! JSON the SQL functions return, a salvage's `manifest.json`, and a token endpoint's answer. One
//! writer ([`str`], [`object`], [`array`]) and one parser of objects, which reads no nested object
//! or array: [`parse_object`] refuses one, and [`parse_members`] checks it and skips it.

use std::fmt::Write as _;

/// The JSON string of `s`.
pub(crate) fn str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 => {
                let _infallible = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A JSON object of `fields`, each value already JSON.
pub(crate) fn object(fields: &[(&str, String)]) -> String {
    let body: Vec<String> = fields
        .iter()
        .map(|(key, value)| format!("{}:{value}", str(key)))
        .collect();
    format!("{{{}}}", body.join(","))
}

/// A JSON array of `items`, each already JSON.
pub(crate) fn array(items: &[String]) -> String {
    format!("[{}]", items.join(","))
}

pub(crate) fn null() -> String {
    "null".to_owned()
}

/// A JSON value of a flat object: no nested objects or arrays.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Value {
    Null,
    Bool(bool),
    Number(f64),
    Text(String),
}

/// Text that is not one flat JSON object.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not a flat JSON object: {why} at byte {at}")]
pub(crate) struct SyntaxError {
    pub(crate) at: usize,
    pub(crate) why: &'static str,
}

/// `n` as a whole number, if it is one from 0 to 2^53 - 1.
pub(crate) fn whole(n: f64) -> Option<u64> {
    const MAX: f64 = 9_007_199_254_740_991.0;
    if n.is_finite() && (0.0..=MAX).contains(&n) && n.fract() == 0.0 {
        #[expect(
            clippy::cast_sign_loss,
            reason = "checked above: a whole number from 0 to 2^53 - 1"
        )]
        #[expect(
            clippy::cast_possible_truncation,
            reason = "checked above: a whole number from 0 to 2^53 - 1"
        )]
        Some(n as u64)
    } else {
        None
    }
}

/// The members of a flat JSON object, in order.
pub(crate) fn parse_object(text: &str) -> Result<Vec<(String, Value)>, SyntaxError> {
    parse(text, Nested::Refuse)
}

/// The members of a JSON object whose values are not objects or arrays, in order: a nested value
/// is checked and skipped, for an answer whose other members the reader does not know.
pub(crate) fn parse_members(text: &str) -> Result<Vec<(String, Value)>, SyntaxError> {
    parse(text, Nested::Skip)
}

/// What the parser does with a member whose value is an object or an array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Nested {
    Refuse,
    Skip,
}

/// How deep a skipped value may nest.
const MAX_DEPTH: usize = 64;

fn parse(text: &str, nested: Nested) -> Result<Vec<(String, Value)>, SyntaxError> {
    let mut p = Parser {
        bytes: text.as_bytes(),
        at: 0,
    };
    p.space();
    p.expect(b'{')?;
    let mut members = Vec::new();
    p.space();
    if p.eat(b'}') {
        return p.end(members);
    }
    loop {
        p.space();
        let key = p.string()?;
        p.space();
        p.expect(b':')?;
        p.space();
        if nested == Nested::Skip && matches!(p.peek(), Some(b'{' | b'[')) {
            p.skip(0)?;
        } else {
            let value = p.value()?;
            members.push((key, value));
        }
        p.space();
        if p.eat(b',') {
            continue;
        }
        p.expect(b'}')?;
        return p.end(members);
    }
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn fail<T>(&self, why: &'static str) -> Result<T, SyntaxError> {
        Err(SyntaxError { at: self.at, why })
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn bump(&mut self) {
        self.at = self.at.saturating_add(1);
    }

    fn space(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.bump();
        }
    }

    fn eat(&mut self, b: u8) -> bool {
        let found = self.peek() == Some(b);
        if found {
            self.bump();
        }
        found
    }

    fn expect(&mut self, b: u8) -> Result<(), SyntaxError> {
        if self.eat(b) {
            Ok(())
        } else {
            self.fail("unexpected character")
        }
    }

    fn end<T>(&mut self, t: T) -> Result<T, SyntaxError> {
        self.space();
        if self.at == self.bytes.len() {
            Ok(t)
        } else {
            self.fail("text after the object")
        }
    }

    fn literal(&mut self, word: &[u8], value: Value) -> Result<Value, SyntaxError> {
        let end = self.at.saturating_add(word.len());
        if self.bytes.get(self.at..end) == Some(word) {
            self.at = end;
            Ok(value)
        } else {
            self.fail("unknown literal")
        }
    }

    fn value(&mut self) -> Result<Value, SyntaxError> {
        match self.peek() {
            Some(b'"') => Ok(Value::Text(self.string()?)),
            Some(b't') => self.literal(b"true", Value::Bool(true)),
            Some(b'f') => self.literal(b"false", Value::Bool(false)),
            Some(b'n') => self.literal(b"null", Value::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => self.fail("not a string, number, boolean or null"),
        }
    }

    /// Checks and skips one value, objects and arrays included, `depth` levels deep.
    fn skip(&mut self, depth: usize) -> Result<(), SyntaxError> {
        let (close, object) = match self.peek() {
            Some(b'{') => (b'}', true),
            Some(b'[') => (b']', false),
            _ => return self.value().map(drop),
        };
        if depth >= MAX_DEPTH {
            return self.fail("nested too deep");
        }
        self.bump();
        self.space();
        if self.eat(close) {
            return Ok(());
        }
        loop {
            self.space();
            if object {
                self.string()?;
                self.space();
                self.expect(b':')?;
                self.space();
            }
            self.skip(depth.saturating_add(1))?;
            self.space();
            if self.eat(b',') {
                continue;
            }
            return self.expect(close);
        }
    }

    fn number(&mut self) -> Result<Value, SyntaxError> {
        let start = self.at;
        while self
            .peek()
            .is_some_and(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'))
        {
            self.bump();
        }
        let text = std::str::from_utf8(self.bytes.get(start..self.at).unwrap_or_default());
        match text.ok().and_then(|t| t.parse::<f64>().ok()) {
            Some(n) => Ok(Value::Number(n)),
            None => self.fail("malformed number"),
        }
    }

    fn string(&mut self) -> Result<String, SyntaxError> {
        self.expect(b'"')?;
        let mut out = Vec::new();
        loop {
            let Some(b) = self.peek() else {
                return self.fail("unterminated string");
            };
            self.bump();
            match b {
                b'"' => break,
                b'\\' => {
                    let Some(e) = self.peek() else {
                        return self.fail("unterminated escape");
                    };
                    self.bump();
                    let c = match e {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let end = self.at.saturating_add(4);
                            let hex = self
                                .bytes
                                .get(self.at..end)
                                .and_then(|h| std::str::from_utf8(h).ok())
                                .and_then(|h| u32::from_str_radix(h, 16).ok())
                                .and_then(char::from_u32);
                            let Some(c) = hex else {
                                return self.fail("malformed \\u escape");
                            };
                            self.at = end;
                            c
                        }
                        _ => return self.fail("unknown escape"),
                    };
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                }
                b if b < 0x20 => return self.fail("control character in a string"),
                b => out.push(b),
            }
        }
        String::from_utf8(out).or_else(|_not_utf8| self.fail("a string that is not UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::Value;
    use super::object;
    use super::parse_members;
    use super::parse_object;
    use super::str;

    #[test]
    fn strings_are_escaped() {
        assert_eq!(str("a\"b\\c\n"), r#""a\"b\\c\u000a""#);
    }

    #[test]
    fn what_the_writer_writes_the_parser_reads() {
        let text = object(&[
            ("a", str("x\"y")),
            ("b", "12".to_owned()),
            ("c", "true".to_owned()),
            ("d", "null".to_owned()),
        ]);
        assert_eq!(parse_object(&text).unwrap(), vec![
            ("a".to_owned(), Value::Text("x\"y".to_owned())),
            ("b".to_owned(), Value::Number(12.0)),
            ("c".to_owned(), Value::Bool(true)),
            ("d".to_owned(), Value::Null),
        ]);
        assert_eq!(parse_object("[]").unwrap_err().at, 0);
        assert_eq!(parse_object("{\"a\":{}}").unwrap_err().at, 5);
    }

    // Members whose values nest are checked and skipped, the others read.
    #[test]
    fn nested_members_are_skipped_but_checked() {
        let text =
            r#"{"a": [1, "x", {"b": [true, null, {}]}, []], "c": "y", "d": {"e": {"f": 2}}}"#;
        assert_eq!(parse_members(text).unwrap(), vec![(
            "c".to_owned(),
            Value::Text("y".to_owned())
        )]);
        assert!(parse_members("{}").unwrap().is_empty());
        for broken in [
            r#"{"a": [1,}"#,
            r#"{"a": {"b"}}"#,
            r#"{"a": {1: 2}}"#,
            r#"{"a": [1] "c": 2}"#,
            r#"{"a": ["#,
        ] {
            assert!(parse_members(broken).is_err(), "{broken}");
        }
        let deep = format!("{{\"a\": {}{}}}", "[".repeat(65), "]".repeat(65));
        assert_eq!(parse_members(&deep).unwrap_err().why, "nested too deep");
        let fits = format!("{{\"a\": {}{}}}", "[".repeat(64), "]".repeat(64));
        assert!(parse_members(&fits).unwrap().is_empty());
    }
}
