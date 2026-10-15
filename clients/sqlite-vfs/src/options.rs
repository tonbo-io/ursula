//! `ursula_open`'s `options_json` (design §8.6): one flat JSON object, parsed at the boundary into
//! [`OpenOptions`].

use crate::ids::Activation;

/// The default backlog bound: 64 MiB of compressed frames (design §4.4).
pub(crate) const DEFAULT_MAX_PENDING_BYTES: u64 = 64 << 20;

/// What `options_json` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenOptions {
    pub(crate) max_pending_bytes: u64,
    /// The platform activation id, 32 hex digits; a random one per process otherwise.
    pub(crate) activation: Option<Activation>,
    /// The bearer token (one per process in M1).
    pub(crate) token: Option<String>,
    /// The SDK's User-Agent token, accepted and unused until M3's transport.
    pub(crate) agent: Option<String>,
}

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions {
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
            activation: None,
            token: None,
            agent: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum OptionsError {
    #[error("options_json is not a JSON object: {why} at byte {at}")]
    Syntax { at: usize, why: &'static str },
    #[error("options_json: unknown option {0:?}")]
    Unknown(String),
    #[error("options_json: {name} must be {expected}")]
    Type {
        name: &'static str,
        expected: &'static str,
    },
    #[error("options_json: {0} is not supported before M2")]
    NotYet(&'static str),
}

/// A JSON value of a flat object: no nested objects or arrays.
#[derive(Debug, Clone, PartialEq)]
enum Value {
    Null,
    Bool(bool),
    Number(f64),
    Text(String),
}

impl TryFrom<&str> for OpenOptions {
    type Error = OptionsError;

    /// An empty text, `null` or `{}` asks for the defaults.
    fn try_from(text: &str) -> Result<Self, OptionsError> {
        let mut options = OpenOptions::default();
        let text = text.trim();
        if text.is_empty() || text == "null" {
            return Ok(options);
        }
        for (key, value) in object(text)? {
            match (key.as_str(), value) {
                (_, Value::Null) => {}
                ("max_pending_bytes", Value::Number(n)) => {
                    options.max_pending_bytes = whole(n).ok_or(OptionsError::Type {
                        name: "max_pending_bytes",
                        expected: "a whole number of bytes",
                    })?;
                }
                ("activation", Value::Text(hex)) => {
                    options.activation = Some(activation(&hex).ok_or(OptionsError::Type {
                        name: "activation",
                        expected: "32 hex digits",
                    })?);
                }
                ("token", Value::Text(token)) => options.token = Some(token),
                ("agent", Value::Text(agent)) => options.agent = Some(agent),
                ("read_only", Value::Bool(false)) | ("recreate", Value::Bool(false)) => {}
                ("read_only", Value::Bool(true)) => return Err(OptionsError::NotYet("read_only")),
                ("recreate", Value::Bool(true)) => return Err(OptionsError::NotYet("recreate")),
                (name @ ("max_pending_bytes" | "activation" | "token" | "agent"), _) => {
                    return Err(OptionsError::Type {
                        name: known(name),
                        expected: if name == "max_pending_bytes" {
                            "a number"
                        } else {
                            "a string"
                        },
                    });
                }
                (name @ ("read_only" | "recreate"), _) => {
                    return Err(OptionsError::Type {
                        name: known(name),
                        expected: "a boolean",
                    });
                }
                (other, _) => return Err(OptionsError::Unknown(other.to_owned())),
            }
        }
        Ok(options)
    }
}

fn known(name: &str) -> &'static str {
    [
        "max_pending_bytes",
        "activation",
        "token",
        "agent",
        "read_only",
        "recreate",
    ]
    .into_iter()
    .find(|k| *k == name)
    .unwrap_or("option")
}

fn whole(n: f64) -> Option<u64> {
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

fn activation(hex: &str) -> Option<Activation> {
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, o) in out.iter_mut().enumerate() {
        let at = i.checked_mul(2)?;
        *o = u8::from_str_radix(hex.get(at..at.checked_add(2)?)?, 16).ok()?;
    }
    Some(Activation(out))
}

/// The members of a flat JSON object.
fn object(text: &str) -> Result<Vec<(String, Value)>, OptionsError> {
    let mut p = Parser {
        bytes: text.as_bytes(),
        at: 0,
    };
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
        let value = p.value()?;
        members.push((key, value));
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
    fn fail<T>(&self, why: &'static str) -> Result<T, OptionsError> {
        Err(OptionsError::Syntax { at: self.at, why })
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

    fn expect(&mut self, b: u8) -> Result<(), OptionsError> {
        if self.eat(b) {
            Ok(())
        } else {
            self.fail("unexpected character")
        }
    }

    fn end<T>(&mut self, t: T) -> Result<T, OptionsError> {
        self.space();
        if self.at == self.bytes.len() {
            Ok(t)
        } else {
            self.fail("text after the object")
        }
    }

    fn literal(&mut self, word: &[u8], value: Value) -> Result<Value, OptionsError> {
        let end = self.at.saturating_add(word.len());
        if self.bytes.get(self.at..end) == Some(word) {
            self.at = end;
            Ok(value)
        } else {
            self.fail("unknown literal")
        }
    }

    fn value(&mut self) -> Result<Value, OptionsError> {
        match self.peek() {
            Some(b'"') => Ok(Value::Text(self.string()?)),
            Some(b't') => self.literal(b"true", Value::Bool(true)),
            Some(b'f') => self.literal(b"false", Value::Bool(false)),
            Some(b'n') => self.literal(b"null", Value::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => self.fail("not a string, number, boolean or null"),
        }
    }

    fn number(&mut self) -> Result<Value, OptionsError> {
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

    fn string(&mut self) -> Result<String, OptionsError> {
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
    use super::DEFAULT_MAX_PENDING_BYTES;
    use super::OpenOptions;
    use super::OptionsError;
    use crate::ids::Activation;

    #[test]
    fn defaults_for_nothing() {
        for text in ["", " ", "null", "{}", " { } "] {
            assert_eq!(OpenOptions::try_from(text), Ok(OpenOptions::default()));
        }
        assert_eq!(
            OpenOptions::default().max_pending_bytes,
            DEFAULT_MAX_PENDING_BYTES
        );
    }

    #[test]
    fn every_option() {
        let o = OpenOptions::try_from(
            r#"{"max_pending_bytes": 1048576, "activation": "000102030405060708090a0b0c0d0e0f",
                "token": "t1\"x", "agent": "sdk/1", "read_only": false, "recreate": null}"#,
        )
        .unwrap();
        assert_eq!(o.max_pending_bytes, 1 << 20);
        assert_eq!(
            o.activation,
            Some(Activation([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
            ]))
        );
        assert_eq!(o.token.as_deref(), Some("t1\"x"));
        assert_eq!(o.agent.as_deref(), Some("sdk/1"));
    }

    #[test]
    fn refusals_name_the_option() {
        let err = |text: &str| OpenOptions::try_from(text).unwrap_err();
        assert!(matches!(err("[]"), OptionsError::Syntax { at: 0, .. }));
        assert!(matches!(err("{\"a\":1"), OptionsError::Syntax { .. }));
        assert!(matches!(err("{} x"), OptionsError::Syntax { .. }));
        assert_eq!(
            err(r#"{"color": "red"}"#),
            OptionsError::Unknown("color".into())
        );
        assert!(matches!(
            err(r#"{"max_pending_bytes": -1}"#),
            OptionsError::Type {
                name: "max_pending_bytes",
                ..
            }
        ));
        assert!(matches!(
            err(r#"{"max_pending_bytes": 1.5}"#),
            OptionsError::Type { .. }
        ));
        assert!(matches!(
            err(r#"{"max_pending_bytes": "1"}"#),
            OptionsError::Type { .. }
        ));
        assert!(matches!(
            err(r#"{"activation": "abc"}"#),
            OptionsError::Type {
                name: "activation",
                ..
            }
        ));
        assert_eq!(
            err(r#"{"read_only": true}"#),
            OptionsError::NotYet("read_only")
        );
        assert_eq!(
            err(r#"{"recreate": true}"#),
            OptionsError::NotYet("recreate")
        );
    }
}
