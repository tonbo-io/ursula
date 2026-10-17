//! `ursula_open`'s `options_json` (design: SQL functions): one flat JSON object, parsed at the
//! boundary into [`OpenOptions`].

use crate::codec::frame::IMPORT_ACTIVATION;
use crate::ids::Activation;
use crate::json::SyntaxError;
use crate::json::Value;
use crate::json::parse_object;
use crate::json::whole;

/// The default backlog bound: 64 MiB of compressed frames (design: Backpressure).
pub(crate) const DEFAULT_MAX_PENDING_BYTES: u64 = 64 << 20;

/// Whether an open makes this process the stream's owner or opens a read-only snapshot of it
/// (design: Open, Readers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Access {
    /// Claims the stream. With `recreate`, a missing stream whose trusted local generation holds
    /// data is created again, the generation salvaged (`recreated`), instead of refusing
    /// `not_found`.
    Owner { recreate: bool },
    /// `read_only`: the stream as of the open, from a reader directory or the binding this process
    /// owns, claiming nothing.
    Reader,
}

/// What `options_json` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenOptions {
    pub(crate) max_pending_bytes: u64,
    /// The platform activation id, 32 hex digits; a random one per open otherwise.
    pub(crate) activation: Option<Activation>,
    /// The bearer token (one per process).
    pub(crate) token: Option<String>,
    /// The SDK's User-Agent token: accepted, and not sent.
    pub(crate) agent: Option<String>,
    /// How long an open started with `timeout_ms` 0 may take: the caller's deadline, 30 s when
    /// absent.
    pub(crate) timeout_ms: Option<u64>,
    /// Owner or reader: `read_only` and `recreate` together are refused.
    pub(crate) access: Access,
}

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions {
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
            activation: None,
            token: None,
            agent: None,
            timeout_ms: None,
            access: Access::Owner { recreate: false },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum OptionsError {
    #[error("options_json: {0}")]
    Syntax(#[from] SyntaxError),
    #[error("options_json: unknown option {0:?}")]
    Unknown(String),
    #[error("options_json: {name} must be {expected}")]
    Type {
        name: &'static str,
        expected: &'static str,
    },
    #[error("options_json: read_only opens claim nothing, so recreate cannot go with it")]
    ReadOnlyRecreate,
    /// The activation every import's claim carries, which no open's claim may.
    #[error("options_json: activation {0} is reserved for ursula_import")]
    ReservedActivation(String),
}

impl TryFrom<&str> for OpenOptions {
    type Error = OptionsError;

    /// An empty text, `null` or `{}` asks for the defaults.
    fn try_from(text: &str) -> Result<Self, OptionsError> {
        let mut options = OpenOptions::default();
        let (mut read_only, mut recreate) = (false, false);
        let text = text.trim();
        if text.is_empty() || text == "null" {
            return Ok(options);
        }
        for (key, value) in parse_object(text)? {
            match (key.as_str(), value) {
                (_, Value::Null) => {}
                ("max_pending_bytes", Value::Number(n)) => {
                    options.max_pending_bytes = whole(n).ok_or(OptionsError::Type {
                        name: "max_pending_bytes",
                        expected: "a whole number of bytes",
                    })?;
                }
                ("activation", Value::Text(hex)) => {
                    let parsed = activation(&hex).ok_or(OptionsError::Type {
                        name: "activation",
                        expected: "32 hex digits",
                    })?;
                    if parsed == IMPORT_ACTIVATION {
                        return Err(OptionsError::ReservedActivation(hex));
                    }
                    options.activation = Some(parsed);
                }
                ("timeout_ms", Value::Number(n)) => {
                    options.timeout_ms =
                        Some(whole(n).filter(|&ms| ms > 0).ok_or(OptionsError::Type {
                            name: "timeout_ms",
                            expected: "a positive whole number of milliseconds",
                        })?);
                }
                ("token", Value::Text(token)) => options.token = Some(token),
                ("agent", Value::Text(agent)) => options.agent = Some(agent),
                ("read_only", Value::Bool(value)) => read_only = value,
                ("recreate", Value::Bool(value)) => recreate = value,
                (
                    name @ ("max_pending_bytes" | "timeout_ms" | "activation" | "token" | "agent"),
                    _,
                ) => {
                    return Err(OptionsError::Type {
                        name: known(name),
                        expected: if matches!(name, "max_pending_bytes" | "timeout_ms") {
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
        options.access = match (read_only, recreate) {
            (true, true) => return Err(OptionsError::ReadOnlyRecreate),
            (true, false) => Access::Reader,
            (false, recreate) => Access::Owner { recreate },
        };
        Ok(options)
    }
}

fn known(name: &str) -> &'static str {
    [
        "max_pending_bytes",
        "timeout_ms",
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

#[cfg(test)]
mod tests {
    use super::Access;
    use super::DEFAULT_MAX_PENDING_BYTES;
    use super::OpenOptions;
    use super::OptionsError;
    use crate::ids::Activation;
    use crate::json::SyntaxError;

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
                "token": "t1\"x", "agent": "sdk/1", "read_only": false, "recreate": null,
                "timeout_ms": 60000}"#,
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
        assert_eq!(o.timeout_ms, Some(60_000));
        assert_eq!(o.access, Access::Owner { recreate: false });
        assert_eq!(
            OpenOptions::try_from(r#"{"recreate": true}"#)
                .unwrap()
                .access,
            Access::Owner { recreate: true }
        );
        assert_eq!(
            OpenOptions::try_from(r#"{"read_only": true, "recreate": false}"#)
                .unwrap()
                .access,
            Access::Reader
        );
    }

    #[test]
    fn refusals_name_the_option() {
        let err = |text: &str| OpenOptions::try_from(text).unwrap_err();
        assert!(matches!(
            err("[]"),
            OptionsError::Syntax(SyntaxError { at: 0, .. })
        ));
        assert!(matches!(err("{\"a\":1"), OptionsError::Syntax(_)));
        assert!(matches!(err("{} x"), OptionsError::Syntax(_)));
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
        assert!(matches!(err(r#"{"timeout_ms": 0}"#), OptionsError::Type {
            name: "timeout_ms",
            ..
        }));
        assert!(matches!(
            err(r#"{"activation": "abc"}"#),
            OptionsError::Type {
                name: "activation",
                ..
            }
        ));
        // The import's activation is no open's.
        assert_eq!(
            err(r#"{"activation": "757273756c612d696d706f72742d7631"}"#),
            OptionsError::ReservedActivation("757273756c612d696d706f72742d7631".to_owned())
        );
        // A reader claims nothing, so it cannot create the stream again.
        assert_eq!(
            err(r#"{"read_only": true, "recreate": true}"#),
            OptionsError::ReadOnlyRecreate
        );
        assert!(matches!(err(r#"{"read_only": 1}"#), OptionsError::Type {
            name: "read_only",
            ..
        }));
        assert!(matches!(err(r#"{"recreate": 1}"#), OptionsError::Type {
            name: "recreate",
            ..
        }));
    }
}
