//! Log lines on the host's stderr, one event per line in logfmt:
//!
//! ```text
//! sqlite-ursula-vfs level=warn event=poisoned file=/data/app.db stream=http://u/b/s fenced=true reason="fenced: ..."
//! ```
//!
//! A loadable extension has no logger of its host to call, so it writes the lines itself. Values
//! are quoted when they are empty or hold a space, `=`, `"` or a control character.

use std::fmt::Display;
use std::io::Write;

#[derive(Clone, Copy)]
pub(crate) enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}

/// Writes one event to stderr. A line that cannot be written (stderr closed: EPIPE) is dropped:
/// this runs inside SQLite's callbacks, where a panic (`eprintln!`'s) would abort the host.
pub(crate) fn emit(level: Level, event: &str, fields: &[(&str, &dyn Display)]) {
    let line = line(level, event, fields);
    if let Err(_unwritable) = writeln!(std::io::stderr().lock(), "{line}") {}
}

fn line(level: Level, event: &str, fields: &[(&str, &dyn Display)]) -> String {
    let mut out = format!("sqlite-ursula-vfs level={} event={event}", level.as_str());
    for (key, value) in fields {
        out.push(' ');
        out.push_str(key);
        out.push('=');
        out.push_str(&quoted(&value.to_string()));
    }
    out
}

fn quoted(value: &str) -> String {
    let plain = !value.is_empty()
        && !value
            .chars()
            .any(|c| c == ' ' || c == '=' || c == '"' || c.is_control());
    if plain {
        return value.to_owned();
    }
    let mut out = String::from("\"");
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::Level;
    use super::line;

    #[test]
    fn values_are_quoted_only_when_needed() {
        let reason = "fenced: epoch 2 superseded by 3 (403)";
        assert_eq!(
            line(Level::Warn, "poisoned", &[
                ("file", &"/data/app.db"),
                ("fenced", &true),
                ("reason", &reason)
            ]),
            "sqlite-ursula-vfs level=warn event=poisoned file=/data/app.db fenced=true \
             reason=\"fenced: epoch 2 superseded by 3 (403)\""
        );
        assert_eq!(
            line(Level::Info, "x", &[("a", &""), ("b", &"q\"\\\nz")]),
            "sqlite-ursula-vfs level=info event=x a=\"\" b=\"q\\\"\\\\\\nz\""
        );
    }
}
