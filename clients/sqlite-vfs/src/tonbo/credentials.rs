//! The login `tonbo login` saves, read as the Tonbo CLI's credentials contract, version 1, tells a
//! reader to (`https://tonbo.dev/contracts/cli/credentials-v1.json`, `reader`).
//!
//! - The file is `${XDG_CONFIG_HOME}/tonbo/credentials.json` when `XDG_CONFIG_HOME` is set and not
//!   empty, else `${HOME}/.config/tonbo/credentials.json` ([`path`]).
//! - No file is no login. A symlink, a file that is not a regular file, and a file any group or
//!   other permission bit (`0077`) is set on are refused and never read, with no other source
//!   tried. The path is checked with `lstat`, opened with `O_NOFOLLOW | O_NONBLOCK` (a symlink put
//!   in its place since is refused, a FIFO never blocks the open), and checked again on the
//!   descriptor.
//! - Content that does not match the schema (`version` 1, a non-empty `account_id` and `key_id`, a
//!   `key` that starts with `tbo_`, and `expires_at` an RFC 3339 date-time or `null`) is unusable,
//!   and a login whose `expires_at` is at or before now is expired. Members the schema does not
//!   name are skipped.
//!
//! Neither the key nor the file's content is ever printed: no error carries them.

use std::fmt;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;

use crate::error::ErrorKind;
use crate::json::Value;
use crate::json::parse_members;
use crate::json::whole;

/// The fix for a login that is missing, unusable or expired.
pub(crate) const LOGIN_FIX: &str = "Run: tonbo login";

/// The largest login file read: a login is a few hundred bytes.
const LIMIT: u64 = 64 << 10;

/// Where `tonbo login` saves the login, `None` when neither `XDG_CONFIG_HOME` nor a home
/// directory is known.
pub(crate) fn path() -> Option<PathBuf> {
    let set = |name: &str| {
        std::env::var_os(name)
            .filter(|v| !v.to_string_lossy().trim().is_empty())
            .map(PathBuf::from)
    };
    let base = set("XDG_CONFIG_HOME").or_else(|| {
        set("HOME")
            .or_else(std::env::home_dir)
            .map(|home| home.join(".config"))
    })?;
    Some(base.join("tonbo").join("credentials.json"))
}

/// Why a login file is refused unread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Refusal {
    #[error("is a symlink")]
    Symlink,
    #[error("is not a regular file")]
    NotRegular,
    /// Some of the bits `0077`: the file's permission bits are `mode`.
    #[error("has group or other permissions (mode {mode:o})")]
    OpenToOthers { mode: u32 },
}

/// Why the login file gave no key.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LoginError {
    #[error("the tonbo login at {} {why}, so it is not read", .path.display())]
    Refused { path: PathBuf, why: Refusal },
    #[error("the tonbo login at {} is not one this extension reads", .path.display())]
    Unusable { path: PathBuf },
    #[error("the tonbo login at {} expired at {at}", .path.display())]
    Expired { path: PathBuf, at: String },
    #[error("cannot read the tonbo login at {}: {source}", .path.display())]
    Unreadable {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl LoginError {
    /// The kind a host reports.
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            LoginError::Refused { .. } => ErrorKind::CredentialsRefused,
            LoginError::Unusable { .. } | LoginError::Expired { .. } => ErrorKind::InvalidApiKey,
            LoginError::Unreadable { .. } => ErrorKind::LocalSetup,
        }
    }

    /// What to do about it: the command to run.
    pub(crate) fn fix(&self) -> String {
        match self {
            LoginError::Refused {
                why: Refusal::Symlink,
                ..
            } => "Replace it with the file itself, or remove it, then run: tonbo login".to_owned(),
            LoginError::Refused {
                why: Refusal::NotRegular,
                ..
            } => "Remove it, then run: tonbo login".to_owned(),
            LoginError::Refused {
                path,
                why: Refusal::OpenToOthers { .. },
            } => format!("Run: chmod 600 {}", ShellWord(path)),
            LoginError::Unusable { .. } | LoginError::Expired { .. } => LOGIN_FIX.to_owned(),
            LoginError::Unreadable { path, .. } => {
                format!("Run: chmod 600 {}, or run: tonbo login", ShellWord(path))
            }
        }
    }
}

/// A path as one shell word: as it is when it needs no quoting, single-quoted otherwise.
struct ShellWord<'a>(&'a Path);

impl fmt::Display for ShellWord<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = self.0.to_string_lossy();
        let plain = !text.is_empty()
            && text
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "_@%+=:,./-".contains(c));
        if plain {
            f.write_str(&text)
        } else {
            write!(f, "'{}'", text.replace('\'', "'\\''"))
        }
    }
}

/// The key of the login file at `path` at `now_ms` (milliseconds since the Unix epoch), `None`
/// when there is no file.
pub(crate) fn read(path: &Path, now_ms: i64) -> Result<Option<String>, LoginError> {
    let refused = |why| LoginError::Refused {
        path: path.to_owned(),
        why,
    };
    let unreadable = |source| LoginError::Unreadable {
        path: path.to_owned(),
        source,
    };
    match std::fs::symlink_metadata(path) {
        Ok(entry) if entry.file_type().is_symlink() => return Err(refused(Refusal::Symlink)),
        Ok(entry) if !entry.file_type().is_file() => return Err(refused(Refusal::NotRegular)),
        Ok(_) => {}
        Err(e) if missing(&e) => return Ok(None),
        Err(e) => return Err(unreadable(e)),
    }
    // O_NOFOLLOW: a symlink put in place since the check is refused, not followed. O_NONBLOCK: a
    // FIFO put in place never holds the open.
    let opened = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path);
    let file = match opened {
        Ok(file) => file,
        Err(e) if e.raw_os_error() == Some(libc::ELOOP) => return Err(refused(Refusal::Symlink)),
        Err(e) => return Err(unreadable(e)),
    };
    let meta = file.metadata().map_err(unreadable)?;
    if !meta.file_type().is_file() {
        return Err(refused(Refusal::NotRegular));
    }
    if (meta.mode() & 0o077) != 0 {
        return Err(refused(Refusal::OpenToOthers {
            mode: meta.mode() & 0o777,
        }));
    }
    let mut bytes = Vec::new();
    file.take(LIMIT.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(unreadable)?;
    let unusable = || LoginError::Unusable {
        path: path.to_owned(),
    };
    let text = String::from_utf8(bytes).map_err(|_not_utf8| unusable())?;
    let login = parse(&text).ok_or_else(unusable)?;
    match login.expires_at {
        Expiry::At { ms, text } if ms <= now_ms => Err(LoginError::Expired {
            path: path.to_owned(),
            at: text,
        }),
        Expiry::Never | Expiry::At { .. } => Ok(Some(login.key)),
    }
}

/// A path that names nothing: the file, or a directory on its way, is missing.
fn missing(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound || e.raw_os_error() == Some(libc::ENOTDIR)
}

/// A login that matches the schema.
#[derive(Debug, PartialEq, Eq)]
struct Login {
    key: String,
    expires_at: Expiry,
}

/// When a login expires.
#[derive(Debug, PartialEq, Eq)]
enum Expiry {
    /// `expires_at: null`.
    Never,
    /// An RFC 3339 date-time, `ms` after the Unix epoch.
    At { ms: i64, text: String },
}

/// The login in `text`, when it is version 1 and matches its schema.
fn parse(text: &str) -> Option<Login> {
    let (mut version, mut account_id, mut key_id, mut key, mut expires_at) =
        (None, None, None, None, None);
    for (name, value) in parse_members(text).ok()? {
        match name.as_str() {
            "version" => version = Some(value),
            "account_id" => account_id = Some(value),
            "key_id" => key_id = Some(value),
            "key" => key = Some(value),
            "expires_at" => expires_at = Some(value),
            _ => {}
        }
    }
    let id = |value: Option<Value>| matches!(value, Some(Value::Text(t)) if !t.is_empty());
    let version_1 = matches!(version, Some(Value::Number(n)) if whole(n) == Some(1));
    if !version_1 || !id(account_id) || !id(key_id) {
        return None;
    }
    let key = match key {
        Some(Value::Text(key))
            if key.starts_with("tbo_")
                && crate::auth::usable(&key).as_deref() == Some(key.as_str()) =>
        {
            key
        }
        _ => return None,
    };
    let expires_at = match expires_at? {
        Value::Null => Expiry::Never,
        Value::Text(text) => Expiry::At {
            ms: date_time_ms(&text)?,
            text,
        },
        Value::Bool(_) | Value::Number(_) => return None,
    };
    Some(Login { key, expires_at })
}

/// An RFC 3339 date-time (`2027-04-24T00:00:00.000Z`, `2027-04-24t02:00:00+02:00`) in
/// milliseconds since the Unix epoch, fractions past milliseconds dropped: `None` for anything
/// else.
fn date_time_ms(text: &str) -> Option<i64> {
    let b = text.as_bytes();
    let field = |from: usize, len: usize| -> Option<i64> {
        let digits = b.get(from..from.checked_add(len)?)?;
        digits.iter().try_fold(0_i64, |n, &d| {
            d.is_ascii_digit()
                .then(|| {
                    n.checked_mul(10)?
                        .checked_add(i64::from(d.checked_sub(b'0')?))
                })
                .flatten()
        })
    };
    let at = |i: usize, c: u8| b.get(i).is_some_and(|x| x.eq_ignore_ascii_case(&c));
    if !(at(4, b'-') && at(7, b'-') && at(10, b'T') && at(13, b':') && at(16, b':')) {
        return None;
    }
    let (year, month, day) = (field(0, 4)?, field(5, 2)?, field(8, 2)?);
    let (hour, minute, second) = (field(11, 2)?, field(14, 2)?, field(17, 2)?);
    if !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    // The fraction: one digit or more, of which milliseconds count.
    let mut i = 19_usize;
    let mut ms = 0_i64;
    if at(i, b'.') {
        i = i.checked_add(1)?;
        let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            if i < start.checked_add(3)? {
                let d = i64::from(b.get(i)?.checked_sub(b'0')?);
                ms = ms.checked_mul(10)?.checked_add(d)?;
            }
            i = i.checked_add(1)?;
        }
        let read = i.checked_sub(start)?;
        if read == 0 {
            return None;
        }
        for _ in read..3 {
            ms = ms.checked_mul(10)?;
        }
    }
    // The offset: `Z`, or `+hh:mm` or `-hh:mm`, and nothing after it.
    let offset_minutes = if at(i, b'Z') && b.len() == i.checked_add(1)? {
        0
    } else {
        let sign = match b.get(i) {
            Some(b'+') => 1_i64,
            Some(b'-') => -1_i64,
            _ => return None,
        };
        let (h, m) = (field(i.checked_add(1)?, 2)?, field(i.checked_add(4)?, 2)?);
        if !at(i.checked_add(3)?, b':') || b.len() != i.checked_add(6)? || h > 23 || m > 59 {
            return None;
        }
        sign.checked_mul(h.checked_mul(60)?.checked_add(m)?)?
    };
    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour.checked_mul(3_600)?)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?
        .checked_sub(offset_minutes.checked_mul(60)?)?;
    seconds.checked_mul(1_000)?.checked_add(ms)
}

/// The days in `month` of `year`, proleptic Gregorian.
fn days_in_month(year: i64, month: i64) -> i64 {
    let leap = year.checked_rem(4) == Some(0)
        && (year.checked_rem(100) != Some(0) || year.checked_rem(400) == Some(0));
    match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// The days from 1970-01-01 to `year-month-day` (Howard Hinnant's `days_from_civil`), for a year
/// from 0 to 9999.
fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    let y = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = (if y >= 0 { y } else { y.checked_sub(399)? }).checked_div(400)?;
    let yoe = y.checked_sub(era.checked_mul(400)?)?;
    let shifted = if month > 2 {
        month.checked_sub(3)?
    } else {
        month.checked_add(9)?
    };
    let doy = shifted
        .checked_mul(153)?
        .checked_add(2)?
        .checked_div(5)?
        .checked_add(day)?
        .checked_sub(1)?;
    let doe = yoe
        .checked_mul(365)?
        .checked_add(yoe.checked_div(4)?)?
        .checked_sub(yoe.checked_div(100)?)?
        .checked_add(doy)?;
    era.checked_mul(146_097)?
        .checked_add(doe)?
        .checked_sub(719_468)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::fs::symlink;
    use std::path::Path;

    use super::Expiry;
    use super::LoginError;
    use super::Refusal;
    use super::date_time_ms;
    use super::parse;
    use super::read;
    use crate::error::ErrorKind;
    use crate::testdir::TempDir;

    /// The contract's `examples.usable`, as it lists them.
    const USABLE: [&str; 2] = [
        r#"{"version": 1, "account_id": "10000000-0000-4000-8000-000000000001", "key_id": "20000000-0000-4000-8000-000000000002", "key": "tbo_secret", "expires_at": "2027-04-24T00:00:00.000Z"}"#,
        r#"{"version": 1, "account_id": "10000000-0000-4000-8000-000000000001", "key_id": "20000000-0000-4000-8000-000000000002", "key": "tbo_secret", "expires_at": null}"#,
    ];

    /// The contract's `examples.unusable`, as it lists them.
    const UNUSABLE: [&str; 6] = [
        r#"{"access_token": "access", "refresh_token": "refresh", "token_type": "bearer"}"#,
        r#"{"version": 2, "account_id": "10000000-0000-4000-8000-000000000001", "key_id": "20000000-0000-4000-8000-000000000002", "key": "tbo_secret", "expires_at": null}"#,
        r#"{"version": 1, "account_id": "10000000-0000-4000-8000-000000000001", "key_id": "20000000-0000-4000-8000-000000000002", "key": "not-a-personal-key", "expires_at": null}"#,
        r#"{"version": 1, "account_id": "", "key_id": "20000000-0000-4000-8000-000000000002", "key": "tbo_secret", "expires_at": null}"#,
        r#"{"version": 1, "account_id": "10000000-0000-4000-8000-000000000001", "key_id": "20000000-0000-4000-8000-000000000002", "key": "tbo_secret", "expires_at": "not a date"}"#,
        r#"{"version": 1, "account_id": "10000000-0000-4000-8000-000000000001", "key_id": "20000000-0000-4000-8000-000000000002", "key": "tbo_secret"}"#,
    ];

    /// 2027-04-24T00:00:00.000Z.
    const EXPIRY_MS: i64 = 1_808_524_800_000;

    fn write(path: &Path, text: &str, mode: u32) {
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
        fs::write(path, text).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    // The contract's usable examples give their key, the one with an `expires_at` until then; its
    // unusable examples, and members of another type, give none.
    #[test]
    fn the_contracts_examples_are_read_as_it_lists_them() {
        let first = parse(USABLE[0]).unwrap();
        assert_eq!(first.key, "tbo_secret");
        assert_eq!(first.expires_at, Expiry::At {
            ms: EXPIRY_MS,
            text: "2027-04-24T00:00:00.000Z".to_owned(),
        });
        assert_eq!(parse(USABLE[1]).unwrap().expires_at, Expiry::Never);
        for text in UNUSABLE {
            assert_eq!(parse(text), None, "{text}");
        }
        let with = |member: &str| {
            USABLE[1].replace(
                r#""expires_at": null"#,
                &format!(r#""expires_at": null, {member}"#),
            )
        };
        assert!(
            parse(&with(r#""extra": {"nested": [1]}"#)).is_some(),
            "other members are skipped"
        );
        for wrong in [
            r#""version": "1""#,
            r#""version": 1.5"#,
            r#""key": "tbo_two words""#,
            r#""key_id": 2"#,
        ] {
            // A later member replaces the earlier one of its name.
            assert_eq!(parse(&with(wrong)), None, "{wrong}");
        }
        assert!(parse(&with(r#""version": 1.0"#)).is_some());
        assert_eq!(parse("not json"), None);
        assert_eq!(parse("[]"), None);
    }

    // RFC 3339 date-times in milliseconds, offsets applied: anything else is not a date.
    #[test]
    fn expires_at_is_an_rfc_3339_date_time() {
        assert_eq!(date_time_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(date_time_ms("2027-04-24T00:00:00.000Z"), Some(EXPIRY_MS));
        assert_eq!(date_time_ms("2027-04-24t02:00:00+02:00"), Some(EXPIRY_MS));
        assert_eq!(date_time_ms("2027-04-23T23:30:00-00:30"), Some(EXPIRY_MS));
        assert_eq!(
            date_time_ms("2027-04-24T00:00:00.5Z"),
            Some(EXPIRY_MS.saturating_add(500))
        );
        assert_eq!(
            date_time_ms("2027-04-24T00:00:00.123456z"),
            Some(EXPIRY_MS.saturating_add(123))
        );
        assert_eq!(
            date_time_ms("2024-02-29T00:00:00Z"),
            Some(1_709_164_800_000)
        );
        assert_eq!(date_time_ms("1969-12-31T23:59:59Z"), Some(-1_000));
        for wrong in [
            "not a date",
            "",
            "2027-04-24",
            "2027-04-24 00:00:00Z",
            "2027-04-24T00:00:00",
            "2027-04-24T00:00:00.Z",
            "2027-04-24T00:00:00Zx",
            "2027-04-24T00:00:00+0200",
            "2027-13-01T00:00:00Z",
            "2027-02-29T00:00:00Z",
            "2027-04-31T00:00:00Z",
            "2027-04-24T24:00:00Z",
            "2027-04-24T00:60:00Z",
            "2027-04-24T00:00:60Z",
            "2027-04-24T00:00:00+24:00",
            "+027-04-24T00:00:00Z",
        ] {
            assert_eq!(date_time_ms(wrong), None, "{wrong}");
        }
    }

    // The file as the contract's reader reads it: no file is no login, a usable one gives its key
    // until it expires, and a symlink, a directory or a file open to group or others is refused
    // unread.
    #[test]
    fn the_login_file_is_read_as_the_contract_says() {
        let dir = TempDir::new("tonbo-login");
        let path = dir.path().join("credentials.json");
        assert!(read(&path, 0).unwrap().is_none(), "no file");
        assert!(
            read(&path.join("below"), 0).unwrap().is_none(),
            "a file on the way"
        );

        write(&path, USABLE[0], 0o600);
        assert_eq!(
            read(&path, EXPIRY_MS.saturating_sub(1)).unwrap().as_deref(),
            Some("tbo_secret")
        );
        let expired = read(&path, EXPIRY_MS).unwrap_err();
        assert!(
            matches!(&expired, LoginError::Expired { at, .. } if at == "2027-04-24T00:00:00.000Z")
        );
        assert_eq!(expired.kind(), ErrorKind::InvalidApiKey);
        assert_eq!(expired.fix(), "Run: tonbo login");
        write(&path, USABLE[1], 0o400);
        assert_eq!(
            read(&path, i64::MAX).unwrap().as_deref(),
            Some("tbo_secret")
        );
        for text in UNUSABLE {
            write(&path, text, 0o600);
            let e = read(&path, 0).unwrap_err();
            assert!(matches!(e, LoginError::Unusable { .. }), "{text}");
            assert_eq!(e.kind(), ErrorKind::InvalidApiKey);
            assert!(!e.to_string().contains("tbo_secret"));
        }

        for mode in [0o640, 0o604, 0o601, 0o700 | 0o020] {
            write(&path, USABLE[1], mode);
            let e = read(&path, 0).unwrap_err();
            assert!(
                matches!(e, LoginError::Refused { why: Refusal::OpenToOthers { mode: m }, .. } if m == mode),
                "{mode:o}"
            );
            assert_eq!(e.kind(), ErrorKind::CredentialsRefused);
            assert_eq!(e.fix(), format!("Run: chmod 600 {}", path.display()));
        }
        fs::remove_file(&path).unwrap();

        let target = dir.path().join("elsewhere.json");
        write(&target, USABLE[1], 0o600);
        symlink(&target, &path).unwrap();
        let e = read(&path, 0).unwrap_err();
        assert!(matches!(e, LoginError::Refused {
            why: Refusal::Symlink,
            ..
        }));
        assert_eq!(e.kind(), ErrorKind::CredentialsRefused);
        fs::remove_file(&path).unwrap();

        fs::create_dir(&path).unwrap();
        let e = read(&path, 0).unwrap_err();
        assert!(matches!(e, LoginError::Refused {
            why: Refusal::NotRegular,
            ..
        }));
        assert_eq!(e.fix(), "Remove it, then run: tonbo login");
    }

    // A path the shell would split or expand is quoted in the fix.
    #[test]
    fn the_fix_quotes_a_path_the_shell_would_split() {
        let e = |path: &str| LoginError::Refused {
            path: path.into(),
            why: Refusal::OpenToOthers { mode: 0o644 },
        };
        assert_eq!(
            e("/home/a/.config/tonbo/credentials.json").fix(),
            "Run: chmod 600 /home/a/.config/tonbo/credentials.json"
        );
        assert_eq!(
            e("/home/it's me/c.json").fix(),
            r"Run: chmod 600 '/home/it'\''s me/c.json'"
        );
        assert_eq!(
            e("/tmp/x").to_string(),
            "the tonbo login at /tmp/x has group or other permissions (mode 644), so it is not read"
        );
    }
}
