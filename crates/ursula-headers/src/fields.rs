//! One pass over a header list, keeping the values of a fixed set of names,
//! and the typed reads of those values.

use crate::error::HeaderError;
use crate::value::Incarnation;
use crate::value::Offset;
use crate::value::parse_decimal;

/// The callback a value renders its headers through.
pub(crate) type Emit<'e> = dyn FnMut(&'static str, &str) + 'e;

/// The values of `NAMES`, in the same order, from `headers`. A name that
/// appears twice is [`HeaderError::Duplicate`].
pub(crate) fn collect<'a, I, const N: usize>(
    headers: I,
    names: [&'static str; N],
) -> Result<[Option<&'a [u8]>; N], HeaderError>
where
    I: IntoIterator<Item = (&'a str, &'a [u8])>,
{
    let mut values = [None; N];
    for (name, value) in headers {
        for (wanted, slot) in names.iter().zip(values.iter_mut()) {
            if name.eq_ignore_ascii_case(wanted) {
                if slot.is_some() {
                    return Err(HeaderError::Duplicate { name: wanted });
                }
                *slot = Some(value);
            }
        }
    }
    Ok(values)
}

/// The value as text with optional whitespace trimmed, or `None` when absent.
pub(crate) fn text<'a>(
    name: &'static str,
    value: Option<&'a [u8]>,
) -> Result<Option<&'a str>, HeaderError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !value
        .iter()
        .all(|byte| *byte == b'\t' || (0x20..=0x7e).contains(byte))
    {
        return Err(HeaderError::NotText { name });
    }
    let text = std::str::from_utf8(value).map_err(|_not_utf8| HeaderError::NotText { name })?;
    Ok(Some(text.trim_matches([' ', '\t'])))
}

/// The value parsed by `parse`, or `None` when absent.
pub(crate) fn parsed<'a, T>(
    name: &'static str,
    value: Option<&'a [u8]>,
    parse: impl FnOnce(&'static str, &'a str) -> Result<T, HeaderError>,
) -> Result<Option<T>, HeaderError> {
    text(name, value)?.map(|text| parse(name, text)).transpose()
}

pub(crate) fn offset(
    name: &'static str,
    value: Option<&[u8]>,
) -> Result<Option<Offset>, HeaderError> {
    parsed(name, value, Offset::parse)
}

pub(crate) fn incarnation(
    name: &'static str,
    value: Option<&[u8]>,
) -> Result<Option<Incarnation>, HeaderError> {
    parsed(name, value, Incarnation::parse)
}

pub(crate) fn decimal(
    name: &'static str,
    value: Option<&[u8]>,
) -> Result<Option<u64>, HeaderError> {
    parsed(name, value, parse_decimal)
}

/// A flag: `false` when absent, `true` when exactly `true`.
pub(crate) fn flag(name: &'static str, value: Option<&[u8]>) -> Result<bool, HeaderError> {
    match text(name, value)? {
        None => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(HeaderError::InvalidFlag { name }),
    }
}

/// `value`, or [`HeaderError::Missing`] naming `name`.
pub(crate) fn required<T>(name: &'static str, value: Option<T>) -> Result<T, HeaderError> {
    value.ok_or(HeaderError::Missing { name })
}

/// Two headers that come together: both or neither.
pub(crate) fn pair<A, B>(
    (name_a, a): (&'static str, Option<A>),
    (name_b, b): (&'static str, Option<B>),
) -> Result<Option<(A, B)>, HeaderError> {
    match (a, b) {
        (None, None) => Ok(None),
        (Some(a), Some(b)) => Ok(Some((a, b))),
        (Some(_), None) => Err(HeaderError::Missing { name: name_b }),
        (None, Some(_)) => Err(HeaderError::Missing { name: name_a }),
    }
}
