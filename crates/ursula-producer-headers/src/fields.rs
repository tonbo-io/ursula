//! One pass over a header list, keeping the values of a fixed set of names.

use crate::error::HeaderError;

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
