//! Content-type normalization and profile detection shared by the node, the
//! gateway and the indexer.
//!
//! A stream's content type is stored normalized (`extensions.md` §1.8): split
//! at `;`, trim each part, drop empty parts, ASCII-lowercase each part and
//! join with `; `. A stream is a keyed stream (`extensions.md` §9.1.1) if and
//! only if its normalized content type equals [`KEYED_BATCH_CONTENT_TYPE`]
//! exactly; a quoted profile value or any additional parameter does not
//! activate the extension.

/// Profile token of the `keyed-batch-v1` record format.
pub const KEYED_BATCH_PROFILE: &str = "keyed-batch-v1";

/// Normalized activation content type of keyed streams.
pub const KEYED_BATCH_CONTENT_TYPE: &str = "application/json; profile=keyed-batch-v1";

const JSON_MEDIA_TYPE: &str = "application/json";

/// Normalizes a content type: split at `;`, trim, drop empty parts,
/// ASCII-lowercase each part and join with `; `.
pub fn normalize_content_type(value: &str) -> String {
    value
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .join("; ")
}

/// The JSON profile a content type activates: the value of its `profile`
/// parameter when the content type normalizes to exactly
/// `application/json; profile=<value>`. Any other media type, any other or
/// additional parameter, or an empty value yields `None`. The value is
/// returned as written after normalization, so a quoted value keeps its
/// quotes and never names a known profile.
pub fn profile_of(content_type: &str) -> Option<String> {
    let normalized = normalize_content_type(content_type);
    let (media_type, parameter) = normalized.split_once("; ")?;
    if media_type != JSON_MEDIA_TYPE {
        return None;
    }
    let value = parameter.strip_prefix("profile=")?;
    if value.is_empty() || value.contains("; ") {
        return None;
    }
    Some(value.to_owned())
}

/// Whether a content type activates `keyed-batch-v1` (`extensions.md`
/// §9.1.1): its normalized form equals [`KEYED_BATCH_CONTENT_TYPE`].
pub fn is_keyed_batch_content_type(content_type: &str) -> bool {
    profile_of(content_type).as_deref() == Some(KEYED_BATCH_PROFILE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_trims_lowercases_and_joins() {
        assert_eq!(
            normalize_content_type(" Application/JSON ;Profile=Keyed-Batch-V1; "),
            KEYED_BATCH_CONTENT_TYPE
        );
        assert_eq!(normalize_content_type("text/plain"), "text/plain");
        assert_eq!(normalize_content_type(";;"), "");
    }

    #[test]
    fn keyed_activation_is_exact_after_normalization() {
        for activating in [
            KEYED_BATCH_CONTENT_TYPE,
            "Application/JSON;Profile=keyed-batch-v1",
            "application/json ;  profile=KEYED-BATCH-V1 ;",
        ] {
            assert!(is_keyed_batch_content_type(activating), "{activating}");
        }
        for inert in [
            "application/json",
            "application/json; profile=\"keyed-batch-v1\"",
            "application/json; profile=keyed-batch-v1; charset=utf-8",
            "application/json; charset=utf-8; profile=keyed-batch-v1",
            "application/json; profile=keyed-batch-v2",
            "application/json; profile = keyed-batch-v1",
            "text/plain; profile=keyed-batch-v1",
            "application/vnd.ursula.keyed-batch+json",
            "",
        ] {
            assert!(!is_keyed_batch_content_type(inert), "{inert}");
        }
    }

    #[test]
    fn profile_of_reads_the_single_profile_parameter() {
        assert_eq!(
            profile_of("application/json; profile=keyed-batch-v1").as_deref(),
            Some(KEYED_BATCH_PROFILE)
        );
        assert_eq!(
            profile_of("application/json; profile=\"x\"").as_deref(),
            Some("\"x\"")
        );
        assert_eq!(profile_of("application/json; profile="), None);
        assert_eq!(profile_of("application/json"), None);
        assert_eq!(profile_of("application/json; charset=utf-8"), None);
    }
}
