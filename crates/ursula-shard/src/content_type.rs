//! Content-type normalization used by the node.
//!
//! A stream's content type is stored normalized (see `api/append.mdx`,
//! Content-Type): split at `;`, trim each part, drop empty parts,
//! ASCII-lowercase each part and join with `; `.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_trims_lowercases_and_joins() {
        assert_eq!(
            normalize_content_type(" Application/JSON ;Charset=UTF-8; "),
            "application/json; charset=utf-8"
        );
        assert_eq!(normalize_content_type("text/plain"), "text/plain");
        assert_eq!(normalize_content_type(";;"), "");
    }
}
