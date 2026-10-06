//! Identity preconditions for the administrative HTTP protocol.

use serde::Deserialize;
use serde::Serialize;

/// A mutation must carry the incarnation observed before its maintenance plan.
pub const PROCESS_INCARNATION_HEADER: &str = "x-ursula-process-incarnation";

/// A fresh identity for one server instance, shared by its HTTP listeners.
/// This is an identity precondition, not a credential or maintenance lease.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProcessIncarnation(String);

impl ProcessIncarnation {
    pub fn from_bits(bits: u128) -> Self {
        Self(format!("{bits:032x}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProcessIncarnation {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err("process incarnation must be 32 lowercase hexadecimal characters");
        }
        Ok(Self(value))
    }
}

impl From<ProcessIncarnation> for String {
    fn from(value: ProcessIncarnation) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessIncarnation;

    #[test]
    fn identity_is_canonical_and_rejects_ambiguous_input() {
        let identity = ProcessIncarnation::from_bits(0xabcd);
        assert_eq!(identity.as_str(), "0000000000000000000000000000abcd");
        assert_eq!(
            ProcessIncarnation::try_from(identity.as_str().to_owned()),
            Ok(identity)
        );
        for invalid in [
            "",
            "abcd",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "\"00000000000000000000000000000000",
            "g0000000000000000000000000000000",
        ] {
            assert!(ProcessIncarnation::try_from(invalid.to_owned()).is_err());
        }
    }
}
