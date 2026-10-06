//! Identity preconditions for the administrative HTTP protocol.

use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

/// A mutation must carry the incarnation observed before its maintenance plan.
pub const PROCESS_INCARNATION_HEADER: &str = "x-ursula-process-incarnation";

/// The immutable executor token admitted by the cell's maintenance reservation.
pub const MAINTENANCE_FENCE_HEADER: &str = "x-ursula-maintenance-fence";

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
        if !canonical_identity(&value) {
            return Err("process incarnation must be 32 lowercase hexadecimal characters");
        }
        Ok(Self(value))
    }
}

fn canonical_identity(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// One admitted executor of a maintenance operation. Generations increase
/// across both takeovers and subsequent operations for the entire cell.
/// This token supplies ordering, not authentication or a reservation store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MaintenanceFenceFields")]
pub struct MaintenanceFence {
    reservation_id: String,
    executor_id: String,
    generation: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceFenceFields {
    reservation_id: String,
    executor_id: String,
    generation: u64,
}

impl MaintenanceFence {
    pub fn new(
        reservation_id: String,
        executor_id: String,
        generation: u64,
    ) -> Result<Self, &'static str> {
        if !canonical_identity(&reservation_id) || !canonical_identity(&executor_id) {
            return Err("maintenance identities must be 32 lowercase hexadecimal characters");
        }
        if generation == 0 {
            return Err("maintenance generation must be nonzero");
        }
        Ok(Self {
            reservation_id,
            executor_id,
            generation,
        })
    }

    pub fn reservation_id(&self) -> &str {
        &self.reservation_id
    }

    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn header_value(&self) -> String {
        format!(
            "{}:{}:{}",
            self.reservation_id, self.generation, self.executor_id
        )
    }
}

impl TryFrom<MaintenanceFenceFields> for MaintenanceFence {
    type Error = &'static str;

    fn try_from(value: MaintenanceFenceFields) -> Result<Self, Self::Error> {
        Self::new(value.reservation_id, value.executor_id, value.generation)
    }
}

impl FromStr for MaintenanceFence {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split(':');
        let reservation = parts.next().ok_or("missing reservation identity")?;
        let generation_text = parts.next().ok_or("missing maintenance generation")?;
        let executor = parts.next().ok_or("missing executor identity")?;
        let generation = generation_text
            .parse::<u64>()
            .map_err(|_invalid| "invalid maintenance generation")?;
        if parts.next().is_some() || generation.to_string() != generation_text {
            return Err("maintenance fence header is not canonical");
        }
        Self::new(reservation.to_owned(), executor.to_owned(), generation)
    }
}

/// Process-local executor admission. Retirement retains the generation so a
/// delayed activation cannot reopen the released executor's authority.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MaintenanceFenceState {
    /// No reservation protocol has been installed; this is uncertified.
    #[default]
    Unclaimed,
    /// The persistent ownership protocol is installed, with no completed
    /// reservation yet. Mutations remain closed until an executor activates.
    AwaitingReservation,
    Active {
        fence: MaintenanceFence,
    },
    Activating {
        fence: MaintenanceFence,
    },
    Retiring {
        fence: MaintenanceFence,
    },
    Retired {
        fence: MaintenanceFence,
    },
}

impl MaintenanceFenceState {
    pub fn fence(&self) -> Option<&MaintenanceFence> {
        match self {
            Self::Unclaimed | Self::AwaitingReservation => None,
            Self::Active { fence }
            | Self::Activating { fence }
            | Self::Retiring { fence }
            | Self::Retired { fence } => Some(fence),
        }
    }
}

/// A trusted startup helper's acknowledged ownership, loaded before listeners.
/// The server generates the incarnation; the helper must return it unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupAdmission {
    pub process_incarnation: ProcessIncarnation,
    pub maintenance_fence: MaintenanceFenceState,
}

impl StartupAdmission {
    /// Startup never grants active mutation authority or uncertified admission.
    pub fn validate(&self) -> Result<(), &'static str> {
        match self.maintenance_fence {
            MaintenanceFenceState::AwaitingReservation
            | MaintenanceFenceState::Activating { .. }
            | MaintenanceFenceState::Retired { .. } => Ok(()),
            _ => Err("startup admission must retain closed maintenance authority"),
        }
    }

    pub fn start_maintenance_drained(&self) -> bool {
        matches!(
            self.maintenance_fence,
            MaintenanceFenceState::Activating { .. }
        )
    }
}

impl From<ProcessIncarnation> for String {
    fn from(value: ProcessIncarnation) -> Self {
        value.0
    }
}

#[cfg(test)]
#[expect(
    clippy::assertions_on_result_states,
    reason = "pre-existing result-state assertion debt; see Known debt in AGENTS.md"
)]
mod tests {
    use super::MaintenanceFence;
    use super::ProcessIncarnation;

    #[test]
    fn fence_header_rejects_ambiguous_generations_and_identities() {
        let id = "00000000000000000000000000000001";
        let fence = MaintenanceFence::new(id.to_owned(), id.to_owned(), u64::MAX).unwrap();
        assert_eq!(fence.header_value().parse::<MaintenanceFence>(), Ok(fence));
        for generation in ["0", "01", "+1", "-1", " 1", "18446744073709551616", ""] {
            assert!(
                format!("{id}:{generation}:{id}")
                    .parse::<MaintenanceFence>()
                    .is_err()
            );
        }
        for value in [
            format!("{id}:1"),
            format!("{id}:1:{id}:extra"),
            format!("bad:1:{id}"),
            format!("{id}:1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        ] {
            assert!(value.parse::<MaintenanceFence>().is_err());
        }
    }

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
