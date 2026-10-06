//! Typed server configuration, presets, loading and validation.
//!
//! Module map:
//! - [`config`]: server/runtime/Raft/storage options.
//! - [`control`]: managed bootstrap recipe and durable identity validation.
//! - [`human`]: human-readable sizes and durations.
//! - [`load`]: TOML loading and preset/CLI merging.
//! - [`preset`]: resource presets.
//! - [`validate`]: cross-field validation.

pub mod config;
pub mod control;
pub mod human;
pub mod load;
pub mod preset;
pub mod validate;

pub use config::UrsulaConfig;
pub use config::*;
pub use human::HumanDuration;
pub use human::HumanSize;
pub use load::ConfigError;
pub use load::find_default_config;
pub use load::load_config;
pub use preset::Preset;

#[cfg(test)]
mod tests;

pub use control::ControlConfig;
