use thiserror::Error;

use crate::config::ColdCacheConfig;
use crate::config::UrsulaConfig;
use crate::config::WalBackend;
use crate::human::HumanSize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Preset {
    /// Development defaults: memory WAL, single-node, no cold storage.
    /// Used as the implicit preset when no config file and no explicit
    /// `--preset` are given.
    #[default]
    Default,
    Tiny,
    Small,
    Standard,
    Large,
}

#[derive(Debug, Error)]
#[error("unknown preset '{0}'")]
pub struct PresetParseError(String);

impl std::str::FromStr for Preset {
    type Err = PresetParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "default" | "dev" | "" => Ok(Preset::Default),
            "tiny" => Ok(Preset::Tiny),
            "small" => Ok(Preset::Small),
            "standard" => Ok(Preset::Standard),
            "large" => Ok(Preset::Large),
            other => Err(PresetParseError(other.to_string())),
        }
    }
}

impl From<Preset> for UrsulaConfig {
    fn from(preset: Preset) -> Self {
        let mut config = Self::default();
        match preset {
            Preset::Default => {
                config.raft.wal.backend = WalBackend::Memory;
                config.raft.node_id = 1;
            }
            Preset::Tiny => {
                config.runtime.live_read_max_waiters_per_core = Some(8_192);
                config.raft.group_count = 64;
                config.raft.max_uncommitted_size_per_group = Some(HumanSize::mib(8));
                config.raft.wal.backend = WalBackend::Memory;
                config.server.http_inflight_body_size = HumanSize::mib(64);
                config.storage.cold.flush_size = HumanSize::mib(4);
                config.storage.cold.flush_max_concurrency = 2;
                config.storage.cold.max_hot_size_per_group = Some(HumanSize::mib(8));
                config.storage.cold.cache = Some(ColdCacheConfig {
                    max_size: HumanSize::mib(64),
                    block_size: HumanSize::mib(1),
                    readahead_blocks: 4,
                });
            }
            Preset::Small => {
                config.runtime.live_read_max_waiters_per_core = Some(8_192);
                config.raft.group_count = 128;
                config.raft.max_uncommitted_size_per_group = Some(HumanSize::mib(16));
                config.server.http_inflight_body_size = HumanSize::mib(64);
                config.storage.cold.flush_size = HumanSize::mib(4);
                config.storage.cold.flush_max_concurrency = 2;
                config.storage.cold.max_hot_size_per_group = Some(HumanSize::mib(16));
                config.storage.cold.cache = Some(ColdCacheConfig {
                    max_size: HumanSize::mib(64),
                    block_size: HumanSize::mib(1),
                    readahead_blocks: 4,
                });
            }
            Preset::Standard => {
                config.runtime.live_read_max_waiters_per_core = Some(65_536);
                config.raft.group_count = 256;
                config.raft.max_uncommitted_size_per_group = Some(HumanSize::mib(64));
                config.server.http_inflight_body_size = HumanSize::mib(256);
                config.storage.cold.flush_size = HumanSize::mib(8);
                config.storage.cold.flush_max_concurrency = 4;
                config.storage.cold.max_hot_size_per_group = Some(HumanSize::mib(64));
                config.storage.cold.cache = Some(ColdCacheConfig {
                    max_size: HumanSize::mib(256),
                    block_size: HumanSize::mib(1),
                    readahead_blocks: 4,
                });
            }
            Preset::Large => {
                config.runtime.live_read_max_waiters_per_core = Some(131_072);
                config.raft.group_count = 512;
                config.raft.max_uncommitted_size_per_group = Some(HumanSize::mib(128));
                config.server.http_inflight_body_size = HumanSize::mib(512);
                config.storage.cold.flush_size = HumanSize::mib(16);
                config.storage.cold.flush_max_concurrency = 8;
                config.storage.cold.max_hot_size_per_group = Some(HumanSize::mib(128));
                config.storage.cold.cache = Some(ColdCacheConfig {
                    max_size: HumanSize::mib(512),
                    block_size: HumanSize::mib(1),
                    readahead_blocks: 4,
                });
            }
        }
        config
    }
}
