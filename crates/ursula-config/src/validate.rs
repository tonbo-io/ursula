use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use thiserror::Error;

use crate::config::ColdBackend;
use crate::config::RaftConfig;
use crate::config::RaftSnapshotBackend;
use crate::config::UrsulaConfig;
use crate::config::WalConfig;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("raft.append_transport_budget_bytes must be between 1 and 4294967295")]
    AppendTransportBudget,
    /// A node of a cluster of several keeps its WAL across restarts.
    #[error("raft.wal.path is required when raft.peers lists another node")]
    RaftWalPathRequired,
    /// Every node of a durable deployment names it, so none draws its own.
    #[error("server.cluster_id is required when raft.wal.path is set")]
    ClusterIdRequired,
    /// Every node of a cluster runs the same groups, so none may take the
    /// default of its own host.
    #[error("raft.group_count is required when raft.peers lists another node")]
    GroupCountRequired,
    #[error("storage.cold.s3.bucket is required when cold backend is 's3'")]
    ColdS3BucketRequired,
    /// A node that splits its cluster plane off serves no client route on
    /// a peer `url`, so a leader redirect needs the peer's client URL.
    #[error("raft peer {node_id} needs client_url when server.cluster_listen is set")]
    PeerClientUrlRequired { node_id: u64 },
    /// A `client_url` equal to the peer's `url` names its cluster listener,
    /// which serves no client route.
    #[error(
        "raft peer {node_id} client_url equals its url, which names its cluster listener when \
         server.cluster_listen is set"
    )]
    PeerClientUrlIsRaftUrl { node_id: u64 },
    /// Every node with `raft.wal.path`, a single node included, lists
    /// itself in `raft.peers`, as does any node whose `raft.peers` is set.
    #[error(
        "raft.node_id {0} must be present in raft.peers (every node with raft.wal.path lists \
         itself)"
    )]
    NodeIdNotInPeers(u64),
    /// The meta group's voters at genesis are nodes of `raft.peers`, or
    /// this node alone when it is empty.
    #[error(
        "raft.meta_voters names node {node_id}, which is not in raft.peers (with raft.peers \
         empty, raft.meta_voters may name only raft.node_id)"
    )]
    MetaVoterOutsidePeers { node_id: u64 },
    #[error("raft.wal.segment_size ({segment_size} bytes) must be at least {minimum} bytes")]
    WalSegmentTooSmall { segment_size: u64, minimum: u64 },
    #[error("raft.wal.lease ({lease:?}) must be from {minimum:?} to {maximum:?}")]
    WalLeaseOutOfRange {
        lease: Duration,
        minimum: Duration,
        maximum: Duration,
    },
    #[error("storage.cold.s3.{setting} must be non-zero")]
    ZeroS3HealthSetting { setting: S3HealthSetting },
    /// A zero deadline fails every request for a peer's leadership-shed
    /// state, so every peer would look unable to campaign.
    #[error("governance.leadership_balance.peer_timeout must be non-zero")]
    ZeroPeerTimeout,
    /// The snapshot driver always runs and alone takes each group's
    /// snapshots, so it needs an interval to tick on.
    #[error(
        "storage.snapshot.drive_interval must be non-zero: the snapshot driver always runs and \
         takes every Raft snapshot, so it cannot be turned off"
    )]
    ZeroSnapshotDriveInterval,
    /// No build could run with 0, which used to be taken as 1 without a
    /// word, and every build holds a copy of its group's state.
    #[error(
        "raft.snapshot_build_max_concurrency ({value}) must be between 1 and {}",
        RaftConfig::MAX_SNAPSHOT_BUILD_CONCURRENCY
    )]
    SnapshotBuildConcurrency { value: usize },
    /// A node with a cold backend copies the parts of a snapshot commit
    /// under its in-flight body budget, which must hold that copy and one
    /// request.
    #[error(
        "server.http_inflight_body_size ({budget} bytes) must be at least {minimum} bytes with a \
         cold backend"
    )]
    InflightBodyBudgetTooSmall { budget: u64, minimum: u64 },
    /// A bound on deleted data shorter than eight snapshot installs (#669).
    #[error(
        "raft.deleted_data_max_age ({value:?}) must be at least {minimum:?}, eight times \
         raft.install_snapshot_timeout"
    )]
    DeletedDataMaxAgeTooShort {
        value: std::time::Duration,
        minimum: std::time::Duration,
    },
    /// A follower's install copy, a GET, up to two PUTs and a HEAD of the
    /// snapshot store, each to the end of its timeout and retries, can take
    /// longer than the install it serves. The leader's send would end first.
    #[error(
        "storage.snapshot.s3_timeout and storage.snapshot.s3_max_retries let a follower's \
         install copy (a GET, up to two PUTs and a HEAD) take up to {copy_budget:?}, longer than \
         raft.install_snapshot_timeout ({install_snapshot_timeout:?})"
    )]
    SnapshotCopyOutlastsInstall {
        copy_budget: std::time::Duration,
        install_snapshot_timeout: std::time::Duration,
    },
    /// The meta group's snapshot holds one placement per group and is
    /// always inline, so the group count is bounded.
    #[error(
        "raft.group_count ({group_count}) must be at most {}",
        ursula_proto::placement::MAX_GROUP_COUNT
    )]
    GroupCountAboveMax { group_count: usize },
    #[error("{0}")]
    Other(String),
}

/// A setting of the snapshot-store health gate in `[storage.cold.s3]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3HealthSetting {
    /// `probe_timeout`: a zero deadline fails every probe.
    ProbeTimeout,
    /// `unhealthy_ticks`.
    UnhealthyTicks,
    /// `heal_ticks`.
    HealTicks,
}

impl fmt::Display for S3HealthSetting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ProbeTimeout => "probe_timeout",
            Self::UnhealthyTicks => "unhealthy_ticks",
            Self::HealTicks => "heal_ticks",
        })
    }
}

impl UrsulaConfig {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.raft.append_transport_budget_bytes == 0
            || self.raft.append_transport_budget_bytes > u32::MAX as usize
        {
            return Err(ValidationError::AppendTransportBudget);
        }
        if self.raft.node_id == 0 {
            return Err(ValidationError::Other(
                "raft.node_id is required (use --node-id CLI flag)".into(),
            ));
        }
        let group_count = self.raft.effective_group_count();
        if !u64::try_from(group_count).is_ok_and(ursula_proto::placement::group_count_within_bound)
        {
            return Err(ValidationError::GroupCountAboveMax { group_count });
        }
        // A cluster member keeps its WAL across restarts; only a single
        // node may run on a temporary one.
        if self.raft.lists_another_peer() && self.raft.wal.path.is_none() {
            return Err(ValidationError::RaftWalPathRequired);
        }
        // Every node of a durable deployment names it, so none may draw
        // its own.
        if self.raft.wal.path.is_some() && self.server.cluster_id.is_none() {
            return Err(ValidationError::ClusterIdRequired);
        }
        let minimum = self.raft.wal.min_available_size.as_bytes();
        let resume = self.raft.wal.resume_available_size.as_bytes();
        if minimum > 0 && resume <= minimum {
            return Err(ValidationError::Other(format!(
                "raft.wal.resume_available_size ({resume} bytes) must exceed min_available_size ({minimum} bytes)",
            )));
        }
        let segment_size = self.raft.wal.segment_size.as_bytes();
        if segment_size < WalConfig::MIN_SEGMENT_SIZE {
            return Err(ValidationError::WalSegmentTooSmall {
                segment_size,
                minimum: WalConfig::MIN_SEGMENT_SIZE,
            });
        }
        let lease = self.raft.wal.lease.as_duration();
        if !(WalConfig::MIN_LEASE..=WalConfig::MAX_LEASE).contains(&lease) {
            return Err(ValidationError::WalLeaseOutOfRange {
                lease,
                minimum: WalConfig::MIN_LEASE,
                maximum: WalConfig::MAX_LEASE,
            });
        }
        if self.storage.cold.backend == ColdBackend::S3 {
            let bucket = self
                .storage
                .cold
                .s3
                .as_ref()
                .and_then(|s3| s3.bucket.as_ref());
            if bucket.is_none_or(|bucket| bucket.trim().is_empty()) {
                return Err(ValidationError::ColdS3BucketRequired);
            }
        }
        let budget = self.server.http_inflight_body_size.as_bytes();
        let minimum = crate::config::ServerConfig::MIN_INFLIGHT_BODY_SIZE_WITH_COLD;
        if self.storage.cold.backend != ColdBackend::None && budget < minimum {
            return Err(ValidationError::InflightBodyBudgetTooSmall { budget, minimum });
        }
        self.validate_s3_health_gate()?;
        if self
            .governance
            .leadership_balance
            .peer_timeout
            .as_duration()
            .is_zero()
        {
            return Err(ValidationError::ZeroPeerTimeout);
        }
        if self
            .storage
            .snapshot
            .drive_interval
            .is_some_and(|interval| interval.as_duration().is_zero())
        {
            return Err(ValidationError::ZeroSnapshotDriveInterval);
        }
        self.validate_peers()?;
        self.validate_meta_voters()?;
        self.validate_non_zero_durations()?;
        let builds = self.raft.snapshot_build_max_concurrency;
        if !(1..=RaftConfig::MAX_SNAPSHOT_BUILD_CONCURRENCY).contains(&builds) {
            return Err(ValidationError::SnapshotBuildConcurrency { value: builds });
        }
        // Only a set value is checked: unset, the bound follows the install
        // timeout, so a config without the key stays valid.
        if let Some(value) = self.raft.deleted_data_max_age.map(|age| age.as_duration()) {
            let minimum = self.raft.min_deleted_data_max_age();
            if value < minimum {
                return Err(ValidationError::DeletedDataMaxAgeTooShort { value, minimum });
            }
        }
        self.validate_install_copy_budget()?;
        if self.raft.snapshot_pressure_max_groups_per_tick == 0 {
            return Err(ValidationError::Other(
                "raft.snapshot_pressure_max_groups_per_tick must be non-zero".into(),
            ));
        }
        if self.storage.cold.compaction_target_size.as_bytes()
            > self.storage.cold.compaction_max_size.as_bytes()
        {
            return Err(ValidationError::Other(
                "storage.cold.compaction_target_size must not exceed compaction_max_size".into(),
            ));
        }
        self.validate_cold_health_watermarks()?;
        Ok(())
    }

    fn validate_cold_health_watermarks(&self) -> Result<(), ValidationError> {
        let low = self.governance.cold_health.hot_size_low.as_bytes();
        let high = self.governance.cold_health.hot_size_high.as_bytes();
        let flush_min = self.storage.cold.flush_min_hot_size().as_bytes();
        if low >= high {
            return Err(ValidationError::Other(
                "governance.cold_health.hot_size_low must be lower than hot_size_high".into(),
            ));
        }
        if high <= flush_min {
            return Err(ValidationError::Other(format!(
                "governance.cold_health.hot_size_high ({high} bytes) must exceed storage.cold.flush_min_hot_size ({flush_min} bytes) so normal flushing can start before leadership shedding",
            )));
        }
        if let Some(max_hot) = self.storage.cold.max_hot_size_per_group
            && max_hot.as_bytes() > 0
            && high >= max_hot.as_bytes()
        {
            return Err(ValidationError::Other(format!(
                "governance.cold_health.hot_size_high ({high} bytes) must be lower than storage.cold.max_hot_size_per_group ({} bytes)",
                max_hot.as_bytes(),
            )));
        }
        Ok(())
    }

    /// The snapshot-store health gate's settings must be non-zero. A zero
    /// probe deadline fails every probe, and zero tick counts would read as
    /// one.
    fn validate_s3_health_gate(&self) -> Result<(), ValidationError> {
        let Some(s3) = self.storage.cold.s3.as_ref() else {
            return Ok(());
        };
        for (setting, zero) in [
            (
                S3HealthSetting::ProbeTimeout,
                s3.probe_timeout.as_duration().is_zero(),
            ),
            (S3HealthSetting::UnhealthyTicks, s3.unhealthy_ticks == 0),
            (S3HealthSetting::HealTicks, s3.heal_ticks == 0),
        ] {
            if zero {
                return Err(ValidationError::ZeroS3HealthSetting { setting });
            }
        }
        Ok(())
    }

    /// On a node that installs snapshots through an S3 snapshot store, a
    /// follower's install copy fits inside `raft.install_snapshot_timeout`.
    /// The defaults leave a margin. Every Raft node, a single node
    /// included, uses the store of `storage.snapshot.backend`.
    fn validate_install_copy_budget(&self) -> Result<(), ValidationError> {
        let s3_snapshots = self
            .storage
            .snapshot
            .backend
            .resolve(self.storage.cold.backend)
            == RaftSnapshotBackend::S3;
        if !s3_snapshots {
            return Ok(());
        }
        let copy_budget = self.storage.snapshot.install_copy_budget();
        let install_snapshot_timeout = self.raft.install_snapshot_timeout.as_duration();
        if copy_budget > install_snapshot_timeout {
            return Err(ValidationError::SnapshotCopyOutlastsInstall {
                copy_budget,
                install_snapshot_timeout,
            });
        }
        Ok(())
    }

    fn validate_peers(&self) -> Result<(), ValidationError> {
        let mut seen_ids = BTreeSet::new();
        for peer in &self.raft.peers {
            if !seen_ids.insert(peer.node_id) {
                return Err(ValidationError::Other(format!(
                    "duplicate raft peer node_id {}",
                    peer.node_id,
                )));
            }
        }
        let must_list_itself = !self.raft.peers.is_empty() || self.raft.wal.path.is_some();
        if must_list_itself && !seen_ids.contains(&self.raft.node_id) {
            return Err(ValidationError::NodeIdNotInPeers(self.raft.node_id));
        }
        if self.server.cluster_listen.is_some() {
            for peer in &self.raft.peers {
                let Some(client_url) = &peer.client_url else {
                    return Err(ValidationError::PeerClientUrlRequired {
                        node_id: peer.node_id,
                    });
                };
                if client_url.trim_end_matches('/') == peer.url.trim_end_matches('/') {
                    return Err(ValidationError::PeerClientUrlIsRaftUrl {
                        node_id: peer.node_id,
                    });
                }
            }
        }
        if self.raft.lists_another_peer() && self.raft.group_count.is_none() {
            return Err(ValidationError::GroupCountRequired);
        }
        Ok(())
    }

    /// Every configured meta voter is in `raft.peers`, or, when it is empty
    /// (a node on a temporary WAL), this node.
    fn validate_meta_voters(&self) -> Result<(), ValidationError> {
        let Some(voters) = &self.raft.meta_voters else {
            return Ok(());
        };
        let listed = |node_id: u64| {
            if self.raft.peers.is_empty() {
                node_id == self.raft.node_id
            } else {
                self.raft.peers.iter().any(|peer| peer.node_id == node_id)
            }
        };
        match voters.iter().map(|id| id.get()).find(|id| !listed(*id)) {
            Some(node_id) => Err(ValidationError::MetaVoterOutsidePeers { node_id }),
            None => Ok(()),
        }
    }

    fn validate_non_zero_durations(&self) -> Result<(), ValidationError> {
        for (name, value) in [
            (
                "raft.bootstrap_peer_probe_interval",
                self.raft.bootstrap_peer_probe_interval.as_duration(),
            ),
            (
                "storage.cold.flush_interval",
                self.storage.cold.flush_interval.as_duration(),
            ),
            (
                "storage.cold.gc_interval",
                self.storage.cold.gc_interval.as_duration(),
            ),
            (
                "storage.cold.compaction_interval",
                self.storage.cold.compaction_interval.as_duration(),
            ),
            (
                "storage.cold.compaction_gc_grace",
                self.storage.cold.compaction_gc_grace.as_duration(),
            ),
            (
                "storage.snapshot.s3_timeout",
                self.storage.snapshot.s3_timeout.as_duration(),
            ),
        ] {
            if value.is_zero() {
                return Err(ValidationError::Other(format!("{name} must be non-zero",)));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::S3HealthSetting;
    use super::ValidationError;
    use crate::HumanDuration;
    use crate::S3Config;
    use crate::UrsulaConfig;
    use crate::human::HumanSize;

    #[test]
    fn s3_health_gate_settings_must_be_non_zero() {
        let validate = |s3: S3Config| {
            let mut config = UrsulaConfig::default();
            config.raft.node_id = 1;
            config.storage.cold.s3 = Some(s3);
            config.validate()
        };
        validate(S3Config::default()).expect("the defaults are valid");

        for (setting, s3) in [
            (S3HealthSetting::ProbeTimeout, S3Config {
                probe_timeout: HumanDuration::sec(0),
                ..S3Config::default()
            }),
            (S3HealthSetting::UnhealthyTicks, S3Config {
                unhealthy_ticks: 0,
                ..S3Config::default()
            }),
            (S3HealthSetting::HealTicks, S3Config {
                heal_ticks: 0,
                ..S3Config::default()
            }),
        ] {
            let err = validate(s3).expect_err("a zero setting is rejected");
            assert!(
                matches!(err, ValidationError::ZeroS3HealthSetting { setting: found } if found == setting),
                "{err}"
            );
            assert_eq!(
                err.to_string(),
                format!("storage.cold.s3.{setting} must be non-zero")
            );
        }
    }

    /// The bound `Bootstrap` also checks: the default and the bound pass,
    /// one group more is refused.
    #[test]
    fn the_group_count_is_bounded_as_bootstrap_bounds_it() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.validate().expect("the default group count is valid");
        let max = usize::try_from(ursula_proto::placement::MAX_GROUP_COUNT).unwrap();
        config.raft.group_count = Some(max);
        config.validate().expect("the bound is valid");
        let above = max.checked_add(1).unwrap();
        config.raft.group_count = Some(above);
        let err = config
            .validate()
            .expect_err("one group above the bound is refused");
        assert!(
            matches!(err, ValidationError::GroupCountAboveMax { group_count } if group_count == above),
            "{err}"
        );
        assert_eq!(
            err.to_string(),
            format!("raft.group_count ({above}) must be at most {max}")
        );
    }

    #[test]
    fn the_leadership_balance_peer_timeout_must_be_non_zero() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.validate().expect("the defaults are valid");
        config.governance.leadership_balance.peer_timeout = HumanDuration::milli(0);
        let err = config
            .validate()
            .expect_err("a zero peer timeout is rejected");
        assert!(matches!(err, ValidationError::ZeroPeerTimeout), "{err}");
        assert_eq!(
            err.to_string(),
            "governance.leadership_balance.peer_timeout must be non-zero"
        );
    }

    /// The snapshot driver always runs: a zero interval is refused, an
    /// unset or non-zero one is valid.
    #[test]
    fn the_snapshot_drive_interval_must_be_non_zero() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.validate().expect("an unset interval is valid");
        config.storage.snapshot.drive_interval = Some(HumanDuration::sec(45));
        config.validate().expect("a non-zero interval is valid");
        config.storage.snapshot.drive_interval = Some(HumanDuration::milli(0));
        let err = config
            .validate()
            .expect_err("a zero drive interval is refused");
        assert!(
            matches!(err, ValidationError::ZeroSnapshotDriveInterval),
            "{err}"
        );
        assert!(
            err.to_string()
                .starts_with("storage.snapshot.drive_interval must be non-zero"),
            "{err}"
        );
    }

    #[test]
    fn cold_health_watermarks_leave_room_between_flush_and_backpressure() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.storage.cold.flush_min_hot_size = Some(HumanSize::mib(8));
        config.storage.cold.max_hot_size_per_group = Some(HumanSize::mib(64));
        config.governance.cold_health.hot_size_low = HumanSize::mib(32);
        config.governance.cold_health.hot_size_high = HumanSize::mib(48);
        config.validate().expect("valid cold-health window");

        config.governance.cold_health.hot_size_low = HumanSize::mib(4);
        config.governance.cold_health.hot_size_high = HumanSize::mib(8);
        let error = config
            .validate()
            .expect_err("shedding at the flush threshold must be rejected");
        assert!(error.to_string().contains("normal flushing can start"));

        config.governance.cold_health.hot_size_low = HumanSize::mib(32);
        config.governance.cold_health.hot_size_high = HumanSize::mib(64);
        let error = config
            .validate()
            .expect_err("shedding at the backpressure cliff must be rejected");
        assert!(error.to_string().contains("max_hot_size_per_group"));
    }

    /// #669: the bound on deleted data is at least eight snapshot installs.
    #[test]
    fn deleted_data_max_age_is_at_least_eight_installs() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        assert_eq!(config.raft.deleted_data_max_age, None);
        assert_eq!(
            config.raft.effective_deleted_data_max_age(),
            std::time::Duration::from_secs(3_600)
        );
        config.validate().expect("the defaults are valid");
        config.raft.deleted_data_max_age = Some(HumanDuration::min(16));
        config
            .validate()
            .expect("eight times the default install timeout");
        config.raft.deleted_data_max_age = Some(HumanDuration::sec(959));
        let err = config.validate().expect_err("shorter than eight installs");
        assert!(
            matches!(
                err,
                ValidationError::DeletedDataMaxAgeTooShort { value, minimum }
                    if value == std::time::Duration::from_secs(959)
                        && minimum == std::time::Duration::from_secs(960)
            ),
            "{err}"
        );
        config.raft.install_snapshot_timeout = HumanDuration::sec(10);
        config.raft.deleted_data_max_age = Some(HumanDuration::sec(80));
        config
            .validate()
            .expect("eight times a shorter install timeout");
    }

    /// #669: a 0.8.0 config with a long install timeout and no
    /// `deleted_data_max_age` stays valid: the unset bound follows the
    /// install timeout.
    #[test]
    fn an_unset_deleted_data_max_age_follows_a_long_install_timeout() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.raft.install_snapshot_timeout = HumanDuration::min(10);
        config
            .validate()
            .expect("an unset bound never refuses a config");
        assert_eq!(
            config.raft.effective_deleted_data_max_age(),
            std::time::Duration::from_secs(80 * 60)
        );
    }

    /// A cluster node of one peer, itself, with S3 snapshots: the node
    /// that copies a snapshot it installs.
    fn s3_snapshot_cluster_node() -> UrsulaConfig {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.raft.peers = vec![crate::config::RaftPeerConfig {
            node_id: 1,
            url: "http://127.0.0.1:4437".to_owned(),
            client_url: None,
        }];
        config.raft.wal.path = Some(std::path::PathBuf::from("/var/lib/ursula/raft"));
        config.server.cluster_id =
            Some(ursula_headers::ClusterId::new("validate-test").expect("a valid cluster id"));
        config.storage.cold.backend = crate::config::ColdBackend::S3;
        config.storage.cold.s3 = Some(S3Config {
            bucket: Some("bucket".to_owned()),
            ..S3Config::default()
        });
        config
    }

    /// #742: with S3 snapshots, the defaults fit a follower's install copy
    /// inside the default install timeout with a margin, so the node starts
    /// with no word about it.
    #[test]
    fn the_default_snapshot_store_budget_fits_an_install_copy() {
        let config = s3_snapshot_cluster_node();
        assert_eq!(
            config
                .storage
                .snapshot
                .backend
                .resolve(config.storage.cold.backend),
            crate::RaftSnapshotBackend::S3
        );
        config.validate().expect("the defaults are valid");
        let copy = config.storage.snapshot.install_copy_budget();
        // A GET, two PUTs and a HEAD of 22 s each.
        assert_eq!(copy, std::time::Duration::from_secs(88));
        assert!(
            copy < config.raft.install_snapshot_timeout.as_duration(),
            "a margin within the default install timeout"
        );
        // The cold store's budget is its own and stays as it was.
        let cold = config.storage.cold.s3.as_ref().unwrap();
        assert_eq!(
            crate::s3_operation_budget(cold.timeout.as_duration(), cold.max_retries),
            std::time::Duration::from_secs(54)
        );
    }

    /// #742: an explicit snapshot-store budget or install timeout that lets
    /// the copy outlast the install is refused, naming the keys. With inline
    /// snapshots there is no copy, and nothing is refused.
    #[test]
    fn an_install_copy_that_outlasts_its_install_is_refused() {
        let mut config = s3_snapshot_cluster_node();
        config.storage.snapshot.s3_timeout = HumanDuration::sec(10);
        config.storage.snapshot.s3_max_retries = 3;
        let err = config
            .validate()
            .expect_err("four 54 s requests outlast 120 s");
        assert!(
            matches!(
                err,
                ValidationError::SnapshotCopyOutlastsInstall {
                    copy_budget,
                    install_snapshot_timeout,
                } if copy_budget == std::time::Duration::from_secs(216)
                    && install_snapshot_timeout == std::time::Duration::from_secs(120)
            ),
            "{err}"
        );
        let message = err.to_string();
        for key in [
            "storage.snapshot.s3_timeout",
            "storage.snapshot.s3_max_retries",
            "raft.install_snapshot_timeout",
        ] {
            assert!(message.contains(key), "{message}");
        }

        let mut config = s3_snapshot_cluster_node();
        config.raft.install_snapshot_timeout = HumanDuration::sec(60);
        config.raft.deleted_data_max_age = Some(HumanDuration::min(8));
        assert!(matches!(
            config.validate(),
            Err(ValidationError::SnapshotCopyOutlastsInstall { .. })
        ));
        config.storage.snapshot.backend = crate::RaftSnapshotBackend::Inline;
        config.validate().expect("inline snapshots copy nothing");
    }

    /// The bound counts the copy's second PUT under a new name: a budget
    /// within three requests' worth of the install timeout but not four is
    /// refused.
    #[test]
    fn an_install_copy_counts_its_retried_put() {
        let mut config = s3_snapshot_cluster_node();
        // 22 s per request: three fit inside 70 s, four do not.
        config.raft.install_snapshot_timeout = HumanDuration::sec(70);
        config.raft.deleted_data_max_age = Some(HumanDuration::min(10));
        let request = config.storage.snapshot.request_budget();
        assert_eq!(request, std::time::Duration::from_secs(22));
        assert!(request.saturating_mul(3) <= config.raft.install_snapshot_timeout.as_duration());
        assert!(matches!(
            config.validate(),
            Err(ValidationError::SnapshotCopyOutlastsInstall { copy_budget, .. })
                if copy_budget == std::time::Duration::from_secs(88)
        ));
    }

    /// Every Raft node uses the snapshot store of `storage.snapshot.backend`,
    /// so a node on a temporary WAL with no peers listed is refused for a
    /// copy that outlasts its install as a cluster node is.
    #[test]
    fn a_node_on_a_temporary_wal_is_refused_for_a_copy_that_outlasts_its_install() {
        let mut single = s3_snapshot_cluster_node();
        single.storage.snapshot.backend = crate::RaftSnapshotBackend::S3;
        single.storage.snapshot.s3_max_retries = 3;
        single.raft.peers.clear();
        single.raft.wal.path = None;
        assert!(matches!(
            single.validate(),
            Err(ValidationError::SnapshotCopyOutlastsInstall { .. })
        ));
    }

    /// A node with `raft.wal.path` lists itself and names its deployment, and
    /// a node that lists another peer keeps a WAL path and sets the group
    /// count. Each refusal is its own variant.
    #[test]
    fn a_durable_node_lists_itself_and_a_cluster_sets_its_group_count() {
        let mut durable = UrsulaConfig::default();
        durable.raft.node_id = 1;
        durable.raft.wal.path = Some(std::path::PathBuf::from("/var/lib/ursula/raft"));
        assert!(matches!(
            durable.validate(),
            Err(ValidationError::ClusterIdRequired)
        ));
        durable.server.cluster_id =
            Some(ursula_headers::ClusterId::new("validate-test").expect("a valid cluster id"));
        assert!(matches!(
            durable.validate(),
            Err(ValidationError::NodeIdNotInPeers(1))
        ));
        let peer = |node_id: u64| crate::config::RaftPeerConfig {
            node_id,
            url: format!("http://node-{node_id}:4437"),
            client_url: None,
        };
        durable.raft.peers = vec![peer(1)];
        durable
            .validate()
            .expect("a single durable node that lists itself");

        let mut cluster = durable.clone();
        cluster.raft.peers.push(peer(2));
        assert!(matches!(
            cluster.validate(),
            Err(ValidationError::GroupCountRequired)
        ));
        cluster.raft.group_count = Some(8);
        cluster
            .validate()
            .expect("a cluster node with its group count");
        cluster.raft.wal.path = None;
        assert!(matches!(
            cluster.validate(),
            Err(ValidationError::RaftWalPathRequired)
        ));

        // A temporary WAL needs neither: the server lists the node and
        // generates its cluster id.
        let mut temporary = UrsulaConfig::default();
        temporary.raft.node_id = 1;
        temporary.validate().expect("a node on a temporary WAL");
        temporary.raft.peers = vec![peer(1)];
        temporary
            .validate()
            .expect("a node on a temporary WAL that lists itself");
    }

    #[test]
    fn snapshot_pressure_batch_must_be_non_zero() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.raft.snapshot_pressure_max_groups_per_tick = 0;
        let error = config
            .validate()
            .expect_err("zero pressure batch must be rejected");
        assert!(
            error
                .to_string()
                .contains("snapshot_pressure_max_groups_per_tick")
        );
    }
}
