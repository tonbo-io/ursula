//! Archive publication tests over the production codec and simulated disk.
//! These do not substitute for the writer's reference-commit/reclaim tests.

#![cfg(madsim)]

use std::path::Path;

use bytes::Bytes;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::entry::RaftEntry;
use openraft::vote::RaftLeaderId;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use serde::Deserialize;
use serde::Serialize;
use ursula_runtime::GroupWriteCommand;
use ursula_shard::BucketStreamId;
use ursula_stream::StreamCommand;

use super::CoreJournalRecord;
use super::RaftGroupLogRecord;
use super::disk::JournalDisk;
use super::disk::JournalFile;
use super::frozen;
use super::frozen::ArchiveDefect;
use super::frozen::ArchiveId;
use super::frozen::FrozenAppend;
use super::frozen::FrozenEntry;
use super::journal;
use super::journal::JournalError;
use super::journal::JournalOp;
use super::journal::JournalReplayMode;
use super::journal::JournalWriter;
use super::sim_disk::SimDisk;
use super::sim_disk::SimDiskError;
use super::sim_disk::SimDiskFault;
use super::writer::WireCodec;
use crate::codec::encode_wire;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::entry_log_bytes;

type Entry = EntryOf<UrsulaRaftTypeConfig>;
type Id = LogIdOf<UrsulaRaftTypeConfig>;

fn simulated(seed: u64, test: impl FnOnce() + 'static) {
    let _guard = crate::tests::madsim_test_guard();
    madsim::runtime::Runtime::with_seed_and_config(seed, Default::default())
        .block_on(async move { test() });
}

fn archive_id() -> ArchiveId {
    ArchiveId {
        group_id: 7,
        segment: 2,
        offset: 32,
        first_index: 1,
        last_index: 3,
        content_hash: [0; 32],
    }
}

fn entries(byte: u8) -> Vec<Entry> {
    (1..=3)
        .map(|index| {
            Entry::new(
                LogId {
                    leader_id: CommittedLeaderId::new(5, 1),
                    index,
                },
                EntryPayload::Normal(GroupWriteCommand::Stream(StreamCommand::Append {
                    stream_id: BucketStreamId::new("archive", "durability"),
                    content_type: None,
                    payload: Bytes::from(vec![byte; 32_768]),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                })),
            )
        })
        .collect()
}

fn replace_bytes(path: &Path, bytes: &[u8]) {
    SimDisk::truncate(path, 0).unwrap();
    let mut file = SimDisk::open_append(path).unwrap();
    file.append(bytes).unwrap();
    file.sync_data().unwrap();
}

fn assert_injected(
    error: JournalError,
    expected_op: JournalOp,
    target: &Path,
    fault: SimDiskFault,
) {
    match error {
        JournalError::Io { op, source, .. } => {
            assert_eq!(op, expected_op);
            assert!(
                matches!(source.get_ref().and_then(|error| error.downcast_ref::<SimDiskError>()),
                Some(SimDiskError::Injected { path, fault: actual }) if path == target && *actual == fault)
            );
        }
        other => panic!("expected injected {fault:?} at {target:?}, got {other}"),
    }
}

#[test]
fn published_archive_survives_power_loss_and_identical_retry() {
    for seed in [1, 7, 19] {
        simulated(seed, || {
            let dir = SimDisk::provision_dir("archive-published").unwrap();
            let expected = entries(11);
            let reference = frozen::publish(&dir, archive_id(), expected.clone()).unwrap();
            assert!(reference.bytes > 32_768);
            let before = SimDisk::read(&frozen::path(&dir, reference.id)).unwrap();
            SimDisk::power_loss(&dir).unwrap();
            assert_eq!(
                encode_wire(&frozen::read(&dir, 7, &reference).unwrap()),
                encode_wire(&expected)
            );
            assert_eq!(
                frozen::publish(&dir, archive_id(), expected.clone()).unwrap(),
                reference
            );
            SimDisk::power_loss_losing_unsynced(&dir).unwrap();
            assert_eq!(
                SimDisk::read(&frozen::path(&dir, reference.id)).unwrap(),
                before
            );
            assert_eq!(
                encode_wire(&frozen::read(&dir, 7, &reference).unwrap()),
                encode_wire(&expected)
            );
        });
    }
}

#[test]
fn publication_faults_never_return_a_reference_and_retry_survives_power_loss() {
    simulated(5, || {
        for (boundary, power_loss) in
            (0..4).flat_map(|boundary| [false, true].map(move |loss| (boundary, loss)))
        {
            let dir = SimDisk::provision_dir("archive-publication-fault").unwrap();
            // Determine the content-addressed destination through publication,
            // then remove this unreferenced probe before arming a first-write fault.
            let probe = frozen::publish(&dir, archive_id(), entries(22)).unwrap();
            let destination = frozen::path(&dir, probe.id);
            SimDisk::remove_file(&destination).unwrap();
            SimDisk::sync_dir(&dir).unwrap();
            let temporary = frozen::path(&dir, archive_id()).with_extension("tmp");
            let (target, fault, operation) = match boundary {
                0 => (temporary.clone(), SimDiskFault::Write, JournalOp::Append),
                1 => (temporary.clone(), SimDiskFault::Sync, JournalOp::Sync),
                2 => (destination.clone(), SimDiskFault::Rename, JournalOp::Rename),
                _ => (dir.clone(), SimDiskFault::Sync, JournalOp::SyncDir),
            };
            SimDisk::inject_fault(&target, fault).unwrap();
            assert_injected(
                frozen::publish(&dir, archive_id(), entries(22)).unwrap_err(),
                operation,
                &target,
                fault,
            );
            if power_loss {
                SimDisk::power_loss_losing_unsynced(&dir).unwrap();
                assert!(!SimDisk::exists(&destination));
            } else {
                SimDisk::process_crash(&dir).unwrap();
                assert_eq!(SimDisk::exists(&destination), boundary == 3);
            }
            let reference = frozen::publish(&dir, archive_id(), entries(22)).unwrap();
            SimDisk::power_loss_losing_unsynced(&dir).unwrap();
            assert_eq!(
                encode_wire(&frozen::read(&dir, 7, &reference).unwrap()),
                encode_wire(&entries(22))
            );
        }
    });
}

#[test]
fn reused_source_position_publishes_distinct_content_without_overwriting_old_payload() {
    simulated(11, || {
        let dir = SimDisk::provision_dir("archive-source-position-reuse").unwrap();
        let first = frozen::publish(&dir, archive_id(), entries(33)).unwrap();
        let first_path = frozen::path(&dir, first.id);
        let before = SimDisk::read(&first_path).unwrap();
        let second = frozen::publish(&dir, archive_id(), entries(44)).unwrap();
        assert_ne!(first.id, second.id);
        assert_eq!(SimDisk::read(&first_path).unwrap(), before);
        SimDisk::power_loss_losing_unsynced(&dir).unwrap();
        assert_eq!(
            encode_wire(&frozen::read(&dir, 7, &first).unwrap()),
            encode_wire(&entries(33))
        );
        assert_eq!(
            encode_wire(&frozen::read(&dir, 7, &second).unwrap()),
            encode_wire(&entries(44))
        );
    });
}

#[test]
fn missing_truncated_corrupt_and_wrong_group_archives_fail_closed_without_repair() {
    simulated(17, || {
        for defect in 0..4 {
            let dir = SimDisk::provision_dir("archive-corruption").unwrap();
            let reference = frozen::publish(&dir, archive_id(), entries(55)).unwrap();
            let path = frozen::path(&dir, reference.id);
            match defect {
                0 => {
                    SimDisk::remove_file(&path).unwrap();
                }
                1 => {
                    SimDisk::truncate(&path, reference.bytes.saturating_sub(1)).unwrap();
                }
                2 => {
                    let mut bytes = SimDisk::read(&path).unwrap();
                    *bytes.last_mut().unwrap() ^= 1;
                    replace_bytes(&path, &bytes);
                }
                _ => {}
            }
            let before = SimDisk::exists(&path).then(|| SimDisk::read(&path).unwrap());
            for _ in 0..2 {
                let error =
                    frozen::read(&dir, if defect == 3 { 8 } else { 7 }, &reference).unwrap_err();
                match defect {
                    0 => assert!(
                        matches!(error, JournalError::Io { op: JournalOp::Open, source, .. }
                        if matches!(source.get_ref().and_then(|error| error.downcast_ref::<SimDiskError>()), Some(SimDiskError::NotFound { path: missing }) if *missing == path))
                    ),
                    1 => assert!(matches!(error, JournalError::FrozenArchive {
                        defect: ArchiveDefect::Length,
                        ..
                    })),
                    2 => assert!(matches!(error, JournalError::FrozenArchive {
                        defect: ArchiveDefect::Checksum,
                        ..
                    })),
                    _ => assert!(matches!(error, JournalError::FrozenArchive {
                        defect: ArchiveDefect::Content,
                        ..
                    })),
                }
                assert_eq!(
                    SimDisk::exists(&path).then(|| SimDisk::read(&path).unwrap()),
                    before
                );
            }
        }
    });
}

#[test]
fn checksum_matching_incomplete_archive_is_still_rejected() {
    simulated(23, || {
        let dir = SimDisk::provision_dir("archive-incomplete-frame").unwrap();
        let mut reference = frozen::publish(&dir, archive_id(), entries(66)).unwrap();
        let path = frozen::path(&dir, reference.id);
        SimDisk::truncate(&path, reference.bytes.saturating_sub(1)).unwrap();
        let before = SimDisk::read(&path).unwrap();
        reference.bytes = u64::try_from(before.len()).unwrap();
        reference.checksum = crc32fast::hash(&before);
        reference.id.content_hash = *blake3::hash(&before).as_bytes();
        let malformed_path = frozen::path(&dir, reference.id);
        SimDisk::rename(&path, &malformed_path).unwrap();
        SimDisk::sync_dir(&dir).unwrap();
        assert!(matches!(
            frozen::read(&dir, 7, &reference).unwrap_err(),
            JournalError::FrozenArchive {
                defect: ArchiveDefect::Content,
                ..
            }
        ));
        assert_eq!(SimDisk::read(&malformed_path).unwrap(), before);
    });
}

#[test]
fn old_decoder_refuses_frozen_reference_without_trimming_either_replay_mode() {
    #[derive(Serialize, Deserialize)]
    struct LegacyCoreJournalRecord {
        group_id: u32,
        record: LegacyRaftGroupLogRecord,
    }
    #[derive(Serialize, Deserialize)]
    enum LegacyRaftGroupLogRecord {
        SaveCommitted(Option<Id>),
        Append(Vec<Entry>),
        TruncateAfter(Option<Id>),
        Purge(Id),
    }
    simulated(29, || {
        let dir = SimDisk::provision_dir("archive-old-decoder").unwrap();
        let values = entries(77);
        let archive = frozen::publish(&dir, archive_id(), values.clone()).unwrap();
        let path = dir.join("journal-00000000000000000001.seg");
        let mut writer = JournalWriter::open(&path, 1).unwrap();
        writer
            .append::<WireCodec<CoreJournalRecord>>(&CoreJournalRecord {
                group_id: 7,
                record: RaftGroupLogRecord::Append(values.clone()),
            })
            .unwrap();
        writer
            .append::<WireCodec<CoreJournalRecord>>(&CoreJournalRecord {
                group_id: 7,
                record: RaftGroupLogRecord::FrozenAppend(Box::new(FrozenAppend {
                    archive,
                    entries: values
                        .iter()
                        .map(|entry| FrozenEntry {
                            log_id: entry.log_id,
                            bytes: u32::try_from(entry_log_bytes(entry)).unwrap(),
                        })
                        .collect(),
                })),
            })
            .unwrap();
        writer.sync().unwrap();
        drop(writer);
        let before = SimDisk::read(&path).unwrap();
        for mode in [JournalReplayMode::Strict, JournalReplayMode::VerifiedPrefix] {
            let mut visited = 0_usize;
            let error = journal::replay::<WireCodec<LegacyCoreJournalRecord>>(
                &path,
                mode,
                |_loc, record| {
                    assert_eq!(record.group_id, 7);
                    match record.record {
                        LegacyRaftGroupLogRecord::Append(entries) => assert_eq!(entries.len(), 3),
                        LegacyRaftGroupLogRecord::SaveCommitted(id)
                        | LegacyRaftGroupLogRecord::TruncateAfter(id) => {
                            panic!("unexpected legacy marker {id:?}")
                        }
                        LegacyRaftGroupLogRecord::Purge(id) => {
                            panic!("unexpected legacy purge {id:?}")
                        }
                    }
                    visited = visited.saturating_add(1);
                    Ok(())
                },
            )
            .unwrap_err();
            assert_eq!(
                visited, 1,
                "old schema accepts the preceding ordinary Append"
            );
            assert!(matches!(error, JournalError::Undecodable { .. }));
            assert_eq!(SimDisk::read(&path).unwrap(), before);
        }
    });
}

#[test]
fn selected_reference_keeps_exact_live_ids_and_rejects_invalid_selection() {
    simulated(31, || {
        let dir = SimDisk::provision_dir("archive-selected-ids").unwrap();
        let values = entries(88);
        let archive = frozen::publish(&dir, archive_id(), values.clone()).unwrap();
        let ids = values.iter().map(|entry| entry.log_id).collect::<Vec<_>>();
        let reference = FrozenAppend {
            archive: archive.clone(),
            entries: [0, 2]
                .into_iter()
                .map(|index| FrozenEntry {
                    log_id: ids[index],
                    bytes: u32::try_from(entry_log_bytes(&values[index])).unwrap(),
                })
                .collect(),
        };
        assert_eq!(
            encode_wire(&frozen::selected(&dir, 7, &reference).unwrap()),
            encode_wire(&vec![values[0].clone(), values[2].clone()])
        );
        let before = SimDisk::read(&frozen::path(&dir, archive.id)).unwrap();
        let wrong_bytes = FrozenAppend {
            archive: archive.clone(),
            entries: vec![FrozenEntry {
                log_id: ids[0],
                bytes: 0,
            }],
        };
        assert!(matches!(
            frozen::selected(&dir, 7, &wrong_bytes).unwrap_err(),
            JournalError::FrozenArchive {
                defect: ArchiveDefect::Content,
                ..
            }
        ));
        for log_ids in [
            vec![],
            vec![ids[0], ids[0]],
            vec![ids[2], ids[0]],
            vec![LogId {
                leader_id: CommittedLeaderId::new(6, 1),
                index: 1,
            }],
            vec![LogId {
                leader_id: CommittedLeaderId::new(5, 1),
                index: 4,
            }],
        ] {
            let reference = FrozenAppend {
                archive: archive.clone(),
                entries: log_ids
                    .into_iter()
                    .map(|log_id| FrozenEntry {
                        log_id,
                        bytes: u32::try_from(entry_log_bytes(&values[0])).unwrap(),
                    })
                    .collect(),
            };
            assert!(matches!(
                frozen::selected(&dir, 7, &reference).unwrap_err(),
                JournalError::FrozenArchive {
                    defect: ArchiveDefect::Content,
                    ..
                }
            ));
            assert_eq!(
                SimDisk::read(&frozen::path(&dir, archive.id)).unwrap(),
                before
            );
        }
    });
}

#[test]
fn archive_and_journal_reference_have_separate_durability_boundaries() {
    simulated(37, || {
        // This composes the production archive and journal primitives. The
        // writer lifecycle tests separately verify its deletion authorization.
        for phase in 0..3 {
            let dir = SimDisk::provision_dir("archive-reference-commit").unwrap();
            let values = entries(99);
            let original = dir.join("journal-00000000000000000001.seg");
            let mut source = JournalWriter::open(&original, 1).unwrap();
            source
                .append::<WireCodec<CoreJournalRecord>>(&CoreJournalRecord {
                    group_id: 7,
                    record: RaftGroupLogRecord::Append(values.clone()),
                })
                .unwrap();
            source.sync().unwrap();
            drop(source);
            let original_bytes = SimDisk::read(&original).unwrap();
            let archive = frozen::publish(&dir, archive_id(), values.clone()).unwrap();
            let destination = dir.join("journal-00000000000000000002.seg");
            let mut target = JournalWriter::open(&destination, 2).unwrap();
            target.sync().unwrap();
            target
                .append::<WireCodec<CoreJournalRecord>>(&CoreJournalRecord {
                    group_id: 7,
                    record: RaftGroupLogRecord::FrozenAppend(Box::new(FrozenAppend {
                        archive: archive.clone(),
                        entries: values
                            .iter()
                            .map(|entry| FrozenEntry {
                                log_id: entry.log_id,
                                bytes: u32::try_from(entry_log_bytes(entry)).unwrap(),
                            })
                            .collect(),
                    })),
                })
                .unwrap();
            target.flush().unwrap();
            match phase {
                0 => {} // Archive durable, reference only in the page cache.
                1 => {
                    SimDisk::inject_fault(&destination, SimDiskFault::Sync).unwrap();
                    assert_injected(
                        target.sync_data().unwrap_err(),
                        JournalOp::Sync,
                        &destination,
                        SimDiskFault::Sync,
                    );
                }
                _ => {
                    target.sync_data().unwrap();
                }
            }
            drop(target);
            if phase == 2 {
                SimDisk::remove_file(&original).unwrap();
                SimDisk::sync_dir(&dir).unwrap();
            }
            SimDisk::power_loss_losing_unsynced(&dir).unwrap();
            let mut resolved = Vec::new();
            journal::replay::<WireCodec<CoreJournalRecord>>(
                &destination,
                JournalReplayMode::Strict,
                |_loc, record| {
                    let RaftGroupLogRecord::FrozenAppend(reference) = record.record else {
                        panic!("expected frozen reference")
                    };
                    resolved.extend(frozen::selected(&dir, record.group_id, &reference).unwrap());
                    Ok(())
                },
            )
            .unwrap();
            if phase == 2 {
                assert!(!SimDisk::exists(&original));
                assert_eq!(encode_wire(&resolved), encode_wire(&values));
            } else {
                assert!(resolved.is_empty());
                assert_eq!(SimDisk::read(&original).unwrap(), original_bytes);
                assert_eq!(
                    encode_wire(&frozen::read(&dir, 7, &archive).unwrap()),
                    encode_wire(&values)
                );
                frozen::collect(&dir, &Default::default()).unwrap();
                assert_eq!(SimDisk::read(&original).unwrap(), original_bytes);
            }
        }
    });
}

#[test]
fn garbage_collection_preserves_duplicate_references_and_syncs_their_deletion_first() {
    simulated(41, || {
        let dir = SimDisk::provision_dir("archive-reference-gc").unwrap();
        let values = entries(111);
        let archive = frozen::publish(&dir, archive_id(), values.clone()).unwrap();
        let archive_path = frozen::path(&dir, archive.id);
        let record = CoreJournalRecord {
            group_id: 7,
            record: RaftGroupLogRecord::FrozenAppend(Box::new(FrozenAppend {
                archive: archive.clone(),
                entries: values
                    .iter()
                    .map(|entry| FrozenEntry {
                        log_id: entry.log_id,
                        bytes: u32::try_from(entry_log_bytes(entry)).unwrap(),
                    })
                    .collect(),
            })),
        };
        let references = [
            dir.join("journal-00000000000000000001.seg"),
            dir.join("journal-00000000000000000002.seg"),
        ];
        for (index, path) in references.iter().enumerate() {
            let mut writer =
                JournalWriter::open(path, u64::try_from(index).unwrap().checked_add(1).unwrap())
                    .unwrap();
            writer
                .append::<WireCodec<CoreJournalRecord>>(&record)
                .unwrap();
            writer.sync().unwrap();
        }
        SimDisk::remove_file(&references[0]).unwrap();
        frozen::collect(&dir, &std::collections::BTreeSet::from([archive.id])).unwrap();
        assert!(
            SimDisk::exists(&archive_path),
            "the second physical reference retains the archive"
        );
        SimDisk::power_loss_losing_unsynced(&dir).unwrap();
        for path in &references {
            assert!(SimDisk::exists(path));
        }
        for path in &references {
            SimDisk::remove_file(path).unwrap();
        }
        SimDisk::inject_fault(&dir, SimDiskFault::Sync).unwrap();
        assert_injected(
            frozen::collect(&dir, &Default::default()).unwrap_err(),
            JournalOp::SyncDir,
            &dir,
            SimDiskFault::Sync,
        );
        assert!(SimDisk::exists(&archive_path));
        SimDisk::power_loss_losing_unsynced(&dir).unwrap();
        for path in &references {
            assert!(SimDisk::exists(path));
        }
        assert_eq!(
            encode_wire(&frozen::read(&dir, 7, &archive).unwrap()),
            encode_wire(&values)
        );
        for path in &references {
            SimDisk::remove_file(path).unwrap();
        }
        // collect must sync the now-absent journal names before unlinking data.
        frozen::collect(&dir, &Default::default()).unwrap();
        SimDisk::power_loss_losing_unsynced(&dir).unwrap();
        for path in &references {
            assert!(!SimDisk::exists(path));
        }
        assert!(!SimDisk::exists(&archive_path));
    });
}
