//! History identity (#581 section 4): one test per rule, through the
//! production entry points, then a proptest of tokens and producer fencing
//! across history events.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::num::NonZeroU64;

use bytes::Bytes;
use proptest::prelude::*;

use super::BucketStreamId;
use super::ClosedBy;
use super::ClosedEpoch;
use super::IncarnationCheck;
use super::ProducerAck;
use super::ProducerDelivery;
use super::ProducerRefusal;
use super::ProducerRequest;
use super::StreamCommand;
use super::StreamErrorCode;
use super::StreamErrorContext;
use super::StreamResponse;
use super::StreamSnapshot;
use super::StreamStateMachine;
use crate::history::InheritedReport;
use crate::history::MAX_CREATED_AT_MS;
use crate::history::incarnation_token;

const OCTET: &str = "application/octet-stream";
const BUCKET: &str = "hist";

fn id(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("non-zero history id")
}

/// A group whose genesis (history `genesis`, at log index 1) applied, with
/// one bucket.
fn group(genesis: u64) -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    assert_eq!(machine.history(), None);
    assert!(matches!(
        machine.apply_entry(1, StreamCommand::NewHistory {
            history: id(genesis),
            now_ms: 0,
            previous: 0,
            kept: 0,
            leader: crate::HistoryLeader {
                term: 0,
                node_id: 0,
            },
        }),
        StreamResponse::HistoryStarted { .. }
    ));
    assert!(matches!(
        machine.apply_entry(2, StreamCommand::CreateBucket {
            bucket_id: BUCKET.to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    machine
}

/// A restore target: a group that applied only its genesis, which is
/// empty.
fn genesis_only(genesis: u64) -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    new_history(&mut machine, 1, genesis, 0);
    machine
}

fn sid(name: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, name)
}

fn create_at(machine: &mut StreamStateMachine, name: &str, now_ms: u64) -> StreamResponse {
    machine.apply(StreamCommand::CreateStream {
        stream_id: sid(name),
        content_type: OCTET.to_owned(),
        initial_payload: Bytes::new(),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms,
    })
}

fn create(machine: &mut StreamStateMachine, name: &str) -> BucketStreamId {
    assert!(matches!(
        create_at(machine, name, 1),
        StreamResponse::Created { .. }
    ));
    sid(name)
}

fn append(
    machine: &mut StreamStateMachine,
    stream_id: &BucketStreamId,
    body: &[u8],
    producer: Option<(u64, u64)>,
) -> StreamResponse {
    machine.apply(append_command(stream_id, body, producer))
}

fn append_command(
    stream_id: &BucketStreamId,
    body: &[u8],
    producer: Option<(u64, u64)>,
) -> StreamCommand {
    StreamCommand::Append {
        stream_id: stream_id.clone(),
        content_type: Some(OCTET.to_owned()),
        payload: Bytes::copy_from_slice(body),
        close_after: false,
        stream_seq: None,
        producer: producer.map(|(epoch, seq)| ProducerRequest {
            producer_id: "p".to_owned(),
            producer_epoch: epoch,
            producer_seq: seq,
            ..Default::default()
        }),
        now_ms: 1,
    }
}

fn tokened(token: u64, command: StreamCommand) -> StreamCommand {
    StreamCommand::IfIncarnation {
        incarnation: token,
        command: Box::new(command),
    }
}

fn new_history(machine: &mut StreamStateMachine, index: u64, history: u64, now_ms: u64) {
    let previous = machine.history_id();
    assert_eq!(
        machine.apply_entry(index, StreamCommand::NewHistory {
            history: id(history),
            now_ms,
            previous,
            kept: index.saturating_sub(1),
            leader: crate::HistoryLeader {
                term: 0,
                node_id: 0,
            },
        }),
        StreamResponse::HistoryStarted {
            history: id(history)
        }
    );
}

fn token(machine: &StreamStateMachine, stream_id: &BucketStreamId) -> u64 {
    machine.stream_incarnation(stream_id).expect("a stream")
}

fn bytes(machine: &StreamStateMachine, stream_id: &BucketStreamId) -> Vec<u8> {
    machine
        .read(stream_id, 0, usize::MAX)
        .expect("a hot read")
        .payload
}

fn ack(response: StreamResponse) -> ProducerAck {
    match response {
        StreamResponse::Appended {
            producer: Some(ack),
            ..
        }
        | StreamResponse::Closed {
            producer: Some(ack),
            ..
        } => ack,
        other => panic!("expected a producer ack, got {other:?}"),
    }
}

fn producer_refusal(response: StreamResponse) -> (StreamErrorCode, ProducerRefusal) {
    match response {
        StreamResponse::Error { code, context, .. } => match context.as_slice() {
            [StreamErrorContext::Producer(refusal)] => (code, *refusal),
            other => panic!("expected a producer refusal, got {code:?} {other:?}"),
        },
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn import(
    target: &mut StreamStateMachine,
    index: u64,
    backup: StreamSnapshot,
    history: u64,
    digest: u8,
) -> StreamResponse {
    target.apply_entry(index, StreamCommand::ImportSnapshot {
        snapshot: Box::new(backup),
        history: id(history),
        backup_digest: [digest; 32],
        now_ms: 1,
    })
}

#[test]
fn every_token_is_the_folded_created_at_in_the_tagged_range() {
    let mut machine = group(5);
    let stream_id = create(&mut machine, "s");
    let created_at_ms = machine.head(&stream_id).unwrap().created_at_ms;
    assert_eq!(
        token(&machine, &stream_id),
        incarnation_token(created_at_ms, 5)
    );
    assert!((1 << 52..1 << 53).contains(&token(&machine, &stream_id)));
}

#[test]
fn a_create_at_or_above_2_pow_52_is_refused_and_creates_nothing() {
    let mut machine = group(1);
    assert!(matches!(
        create_at(&mut machine, "far", MAX_CREATED_AT_MS),
        StreamResponse::Created { .. }
    ));
    let before = machine.snapshot();
    assert!(matches!(
        create_at(&mut machine, "beyond", MAX_CREATED_AT_MS),
        StreamResponse::Error {
            code: StreamErrorCode::CreatedAtOutOfRange,
            ..
        }
    ));
    assert_eq!(machine.snapshot(), before);
    let mut fresh = group(1);
    assert!(matches!(
        create_at(&mut fresh, "beyond", MAX_CREATED_AT_MS + 1),
        StreamResponse::Error {
            code: StreamErrorCode::CreatedAtOutOfRange,
            ..
        }
    ));
}

/// W2: two clusters that create one URL in the same millisecond give two
/// tokens, because each group drew its own genesis id.
#[test]
fn equal_created_at_under_two_genesis_ids_gives_two_tokens() {
    let mut first = group(11);
    let mut second = group(12);
    let a = create(&mut first, "same");
    let b = create(&mut second, "same");
    assert_eq!(
        first.head(&a).unwrap().created_at_ms,
        second.head(&b).unwrap().created_at_ms
    );
    assert_ne!(token(&first, &a), token(&second, &b));
}

#[test]
fn new_history_changes_every_token_and_no_byte() {
    let mut machine = group(1);
    let a = create(&mut machine, "a");
    let b = create(&mut machine, "b");
    for stream_id in [&a, &b] {
        assert!(matches!(
            append(&mut machine, stream_id, b"abc", None),
            StreamResponse::Appended { .. }
        ));
    }
    let before = machine.snapshot();
    let tokens = [token(&machine, &a), token(&machine, &b)];

    new_history(&mut machine, 40, 2, 7_000);

    assert_ne!(token(&machine, &a), tokens[0]);
    assert_ne!(token(&machine, &b), tokens[1]);
    let after = machine.snapshot();
    // Every stream keeps its incarnation, cold generation, bytes and tail.
    assert_eq!(after.streams, before.streams);
    let history = machine.history().expect("a history");
    assert_eq!(history.id, id(2));
    assert_eq!(history.event_index, 40);
    assert_eq!(history.imported, None);
    // C7: the counter is at least the event's clock.
    assert_eq!(machine.last_created_at_ms(), 7_000);
    let c = create(&mut machine, "c");
    assert_eq!(machine.head(&c).unwrap().created_at_ms, 7_001);
}

#[test]
fn a_write_naming_a_token_of_an_earlier_history_answers_412() {
    let mut machine = group(1);
    let stream_id = create(&mut machine, "s");
    let old = token(&machine, &stream_id);
    new_history(&mut machine, 10, 2, 0);
    let current = token(&machine, &stream_id);
    let before = machine.snapshot();
    match machine.apply(tokened(old, append_command(&stream_id, b"x", None))) {
        StreamResponse::Error {
            code: StreamErrorCode::IncarnationMismatch,
            context,
            ..
        } => assert_eq!(context, vec![StreamErrorContext::StreamIncarnation {
            incarnation: current
        }]),
        other => panic!("expected 412, got {other:?}"),
    }
    assert_eq!(machine.snapshot(), before);
    assert!(matches!(
        machine.apply(tokened(current, append_command(&stream_id, b"x", None))),
        StreamResponse::Appended { .. }
    ));
}

/// Row 11 (G2): a history event closes every producer's current epoch at
/// its surviving `last_seq`.
#[test]
fn a_history_event_fences_every_producer_record() {
    let mut machine = group(1);
    let stream_id = create(&mut machine, "s");
    for seq in 0..3 {
        ack(append(&mut machine, &stream_id, b"x", Some((4, seq))));
    }
    new_history(&mut machine, 10, 2, 0);
    let before = machine.snapshot();

    // Above `last`: epoch-closed by history, the same epoch closed at 2.
    assert_eq!(
        producer_refusal(append(&mut machine, &stream_id, b"y", Some((4, 3)))),
        (
            StreamErrorCode::ProducerEpochClosed,
            ProducerRefusal::EpochClosed {
                epoch: 4,
                by: ClosedBy::History,
                closed: ClosedEpoch { epoch: 4, seq: 2 },
            }
        )
    );
    // A gap above it is the same answer: nothing of the epoch applies.
    assert!(matches!(
        producer_refusal(append(&mut machine, &stream_id, b"y", Some((4, 9)))).1,
        ProducerRefusal::EpochClosed {
            by: ClosedBy::History,
            ..
        }
    ));
    // Repeats at and below `last` applied in the history that survived.
    assert!(matches!(
        ack(append(&mut machine, &stream_id, b"x", Some((4, 2)))).delivery,
        ProducerDelivery::Repeat(_)
    ));
    assert_eq!(
        ack(append(&mut machine, &stream_id, b"x", Some((4, 1)))).delivery,
        ProducerDelivery::Evicted
    );
    assert_eq!(machine.snapshot(), before);

    // A claim of a later epoch applies and reports what the event closed.
    let claim = ack(append(&mut machine, &stream_id, b"z", Some((5, 0))));
    assert_eq!(claim.closed_epoch, Some(ClosedEpoch { epoch: 4, seq: 2 }));
    assert!(matches!(claim.delivery, ProducerDelivery::Applied(_)));
    // The new epoch belongs to the current history: it is not fenced.
    assert!(matches!(
        ack(append(&mut machine, &stream_id, b"z", Some((5, 1)))).delivery,
        ProducerDelivery::Applied(_)
    ));
    // The old epoch now answers row 4, closed by the claim.
    assert_eq!(
        producer_refusal(append(&mut machine, &stream_id, b"y", Some((4, 3)))).1,
        ProducerRefusal::EpochClosed {
            epoch: 5,
            by: ClosedBy::Claim,
            closed: ClosedEpoch { epoch: 4, seq: 2 },
        }
    );
}

/// The early answer (section 2.5) reads the same rows: a fenced record's
/// next seq is not answered early, its repeat is.
#[test]
fn early_answers_see_the_history_fence() {
    let mut machine = group(1);
    let stream_id = create(&mut machine, "s");
    ack(append(&mut machine, &stream_id, b"x", Some((0, 0))));
    new_history(&mut machine, 10, 2, 0);
    let current = token(&machine, &stream_id);
    let request = |seq| ProducerRequest {
        producer_id: "p".to_owned(),
        producer_epoch: 0,
        producer_seq: seq,
        ..Default::default()
    };
    assert!(
        machine
            .producer_early_answer(&stream_id, &request(0), current, 1)
            .is_some()
    );
    assert_eq!(
        machine.producer_early_answer(&stream_id, &request(1), current, 1),
        None
    );
    assert!(!machine.append_would_deduplicate(
        &stream_id,
        Some(&request(1)),
        IncarnationCheck::Matched,
        1
    ));
}

#[test]
fn an_import_is_a_history_event_that_fences_restored_records() {
    let mut source = group(1);
    let stream_id = create(&mut source, "s");
    for seq in 0..2 {
        ack(append(&mut source, &stream_id, b"ab", Some((3, seq))));
    }
    let source_token = token(&source, &stream_id);

    let mut target = genesis_only(9);
    assert_eq!(
        import(&mut target, 5, source.snapshot(), 21, 0xaa),
        StreamResponse::SnapshotImported {
            buckets: 1,
            streams: 1,
            history: id(21),
            already: false,
        }
    );
    let history = target.history().expect("a history");
    assert_eq!(history.id, id(21));
    assert_eq!(history.event_index, 5);
    assert_eq!(history.imported, Some([0xaa; 32]));
    assert_ne!(token(&target, &stream_id), source_token);
    assert_eq!(bytes(&target, &stream_id), b"abab");
    assert_eq!(
        producer_refusal(append(&mut target, &stream_id, b"c", Some((3, 2)))).1,
        ProducerRefusal::EpochClosed {
            epoch: 3,
            by: ClosedBy::History,
            closed: ClosedEpoch { epoch: 3, seq: 1 },
        }
    );
    // Two restores of one backup start two histories.
    let mut other = genesis_only(9);
    import(&mut other, 5, source.snapshot(), 22, 0xaa);
    assert_ne!(token(&other, &stream_id), token(&target, &stream_id));
}

#[test]
fn a_repeated_import_answers_already_and_another_backup_conflicts() {
    let mut source = group(1);
    create(&mut source, "s");
    let backup = source.snapshot();
    let mut target = genesis_only(9);
    import(&mut target, 5, backup.clone(), 21, 0xaa);
    let after = target.snapshot();

    assert_eq!(
        import(&mut target, 8, backup.clone(), 33, 0xaa),
        StreamResponse::SnapshotImported {
            buckets: 1,
            streams: 1,
            history: id(21),
            already: true,
        }
    );
    assert_eq!(target.snapshot(), after);
    assert!(matches!(
        import(&mut target, 9, backup, 34, 0xbb),
        StreamResponse::Error {
            code: StreamErrorCode::ImportConflict,
            ..
        }
    ));
    assert_eq!(target.snapshot(), after);
}

#[test]
fn a_group_that_applied_only_its_genesis_is_empty() {
    let mut machine = genesis_only(4);
    let mut source = group(1);
    create(&mut source, "s");
    assert!(matches!(
        import(&mut machine, 2, source.snapshot(), 5, 1),
        StreamResponse::SnapshotImported { already: false, .. }
    ));
}

#[test]
fn an_import_never_lowers_the_c7_counter_below_its_clock() {
    let mut source = group(1);
    create(&mut source, "s");
    let mut target = genesis_only(2);
    target.apply_entry(3, StreamCommand::ImportSnapshot {
        snapshot: Box::new(source.snapshot()),
        history: id(8),
        backup_digest: [1; 32],
        now_ms: 9_000,
    });
    assert_eq!(target.last_created_at_ms(), 9_000);
}

/// W5: a stream that has not been written since a history event reports
/// its token in the earlier history and its whole tail.
#[test]
fn head_names_the_previous_token_and_tail_after_an_event() {
    let mut machine = group(1);
    let stream_id = create(&mut machine, "s");
    assert_eq!(machine.stream_inherited(&stream_id), None);
    append(&mut machine, &stream_id, b"hello", None);
    let old = token(&machine, &stream_id);
    new_history(&mut machine, 10, 2, 0);
    let report = InheritedReport {
        previous_incarnation: old,
        offset: 5,
    };
    assert_eq!(machine.stream_inherited(&stream_id), Some(report));

    // The first write records the same values, and they survive it.
    append(&mut machine, &stream_id, b"world", None);
    assert_eq!(machine.stream_inherited(&stream_id), Some(report));
    // So does a close.
    machine.apply(StreamCommand::Close {
        stream_id: stream_id.clone(),
        stream_seq: None,
        producer: None,
        now_ms: 1,
    });
    assert_eq!(machine.stream_inherited(&stream_id), Some(report));
}

#[test]
fn two_events_report_the_history_the_stream_was_last_written_in() {
    let mut machine = group(1);
    let stream_id = create(&mut machine, "s");
    append(&mut machine, &stream_id, b"ab", None);
    let first = token(&machine, &stream_id);
    // No write between two events: the oldest token, the same tail.
    new_history(&mut machine, 10, 2, 0);
    new_history(&mut machine, 11, 3, 0);
    assert_eq!(
        machine.stream_inherited(&stream_id),
        Some(InheritedReport {
            previous_incarnation: first,
            offset: 2,
        })
    );
    // A write, then an event: the history between them.
    append(&mut machine, &stream_id, b"cd", None);
    let third = token(&machine, &stream_id);
    new_history(&mut machine, 12, 4, 0);
    assert_eq!(
        machine.stream_inherited(&stream_id),
        Some(InheritedReport {
            previous_incarnation: third,
            offset: 4,
        })
    );
}

#[test]
fn an_import_reports_the_source_token_and_the_restored_tail() {
    let mut source = group(1);
    let stream_id = create(&mut source, "s");
    append(&mut source, &stream_id, b"ab", None);
    // A write after an event: the source's current token is what the
    // import names, with the restored tail.
    new_history(&mut source, 10, 2, 0);
    append(&mut source, &stream_id, b"c", None);
    let source_token = token(&source, &stream_id);
    let mut target = genesis_only(9);
    import(&mut target, 5, source.snapshot(), 21, 1);
    assert_eq!(
        target.stream_inherited(&stream_id),
        Some(InheritedReport {
            previous_incarnation: source_token,
            offset: 3,
        })
    );
}

/// #581 section 4.8 and extensions section 2.4: two rewrites with no write
/// between them report the token from before both. The source went through
/// an event and was not written since: the import names the token before
/// that event, as a second event on the source itself would.
#[test]
fn an_import_of_a_stream_not_written_since_the_sources_event_names_the_token_before_it() {
    let mut source = group(1);
    let stream_id = create(&mut source, "s");
    append(&mut source, &stream_id, b"abc", None);
    let before = token(&source, &stream_id);
    new_history(&mut source, 10, 2, 0);
    let mut target = genesis_only(9);
    import(&mut target, 5, source.snapshot(), 21, 1);
    assert_eq!(
        target.stream_inherited(&stream_id),
        Some(InheritedReport {
            previous_incarnation: before,
            offset: 3,
        })
    );
    // The same as the source's own second event.
    new_history(&mut source, 11, 3, 0);
    assert_eq!(
        source.stream_inherited(&stream_id),
        target.stream_inherited(&stream_id)
    );
}

/// A double restore with no write between: the backup of a node that was
/// itself restored names the token from before the first restore, A, with
/// the offset X it inherited.
#[test]
fn a_double_restore_without_a_write_between_names_the_first_token() {
    let mut source = group(1);
    let stream_id = create(&mut source, "s");
    append(&mut source, &stream_id, b"abc", None);
    let a = token(&source, &stream_id);
    let mut restored = genesis_only(9);
    import(&mut restored, 5, source.snapshot(), 21, 1);
    assert_eq!(
        restored.stream_inherited(&stream_id),
        Some(InheritedReport {
            previous_incarnation: a,
            offset: 3,
        })
    );
    let mut again = genesis_only(8);
    import(&mut again, 5, restored.snapshot(), 22, 2);
    assert_ne!(token(&again, &stream_id), token(&restored, &stream_id));
    assert_eq!(
        again.stream_inherited(&stream_id),
        Some(InheritedReport {
            previous_incarnation: a,
            offset: 3,
        })
    );
}

/// A double restore with a write between: the second restore names the
/// token the first restore's node served the write under, B, with its tail.
#[test]
fn a_double_restore_with_a_write_between_names_the_restored_token() {
    let mut source = group(1);
    let stream_id = create(&mut source, "s");
    append(&mut source, &stream_id, b"abc", None);
    let mut restored = genesis_only(9);
    import(&mut restored, 5, source.snapshot(), 21, 1);
    append(&mut restored, &stream_id, b"d", None);
    let b = token(&restored, &stream_id);
    let mut again = genesis_only(8);
    import(&mut again, 5, restored.snapshot(), 22, 2);
    assert_eq!(
        again.stream_inherited(&stream_id),
        Some(InheritedReport {
            previous_incarnation: b,
            offset: 4,
        })
    );
}

#[test]
fn a_recreate_after_an_event_reports_nothing() {
    let mut machine = group(1);
    let stream_id = create(&mut machine, "s");
    append(&mut machine, &stream_id, b"abc", None);
    new_history(&mut machine, 10, 2, 0);
    assert!(machine.stream_inherited(&stream_id).is_some());
    machine.apply(StreamCommand::DeleteStream {
        stream_id: stream_id.clone(),
        now_ms: 0,
    });
    create(&mut machine, "s");
    assert_eq!(machine.stream_inherited(&stream_id), None);
}

#[test]
fn history_state_survives_a_snapshot_round_trip() {
    let mut machine = group(1);
    let stream_id = create(&mut machine, "s");
    ack(append(&mut machine, &stream_id, b"ab", Some((0, 0))));
    new_history(&mut machine, 10, 2, 0);
    append(&mut machine, &stream_id, b"cd", None);
    let snapshot = machine.snapshot();
    let restored = StreamStateMachine::restore(snapshot.clone()).expect("restore");
    assert_eq!(restored.snapshot(), snapshot);
    assert_eq!(restored.history(), machine.history());
    assert_eq!(
        restored.stream_inherited(&stream_id),
        machine.stream_inherited(&stream_id)
    );
    assert_eq!(
        restored.stream_incarnation(&stream_id),
        machine.stream_incarnation(&stream_id)
    );
}

/// A client of the proptest: one producer per stream, which claims a new
/// epoch whenever it is refused.
#[derive(Debug, Clone, Copy, Default)]
struct Client {
    epoch: u64,
    seq: u64,
}

#[derive(Debug, Clone)]
enum Op {
    /// An append by the stream's producer, carrying the current token, a
    /// token the client remembers from before, or none.
    Write {
        stream: usize,
        body: u8,
        token: TokenChoice,
    },
    /// The same write again.
    Repeat { stream: usize },
    /// An accepted loss: the group goes back `back` steps and starts a new
    /// history.
    Loss { back: usize },
    /// A restore: a fresh group imports the current state.
    Restore,
    /// Delete and create the stream again.
    Recreate { stream: usize },
}

#[derive(Debug, Clone, Copy)]
enum TokenChoice {
    Current,
    Remembered,
    None,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0..2usize, any::<u8>(), prop_oneof![
            Just(TokenChoice::Current),
            Just(TokenChoice::Remembered),
            Just(TokenChoice::None),
        ])
            .prop_map(|(stream, body, token)| Op::Write { stream, body, token }),
        2 => (0..2usize).prop_map(|stream| Op::Repeat { stream }),
        1 => (0..6usize).prop_map(|back| Op::Loss { back }),
        1 => Just(Op::Restore),
        1 => (0..2usize).prop_map(|stream| Op::Recreate { stream }),
    ]
}

/// What every read and `HEAD` told a client, by stream and token.
#[derive(Debug, Default)]
struct Oracle {
    /// The longest bytes served under each `(stream, token)`.
    served: BTreeMap<(usize, u64), Vec<u8>>,
    /// The `(created_at_ms, history)` each `(stream, token)` names.
    owners: BTreeMap<(usize, u64), (u64, u64)>,
    /// Producer epochs a history event closed, by stream and the token the
    /// stream had right after the event, with the last seq that applied:
    /// no seq above it may apply under that token.
    fenced: Vec<(usize, u64, u64, u64)>,
}

impl Oracle {
    /// Records what the machine serves now, and checks it against what was
    /// served before under the same token. Returns the first contradiction.
    fn observe(
        &mut self,
        machine: &StreamStateMachine,
        streams: &[BucketStreamId],
    ) -> Result<(), String> {
        for (index, stream_id) in streams.iter().enumerate() {
            let Some(token) = machine.stream_incarnation(stream_id) else {
                continue;
            };
            let created_at_ms = machine.head(stream_id).expect("a stream").created_at_ms;
            let owner = (created_at_ms, machine.history_id());
            if let Some(previous) = self.owners.insert((index, token), owner)
                && previous != owner
            {
                return Err(format!(
                    "token {token} names {previous:?} and {owner:?} for stream {index}"
                ));
            }
            let now = bytes(machine, stream_id);
            if let Some(before) = self.served.get(&(index, token))
                && !now.starts_with(before)
            {
                return Err(format!(
                    "stream {index} under token {token} served {before:?}, now {now:?}"
                ));
            }
            if let Some(report) = machine.stream_inherited(stream_id) {
                let offset = usize::try_from(report.offset).map_err(|_overflow| "a huge offset")?;
                let previous = self
                    .served
                    .get(&(index, report.previous_incarnation))
                    .ok_or_else(|| format!("stream {index} inherits from a token never served"))?;
                if previous.get(..offset) != now.get(..offset) {
                    return Err(format!(
                        "stream {index} inherits {offset} bytes of {previous:?}, serves {now:?}"
                    ));
                }
            }
            self.served.insert((index, token), now);
        }
        Ok(())
    }
}

/// Drives `ops` and checks the oracle after each. `with_event: false` is the
/// negative control: a loss without its history event.
fn run(ops: &[Op], with_event: bool) -> Result<(), String> {
    let mut machine = group(1);
    let streams = [create(&mut machine, "s0"), create(&mut machine, "s1")];
    let mut clients = [Client::default(); 2];
    let mut remembered: [Option<u64>; 2] = [None; 2];
    let mut last: [Option<StreamCommand>; 2] = [None, None];
    let mut past: Vec<StreamSnapshot> = vec![machine.snapshot()];
    let mut next_history = 2u64;
    let mut index = 10u64;
    let mut oracle = Oracle::default();
    oracle.observe(&machine, &streams)?;
    for op in ops {
        index = index.saturating_add(1);
        match op {
            Op::Write {
                stream,
                body,
                token,
            } => {
                let stream_id = &streams[*stream];
                let client = clients[*stream];
                let command = append_command(stream_id, &[*body], Some((client.epoch, client.seq)));
                let command = match token {
                    TokenChoice::Current => match machine.stream_incarnation(stream_id) {
                        Some(token) => tokened(token, command),
                        None => command,
                    },
                    TokenChoice::Remembered => match remembered[*stream] {
                        Some(token) => tokened(token, command),
                        None => command,
                    },
                    TokenChoice::None => command,
                };
                if let Some(token) = machine.stream_incarnation(stream_id) {
                    remembered[*stream] = Some(token);
                }
                last[*stream] = Some(command.clone());
                let token_before = machine.stream_incarnation(stream_id);
                let response = machine.apply_entry(index, command.clone());
                check_fence(&oracle, *stream, token_before, &command, &response)?;
                clients[*stream] = next_client(client, &response);
            }
            Op::Repeat { stream } => {
                let Some(command) = last[*stream].clone() else {
                    continue;
                };
                let token_before = machine.stream_incarnation(&streams[*stream]);
                let response = machine.apply_entry(index, command.clone());
                check_fence(&oracle, *stream, token_before, &command, &response)?;
            }
            Op::Loss { back } => {
                let restored = past
                    .get(past.len().saturating_sub(back.saturating_add(1)))
                    .cloned()
                    .unwrap_or_else(|| past[0].clone());
                machine =
                    StreamStateMachine::restore(restored).map_err(|error| error.to_string())?;
                if with_event {
                    new_history(&mut machine, index, next_history, 0);
                    next_history = next_history.saturating_add(1);
                    fence(&mut oracle, &machine, &streams);
                }
            }
            Op::Restore => {
                let mut target = genesis_only(next_history);
                next_history = next_history.saturating_add(1);
                if with_event {
                    import(&mut target, index, machine.snapshot(), next_history, 1);
                    next_history = next_history.saturating_add(1);
                    machine = target;
                    fence(&mut oracle, &machine, &streams);
                } else {
                    machine = StreamStateMachine::restore(machine.snapshot())
                        .map_err(|error| error.to_string())?;
                }
            }
            Op::Recreate { stream } => {
                let stream_id = &streams[*stream];
                machine.apply_entry(index, StreamCommand::DeleteStream {
                    stream_id: stream_id.clone(),
                    now_ms: 0,
                });
                if !matches!(
                    create_at(&mut machine, &stream_id.stream_id, 1),
                    StreamResponse::Created { .. }
                ) {
                    return Err("recreate refused".to_owned());
                }
                // The client keeps its epochs: they only grow, so a key a
                // fence closed is never claimed again.
            }
        }
        oracle.observe(&machine, &streams)?;
        past.push(machine.snapshot());
    }
    Ok(())
}

fn producer_key(command: &StreamCommand) -> Client {
    match command {
        StreamCommand::IfIncarnation { command, .. } => producer_key(command),
        StreamCommand::Append {
            producer: Some(producer),
            ..
        } => Client {
            epoch: producer.producer_epoch,
            seq: producer.producer_seq,
        },
        other => panic!("not a producer write: {other:?}"),
    }
}

/// After a history event, every record's epoch is closed at its `last`.
fn fence(oracle: &mut Oracle, machine: &StreamStateMachine, streams: &[BucketStreamId]) {
    for (index, stream_id) in streams.iter().enumerate() {
        if let (Some(token), Some(record)) = (
            machine.stream_incarnation(stream_id),
            machine
                .stream_slot(stream_id)
                .and_then(|slot| slot.producers.get("p")),
        ) {
            oracle
                .fenced
                .push((index, token, record.epoch, record.last_seq));
        }
    }
}

/// No pre-rewind key applies after a rewind: a write that names a token of
/// an earlier history never applies (G1), and no seq above a fenced
/// epoch's last applies (G2, row 11).
fn check_fence(
    oracle: &Oracle,
    stream: usize,
    token_before: Option<u64>,
    command: &StreamCommand,
    response: &StreamResponse,
) -> Result<(), String> {
    let applied = matches!(response, StreamResponse::Appended {
        producer: Some(ProducerAck {
            delivery: ProducerDelivery::Applied(_),
            ..
        }),
        ..
    });
    if !applied {
        return Ok(());
    }
    if let StreamCommand::IfIncarnation { incarnation, .. } = command
        && Some(*incarnation) != token_before
    {
        return Err(format!(
            "stream {stream}: a write naming token {incarnation} applied under {token_before:?}"
        ));
    }
    let key = producer_key(command);
    if oracle.fenced.iter().any(|(fenced, token, epoch, last)| {
        *fenced == stream && Some(*token) == token_before && *epoch == key.epoch && key.seq > *last
    }) {
        return Err(format!(
            "stream {stream}: key ({}, {}) applied above a history fence",
            key.epoch, key.seq
        ));
    }
    Ok(())
}

/// The client's next key after `response`: the seq after the epoch's
/// highest applied one after a receipt, the expected seq after a gap, and a
/// claim above every epoch it knows after any other refusal. Its epochs
/// only grow.
fn next_client(client: Client, response: &StreamResponse) -> Client {
    match response {
        StreamResponse::Appended {
            producer: Some(ack),
            ..
        } if ack.epoch == client.epoch => Client {
            epoch: client.epoch,
            seq: ack.seq.saturating_add(1),
        },
        StreamResponse::Error { context, .. } => match context.as_slice() {
            [StreamErrorContext::ProducerSeqConflict { expected_seq, .. }] => Client {
                epoch: client.epoch,
                seq: *expected_seq,
            },
            [
                StreamErrorContext::Producer(
                    ProducerRefusal::EpochClosed { epoch, .. }
                    | ProducerRefusal::StaleEpoch { epoch }
                    | ProducerRefusal::Expired { epoch }
                    | ProducerRefusal::KeyConflict { epoch },
                ),
            ] => Client {
                epoch: client.epoch.max(*epoch).saturating_add(1),
                seq: 0,
            },
            _ => Client {
                epoch: client.epoch.saturating_add(1),
                seq: 0,
            },
        },
        _ => Client {
            epoch: client.epoch.saturating_add(1),
            seq: 0,
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Across history events and restores: a token always names one
    /// incarnation in one history, no read under a token contradicts an
    /// earlier read under it, every inherited prefix is what the previous
    /// token served, and no producer key above a history fence applies.
    #[test]
    fn tokens_stay_unique_and_fenced_keys_never_apply(
        ops in proptest::collection::vec(op_strategy(), 1..60)
    ) {
        prop_assert_eq!(run(&ops, true), Ok(()));
    }
}

/// Negative control: a loss without its history event serves different
/// bytes under a token that already served others, and the oracle sees it.
#[test]
fn a_loss_without_its_event_is_caught() {
    let ops = [
        Op::Write {
            stream: 0,
            body: 1,
            token: TokenChoice::None,
        },
        Op::Loss { back: 1 },
        Op::Write {
            stream: 0,
            body: 2,
            token: TokenChoice::None,
        },
    ];
    assert_eq!(run(&ops, true), Ok(()));
    assert!(run(&ops, false).is_err());
}

/// Negative control for the fence: without the event, the pre-loss key
/// applies again.
#[test]
fn the_fence_is_what_refuses_the_pre_loss_key() {
    let mut fenced = group(1);
    let stream_id = create(&mut fenced, "s");
    ack(append(&mut fenced, &stream_id, b"a", Some((0, 0))));
    let before = fenced.snapshot();
    ack(append(&mut fenced, &stream_id, b"b", Some((0, 1))));
    // The loss takes the group back before seq 1.
    let mut unfenced = StreamStateMachine::restore(before.clone()).expect("restore");
    let mut fenced = StreamStateMachine::restore(before).expect("restore");
    new_history(&mut fenced, 20, 2, 0);
    assert!(matches!(
        ack(append(&mut unfenced, &stream_id, b"c", Some((0, 1)))).delivery,
        ProducerDelivery::Applied(_)
    ));
    assert!(matches!(
        producer_refusal(append(&mut fenced, &stream_id, b"c", Some((0, 1)))).1,
        ProducerRefusal::EpochClosed {
            by: ClosedBy::History,
            ..
        }
    ));
}

#[test]
fn history_ids_drawn_for_events_differ() {
    let ids = (0..64)
        .map(|_| crate::history::draw_history_id())
        .collect::<Vec<_>>();
    let mut seen = HashMap::new();
    for id in ids {
        assert!(seen.insert(id, ()).is_none(), "a repeated history id");
    }
}

/// #613: a history event records the history it replaced and the index it
/// kept, and one that names another history than the group's current one
/// applies nothing.
#[test]
fn a_history_event_records_what_it_replaced_and_refuses_a_stale_predecessor() {
    let mut machine = group(7);
    let raced = machine.apply_entry(5, StreamCommand::NewHistory {
        history: id(9),
        now_ms: 0,
        previous: 8,
        kept: 3,
        leader: crate::HistoryLeader {
            term: 0,
            node_id: 0,
        },
    });
    assert!(matches!(raced, StreamResponse::Error {
        code: crate::StreamErrorCode::HistoryMismatch,
        ..
    }));
    assert_eq!(machine.history_id(), 7);
    assert!(matches!(
        machine.apply_entry(6, StreamCommand::NewHistory {
            history: id(9),
            now_ms: 0,
            previous: 7,
            kept: 3,
            leader: crate::HistoryLeader {
                term: 0,
                node_id: 0,
            },
        }),
        StreamResponse::HistoryStarted { .. }
    ));
    let event = machine.history().expect("an event");
    assert_eq!((event.previous, event.kept, event.event_index), (7, 3, 6));
}
