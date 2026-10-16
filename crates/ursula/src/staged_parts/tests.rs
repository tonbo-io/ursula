use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ursula_headers::StagedPartsLimit;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::SweptPartsObserver;
use ursula_runtime::new_snapshot_part_id;
use ursula_runtime::snapshot_part_path;
use ursula_shard::BucketStreamId;

use super::MAX_TRACKED_STREAMS;
use super::RELIST_AFTER;
use super::RETIRED_MIN_AGE_MS;
use super::StagedOwner;
use super::StagedParts;
use super::StagedReservation;
use crate::snapshot_parts::PartsRefusal;

fn memory() -> ColdStoreHandle {
    Arc::new(ColdStore::memory().expect("memory"))
}

fn owner(stream_id: &BucketStreamId, incarnation: u64, term: u64) -> StagedOwner<'_> {
    StagedOwner {
        stream_id,
        raft_group_id: 0,
        incarnation,
        term,
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_millis(),
    )
    .expect("fits")
}

/// Writes one part object of `len` bytes for `owner`, as a node stages it.
async fn write_part(cold_store: &ColdStoreHandle, owner: StagedOwner<'_>, len: usize) {
    let path = snapshot_part_path(
        owner.stream_id,
        owner.raft_group_id,
        owner.incarnation,
        &new_snapshot_part_id(),
    );
    cold_store
        .write_chunk(&path, &vec![1; len])
        .await
        .expect("stage part");
}

/// Stages one part of `len` bytes as a node does: reserves it, writes it,
/// and counts it staged.
async fn stage(
    staged: &StagedParts,
    cold_store: &ColdStoreHandle,
    owner: StagedOwner<'_>,
    len: usize,
) {
    let reservation = staged
        .reserve(cold_store, owner, len as u64, now_ms())
        .await
        .expect("room");
    write_part(cold_store, owner, len).await;
    reservation.staged(len as u64);
}

fn refused(result: Result<StagedReservation, PartsRefusal>) -> StagedPartsLimit {
    match result {
        Err(PartsRefusal::StagedLimit { limit, .. }) => limit,
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("reserved"),
    }
}

/// Holds every prefix listing once it is read, until [`ListingGate::open`].
pub(crate) struct ListingGate {
    permits: Arc<tokio::sync::Semaphore>,
    waiting: tokio::sync::watch::Sender<usize>,
}

impl ListingGate {
    pub(crate) fn on(cold_store: &ColdStore) -> Self {
        let permits = Arc::new(tokio::sync::Semaphore::new(0));
        let waiting = tokio::sync::watch::Sender::new(0_usize);
        cold_store.set_fault_policy(|context| {
            (context.operation == ursula_runtime::ColdStoreOperation::ListPrefix)
                .then(|| ursula_runtime::ColdStoreFaultEffect::delay(Duration::from_millis(1)))
        });
        let (gate, count) = (Arc::clone(&permits), waiting.clone());
        cold_store.set_delay_fn(move |_| {
            let (gate, count) = (Arc::clone(&gate), count.clone());
            async move {
                count.send_modify(|waiting| *waiting = waiting.saturating_add(1));
                if let Ok(permit) = gate.acquire().await {
                    permit.forget();
                }
            }
        });
        Self { permits, waiting }
    }

    /// Waits until `listings` listings wait at the gate.
    pub(crate) async fn reached(&self, listings: usize) {
        self.waiting
            .subscribe()
            .wait_for(|waiting| *waiting >= listings)
            .await
            .expect("the gate lives");
    }

    pub(crate) fn open(&self) {
        self.permits.add_permits(1 << 20);
    }
}

/// A leader change: the node that leads in a later term lists, so it counts
/// the parts another leader staged meanwhile, and a node that leads again
/// after another did counts them too, although it counted the stream
/// before.
#[tokio::test]
async fn a_new_term_lists_what_other_leaders_staged() {
    let cold_store = memory();
    let stream_id = BucketStreamId::new("b", "s");
    let a = StagedParts::for_test(3, 1_000);
    let b = StagedParts::for_test(3, 1_000);
    stage(&a, &cold_store, owner(&stream_id, 7, 1), 10).await;
    stage(&b, &cold_store, owner(&stream_id, 7, 2), 10).await;
    // A's tally of term 1 counts one part, the listing of term 3 two.
    stage(&a, &cold_store, owner(&stream_id, 7, 3), 10).await;
    assert_eq!(
        refused(
            a.reserve(&cold_store, owner(&stream_id, 7, 3), 10, now_ms())
                .await
        ),
        StagedPartsLimit::Parts(3)
    );
}

/// A delete and a recreate do not reset the bound: the parts of the old
/// incarnation count while they are young, and a listing deletes them once
/// they are old enough that no newer incarnation can own them.
#[tokio::test]
async fn an_earlier_incarnations_parts_count_until_a_listing_deletes_them() {
    let cold_store = memory();
    let stream_id = BucketStreamId::new("b", "s");
    let staged = StagedParts::for_test(2, 1_000);
    stage(&staged, &cold_store, owner(&stream_id, 7, 1), 10).await;
    stage(&staged, &cold_store, owner(&stream_id, 7, 1), 10).await;
    // Recreated: incarnation 8, a moment later.
    assert_eq!(
        refused(
            staged
                .reserve(&cold_store, owner(&stream_id, 8, 1), 10, now_ms())
                .await
        ),
        StagedPartsLimit::Parts(2),
        "the young parts of incarnation 7 still count"
    );
    let later = now_ms().saturating_add(RETIRED_MIN_AGE_MS);
    drop(
        staged
            .reserve(&cold_store, owner(&stream_id, 8, 1), 10, later)
            .await
            .expect("the retired parts are deleted"),
    );
    assert_eq!(staged.counted(&stream_id), Some((0, 0)));
    let dir = ursula_runtime::snapshot_part_dir("b", 0);
    assert!(
        cold_store
            .list_file_names(&dir)
            .await
            .expect("list")
            .is_empty()
    );
}

/// A part counts its reservation while it uploads, so concurrent parts
/// cannot pass the byte limit together, and a part that fails gives its
/// reservation back.
#[tokio::test]
async fn uploading_parts_count_their_reservation() {
    let cold_store = memory();
    let stream_id = BucketStreamId::new("b", "s");
    let staged = StagedParts::for_test(10, 100);
    let first = staged
        .reserve(&cold_store, owner(&stream_id, 1, 0), 60, now_ms())
        .await
        .expect("room");
    assert_eq!(
        refused(
            staged
                .reserve(&cold_store, owner(&stream_id, 1, 0), 60, now_ms())
                .await
        ),
        StagedPartsLimit::Bytes(100)
    );
    drop(first);
    stage(&staged, &cold_store, owner(&stream_id, 1, 0), 40).await;
    drop(
        staged
            .reserve(&cold_store, owner(&stream_id, 1, 0), 60, now_ms())
            .await
            .expect("40 + 60 bytes fit"),
    );
}

/// M2 of the #622 review, one direction: a part that finishes uploading
/// while a listing runs, after the listing read the store, is added on top
/// of the listing, not lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_staged_during_a_listing_counts() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory"));
    let handle: ColdStoreHandle = cold_store.clone();
    let stream_id = BucketStreamId::new("b", "s");
    let staged = StagedParts::for_test(10, 1_000);
    stage(&staged, &handle, owner(&stream_id, 1, 1), 10).await;
    let uploading = staged
        .reserve(&handle, owner(&stream_id, 1, 1), 20, now_ms())
        .await
        .expect("room");
    let gate = ListingGate::on(&cold_store);
    // A new term lists; the listing has read the store and waits.
    let listing = tokio::spawn({
        let (staged, handle, stream_id) = (staged.clone(), handle.clone(), stream_id.clone());
        async move {
            drop(
                staged
                    .reserve(&handle, owner(&stream_id, 1, 2), 5, now_ms())
                    .await
                    .expect("room"),
            );
        }
    });
    gate.reached(1).await;
    write_part(&handle, owner(&stream_id, 1, 1), 20).await;
    uploading.staged(20);
    gate.open();
    listing.await.expect("listing");
    assert_eq!(
        staged.counted(&stream_id),
        Some((2, 30)),
        "the listing saw one part, and the part staged during it is added"
    );
}

/// M2 of the #622 review, the other direction: a reservation released
/// while a listing runs is not carried over by the listing, so it does not
/// stay counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_that_fails_during_a_listing_is_not_kept() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory"));
    let handle: ColdStoreHandle = cold_store.clone();
    let stream_id = BucketStreamId::new("b", "s");
    let staged = StagedParts::for_test(10, 1_000);
    let failing = staged
        .reserve(&handle, owner(&stream_id, 1, 1), 20, now_ms())
        .await
        .expect("room");
    let gate = ListingGate::on(&cold_store);
    let listing = tokio::spawn({
        let (staged, handle, stream_id) = (staged.clone(), handle.clone(), stream_id.clone());
        async move {
            staged
                .reserve(&handle, owner(&stream_id, 1, 2), 5, now_ms())
                .await
                .expect("room")
        }
    });
    gate.reached(1).await;
    drop(failing);
    gate.open();
    let reservation = listing.await.expect("listing");
    assert_eq!(
        staged.counted(&stream_id),
        Some((1, 5)),
        "only the reservation of the listing's own request"
    );
    drop(reservation);
    assert_eq!(staged.counted(&stream_id), Some((0, 0)));
}

/// The relisting gate, with its real interval on a paused clock: a stream
/// whose part the sweep reclaimed stays refused until its last listing is
/// a minute old, and the next part then lists and fits.
#[tokio::test(start_paused = true)]
async fn a_refused_part_lists_again_after_a_minute() {
    assert_eq!(RELIST_AFTER, Duration::from_secs(60));
    let cold_store = memory();
    let stream_id = BucketStreamId::new("b", "s");
    let staged = StagedParts::for_test_relisting_after(1, 1_000, RELIST_AFTER);
    stage(&staged, &cold_store, owner(&stream_id, 1, 0), 10).await;
    let dir = ursula_runtime::snapshot_part_dir("b", 0);
    for name in cold_store.list_file_names(&dir).await.expect("list") {
        cold_store
            .delete_chunk(&format!("{dir}{name}"))
            .await
            .expect("the sweep reclaims it");
    }
    assert_eq!(
        refused(
            staged
                .reserve(&cold_store, owner(&stream_id, 1, 0), 1, now_ms())
                .await
        ),
        StagedPartsLimit::Parts(1)
    );
    tokio::time::advance(Duration::from_secs(59)).await;
    refused(
        staged
            .reserve(&cold_store, owner(&stream_id, 1, 0), 1, now_ms())
            .await,
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    drop(
        staged
            .reserve(&cold_store, owner(&stream_id, 1, 0), 1, now_ms())
            .await
            .expect("listed again, the part fits"),
    );
}

/// A node tracks a bounded number of streams that no request holds. An
/// evicted stream lists again at its next part, and counts what it had. A
/// held slot is never evicted.
#[tokio::test]
async fn the_tracked_streams_are_bounded() {
    let cold_store = memory();
    let staged = StagedParts::for_test(1, 100);
    let first = BucketStreamId::new("b", "first");
    stage(&staged, &cold_store, owner(&first, 1, 0), 1).await;
    let held_stream = BucketStreamId::new("b", "held");
    let held = staged
        .reserve(&cold_store, owner(&held_stream, 1, 0), 1, now_ms())
        .await
        .expect("room");
    for index in 0..MAX_TRACKED_STREAMS {
        let stream_id = BucketStreamId::new("b", format!("s{index}"));
        drop(
            staged
                .reserve(&cold_store, owner(&stream_id, 1, 0), 1, now_ms())
                .await
                .expect("room"),
        );
    }
    assert_eq!(staged.tracks(&first), (MAX_TRACKED_STREAMS, false));
    assert!(staged.tracks(&held_stream).1, "a held slot stays");
    drop(held);
    assert_eq!(
        refused(
            staged
                .reserve(&cold_store, owner(&first, 1, 0), 1, now_ms())
                .await
        ),
        StagedPartsLimit::Parts(1),
        "evicted, it lists its part again"
    );
}

fn seed_of(entries: &[(&BucketStreamId, u64, u64)]) -> HashMap<String, (String, u64, u64)> {
    entries
        .iter()
        .map(|(stream_id, parts, bytes)| {
            (
                ursula_runtime::snapshot_part_stream_prefix(stream_id),
                (stream_id.bucket_id.clone(), *parts, *bytes),
            )
        })
        .collect()
}

/// Usage counts the staged bytes of the groups a node leads, in the term it
/// leads them in now. A group's totals are partial until its seed listing
/// finishes, and a stream listed meanwhile keeps its own count.
#[tokio::test]
async fn usage_sums_the_led_groups_per_bucket() {
    let cold_store = memory();
    let staged = StagedParts::for_test(10, 1_000);
    staged.lead_as(|_| Some(4));
    let a = BucketStreamId::new("a", "s");
    let a2 = BucketStreamId::new("a", "t");
    let b = BucketStreamId::new("b", "s");
    assert!(staged.groups().lead(0, 4));
    stage(&staged, &cold_store, owner(&a, 1, 4), 10).await;
    stage(&staged, &cold_store, owner(&a2, 1, 4), 5).await;
    let report = staged.report();
    assert_eq!(report.len(), 1);
    assert!(!report[0].counting_finished, "the seed has not finished");
    assert_eq!(report[0].buckets.get("a"), Some(&15));
    // The seed, which listed before `a` staged its part, finds `b` and an
    // older count of `a`, which keeps its own.
    staged
        .groups()
        .seeded(0, 4, seed_of(&[(&a, 0, 0), (&b, 1, 7)]));
    let report = staged.report();
    assert!(report[0].counting_finished);
    assert_eq!(report[0].buckets.get("a"), Some(&15));
    assert_eq!(report[0].buckets.get("b"), Some(&7));
    staged.lead_as(|_| Some(5));
    assert!(
        staged.report().is_empty(),
        "a node that leads a later term reports none of this term's"
    );
}

/// #648 re-review, race 1: a part arrives right after a leader change,
/// before the counting task starts the term. Its listing starts the term's
/// totals, so the seed that follows does not undercount it.
#[tokio::test]
async fn a_listing_before_the_seed_starts_the_terms_totals() {
    let cold_store = memory();
    let a = BucketStreamId::new("a", "s");
    let staged = StagedParts::for_test(10, 1_000);
    staged.lead_as(|_| Some(4));
    for _ in 0..3 {
        write_part(&cold_store, owner(&a, 1, 3), 10).await;
    }
    let reservation = staged
        .reserve(&cold_store, owner(&a, 1, 4), 10, now_ms())
        .await
        .expect("room");
    assert!(
        staged.groups().lead(0, 4),
        "the term is started, not seeded"
    );
    write_part(&cold_store, owner(&a, 1, 4), 10).await;
    reservation.staged(10);
    staged.groups().seeded(0, 4, seed_of(&[(&a, 4, 40)]));
    let report = staged.report();
    assert!(report[0].counting_finished);
    assert_eq!(report[0].buckets.get("a"), Some(&40));
}

/// #648 re-review, race 2: a discard empties a stream after the seed read
/// its directory. The zero it lists stays until the seed finishes, so the
/// seed's older count does not come back.
#[tokio::test]
async fn a_stream_emptied_during_the_seed_stays_empty() {
    let cold_store = memory();
    let a = BucketStreamId::new("a", "s");
    let staged = StagedParts::for_test(10, 1_000);
    staged.lead_as(|_| Some(1));
    write_part(&cold_store, owner(&a, 1, 1), 10).await;
    write_part(&cold_store, owner(&a, 1, 1), 10).await;
    assert!(staged.groups().lead(0, 1));
    let read_by_the_seed = seed_of(&[(&a, 2, 20)]);
    staged
        .discard(&cold_store, owner(&a, 1, 1), now_ms())
        .await
        .expect("discard");
    staged.groups().seeded(0, 1, read_by_the_seed);
    let report = staged.report();
    assert!(report[0].counting_finished);
    assert!(report[0].buckets.is_empty(), "{report:?}");
}

/// #648 re-review, race 3: a node that leads after the parts were staged
/// elsewhere seeds them, then commits them. Its commit lists the stream
/// after the delete, with no count of the stream of its own, or with one
/// from an older term, and the totals give the parts back.
#[tokio::test]
async fn a_commit_on_a_node_that_did_not_stage_the_parts_gives_them_back() {
    let cold_store = memory();
    let a = BucketStreamId::new("a", "s");
    let b = BucketStreamId::new("a", "t");
    let staged = StagedParts::for_test(10, 1_000);
    // `b` was counted in term 4, `a` never.
    staged.lead_as(|_| Some(4));
    stage(&staged, &cold_store, owner(&b, 1, 4), 10).await;
    write_part(&cold_store, owner(&a, 1, 3), 10).await;
    staged.lead_as(|_| Some(5));
    assert!(staged.groups().lead(0, 5));
    staged
        .groups()
        .seeded(0, 5, seed_of(&[(&a, 1, 10), (&b, 1, 10)]));
    assert_eq!(staged.report()[0].buckets.get("a"), Some(&20));
    let dir = ursula_runtime::snapshot_part_dir("a", 0);
    let names = cold_store.list_file_names(&dir).await.expect("list");
    for stream_id in [&a, &b] {
        let prefix = ursula_runtime::snapshot_part_stream_prefix(stream_id);
        let paths = names
            .iter()
            .filter(|name| name.starts_with(&prefix))
            .map(|name| format!("{dir}{name}"))
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 1);
        staged.delete_spent(&cold_store, stream_id, 0, paths).await;
    }
    assert!(
        staged.report()[0].buckets.is_empty(),
        "{:?}",
        staged.report()
    );
}

/// M1 of the #622 re-review: node A counted the stream at incarnation 7.
/// Another leader recreated it as incarnation 8 and staged a part, a while
/// ago. A commit on A that spends parts lists again, and that listing
/// deletes nothing of incarnation 8, although A last knew incarnation 7.
#[tokio::test]
async fn a_commits_listing_deletes_no_other_incarnations_parts() {
    let cold_store = memory();
    let stream_id = BucketStreamId::new("b", "s");
    let a = StagedParts::for_test(10, 1_000);
    stage(&a, &cold_store, owner(&stream_id, 7, 1), 10).await;
    let old_nanos = u128::from(now_ms().saturating_sub(10 * RETIRED_MIN_AGE_MS)) * 1_000_000;
    let b_part = snapshot_part_path(
        &stream_id,
        0,
        8,
        &format!("{old_nanos:032x}-{:016x}.part", 1),
    );
    cold_store
        .write_chunk(&b_part, b"staged on B")
        .await
        .expect("a part B staged");
    a.delete_spent(&cold_store, &stream_id, 0, Vec::new()).await;
    assert_eq!(
        cold_store.object_size(&b_part).await.expect("still staged"),
        11
    );
    assert_eq!(a.counted(&stream_id), Some((2, 21)));
}

/// m1 of the #622 re-review: a part uploading while its stream's tally is
/// marked stale and listed again still counts.
#[tokio::test]
async fn an_upload_survives_a_stale_tally() {
    let cold_store = memory();
    let stream_id = BucketStreamId::new("b", "s");
    let staged = StagedParts::for_test(2, 1_000);
    let uploading = staged
        .reserve(&cold_store, owner(&stream_id, 1, 1), 10, now_ms())
        .await
        .expect("room");
    staged
        .existing_slot(&stream_id)
        .expect("tracked")
        .mark_stale();
    stage(&staged, &cold_store, owner(&stream_id, 1, 1), 10).await;
    assert_eq!(
        refused(
            staged
                .reserve(&cold_store, owner(&stream_id, 1, 1), 1, now_ms())
                .await
        ),
        StagedPartsLimit::Parts(2),
        "the upload still counts"
    );
    drop(uploading);
}

/// #639: the sweep gives back what it reclaims, so usage drops without a
/// listing, and the stream's next part lists. A purge forgets the bucket.
#[tokio::test]
async fn the_sweep_and_a_purge_give_usage_back() {
    let cold_store = memory();
    let stream_id = BucketStreamId::new("b", "s");
    let staged = StagedParts::for_test(10, 1_000);
    staged.lead_as(|_| Some(1));
    assert!(staged.groups().lead(0, 1));
    staged.groups().seeded(0, 1, HashMap::new());
    stage(&staged, &cold_store, owner(&stream_id, 1, 1), 10).await;
    stage(&staged, &cold_store, owner(&stream_id, 1, 1), 5).await;
    let dir = ursula_runtime::snapshot_part_dir("b", 0);
    let names = cold_store.list_file_names(&dir).await.expect("list");
    let swept = format!("{dir}{}", names.first().expect("a part"));
    let bytes = cold_store.object_size(&swept).await.expect("size");
    cold_store.delete_chunk(&swept).await.expect("the sweep");
    staged.swept(&[ursula_runtime::SweptPart { path: swept, bytes }]);
    assert_eq!(staged.report()[0].buckets.get("b"), Some(&(15 - bytes)));
    stage(&staged, &cold_store, owner(&stream_id, 1, 1), 1).await;
    assert_eq!(
        staged.counted(&stream_id),
        Some((2, 16 - bytes)),
        "listed again"
    );
    staged.purged("b");
    assert!(staged.report()[0].buckets.is_empty());
}
