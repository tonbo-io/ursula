//! The nonces of new cold object names under madsim (C2a, K1). They come
//! from a per-thread generator seeded with the run's seed, never from
//! madsim's own generator, so a seed replays its names, two seeds name their
//! objects apart, and the nodes of one simulation, which madsim runs on one
//! thread, draw from one shared sequence and never share a name.

use std::collections::BTreeSet;
use std::time::Duration;

use ursula_shard::BucketStreamId;

use super::run_with_madsim;
use super::sim_test_guard;

/// The draws each node makes, one name of every kind per draw.
const DRAWS: usize = 16;

/// One name of every kind a node draws: an exclusive chunk, a pack, an
/// external payload and a snapshot part id.
fn draw_names() -> [String; 4] {
    let stream_id = BucketStreamId::new("tenant", "s");
    [
        ursula_runtime::new_cold_chunk_path_in_generation(&stream_id, 1, 0, 4),
        ursula_runtime::new_cold_pack_path("tenant", 1),
        ursula_runtime::new_external_payload_path(&stream_id),
        ursula_runtime::new_snapshot_part_id(),
    ]
}

/// The names two nodes of one simulation under `seed` draw, interleaved by
/// simulated sleeps.
fn two_node_names(seed: u64) -> [Vec<String>; 2] {
    run_with_madsim(seed, async {
        let [a, b] = {
            let handle = madsim::runtime::Handle::current();
            ["names-a", "names-b"].map(|name| {
                handle.create_node().name(name).build().spawn(async {
                    let mut names = Vec::new();
                    for _ in 0..DRAWS {
                        names.extend(draw_names());
                        madsim::time::sleep(Duration::from_millis(1)).await;
                    }
                    names
                })
            })
        };
        [
            a.await.expect("node a draws its names"),
            b.await.expect("node b draws its names"),
        ]
    })
}

/// A seed draws the same names on every run, another seed draws others, and
/// the two nodes of a run never share a name. Outside a runtime the names
/// are deterministic too, so a madsim build of a test that names objects
/// without a runtime replays.
#[test]
fn cold_object_names_follow_the_seed_replays() {
    let _guard = sim_test_guard();
    let first = two_node_names(7);
    assert_eq!(two_node_names(7), first, "seed 7 draws its names again");

    let [a, b] = &first;
    let a = a.iter().collect::<BTreeSet<_>>();
    let b = b.iter().collect::<BTreeSet<_>>();
    assert_eq!(a.len(), first[0].len(), "node a never repeats a name");
    assert_eq!(b.len(), first[1].len(), "node b never repeats a name");
    assert!(a.is_disjoint(&b), "the nodes of one run share no name");

    let other = two_node_names(8);
    let seven = first.iter().flatten().collect::<BTreeSet<_>>();
    assert!(
        other.iter().flatten().all(|name| !seven.contains(name)),
        "seed 8 names its objects apart from seed 7"
    );

    let outside = || {
        std::thread::spawn(draw_names)
            .join()
            .expect("draw names outside a runtime")
    };
    assert_eq!(outside(), outside(), "names outside a runtime replay");
}
