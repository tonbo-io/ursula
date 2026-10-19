//! The nonces of new cold object names under madsim (C2a, K1). They come
//! from a generator seeded with the seed of the runtime that draws them,
//! never from madsim's own generator. Its sequence starts again with each
//! runtime, so a seed replays its names even on a thread that ran it
//! before, and two seeds draw different names. madsim runs every node of a
//! simulation on one thread, so the nodes draw from one sequence.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
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

/// What two nodes of one simulation draw: each node's names, and the order
/// of their draws by node index.
#[derive(Debug, PartialEq, Eq)]
struct TwoNodeNames {
    names: [Vec<String>; 2],
    order: Vec<usize>,
}

/// The names two nodes of one simulation under `seed` draw, interleaved by
/// simulated sleeps.
fn two_node_names(seed: u64) -> TwoNodeNames {
    run_with_madsim(seed, async {
        let order = Arc::new(Mutex::new(Vec::new()));
        let [a, b] = {
            let handle = madsim::runtime::Handle::current();
            [0, 1].map(|node| {
                let order = Arc::clone(&order);
                handle
                    .create_node()
                    .name(format!("names-{node}"))
                    .build()
                    .spawn(async move {
                        let mut names = Vec::new();
                        for _ in 0..DRAWS {
                            names.extend(draw_names());
                            order.lock().expect("the draw order").push(node);
                            madsim::time::sleep(Duration::from_millis(1)).await;
                        }
                        names
                    })
            })
        };
        let names = [
            a.await.expect("node a draws its names"),
            b.await.expect("node b draws its names"),
        ];
        let order = order.lock().expect("the draw order").clone();
        TwoNodeNames { names, order }
    })
}

/// A seed draws the same names on every run, another seed draws others, and
/// the two nodes of a run, whose draws interleave on one sequence, share no
/// name. Outside a runtime the names are deterministic too, so a madsim
/// build of a test that names objects without a runtime replays.
#[test]
fn cold_object_names_follow_the_seed_replays() {
    let _guard = sim_test_guard();
    let first = two_node_names(7);
    assert_eq!(two_node_names(7), first, "seed 7 draws its names again");

    assert!(
        first
            .order
            .windows(2)
            .filter(|pair| pair[0] != pair[1])
            .count()
            > 1,
        "the nodes' draws interleave: {:?}",
        first.order
    );
    let [a, b] = &first.names;
    let a = a.iter().collect::<BTreeSet<_>>();
    let b = b.iter().collect::<BTreeSet<_>>();
    assert_eq!(a.len(), first.names[0].len(), "node a never repeats a name");
    assert_eq!(b.len(), first.names[1].len(), "node b never repeats a name");
    assert!(a.is_disjoint(&b), "the nodes of one run share no name");

    let other = two_node_names(8);
    let seven = first.names.iter().flatten().collect::<BTreeSet<_>>();
    assert!(
        other
            .names
            .iter()
            .flatten()
            .all(|name| !seven.contains(name)),
        "seed 8 names its objects apart from seed 7"
    );

    let outside = || {
        std::thread::spawn(draw_names)
            .join()
            .expect("draw names outside a runtime")
    };
    assert_eq!(outside(), outside(), "names outside a runtime replay");
}

/// Runs seed 7 twice, one runtime after the other on the calling thread,
/// and returns the names each run draws. Only this test builds a runtime
/// outside `run_isolated`: it needs the reuse of a thread that
/// `run_isolated` rules out for every simulation.
fn two_runtimes_on_one_thread() -> [Vec<[String; 4]>; 2] {
    [(), ()].map(|()| {
        madsim::runtime::Runtime::with_seed_and_config(7, madsim::Config::default())
            .block_on(async { (0..DRAWS).map(|_| draw_names()).collect::<Vec<_>>() })
    })
}

/// Two runtimes of one seed, one after the other on one thread, draw the
/// same names: the sequence starts again with each runtime, not with each
/// seed or thread.
#[test]
fn cold_object_names_restart_with_each_runtime_replays() {
    let _guard = sim_test_guard();
    let [first, second] = std::thread::spawn(two_runtimes_on_one_thread)
        .join()
        .expect("two runtimes on one thread");
    assert_eq!(
        first, second,
        "the second runtime of seed 7 draws its names again"
    );
}

/// Naming objects draws nothing from madsim's generator: a run that names
/// objects and then draws from it gets the draw a run of the same seed that
/// names nothing gets.
#[test]
fn cold_object_names_draw_nothing_from_madsim_rng_replays() {
    let _guard = sim_test_guard();
    let after_names = run_with_madsim(7, async {
        for _ in 0..DRAWS {
            draw_names();
        }
        madsim::rand::random::<u64>()
    });
    let alone = run_with_madsim(7, async { madsim::rand::random::<u64>() });
    assert_eq!(after_names, alone, "naming objects shifted madsim's draws");
}
