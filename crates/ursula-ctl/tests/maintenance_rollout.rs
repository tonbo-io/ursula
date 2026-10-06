//! Chart controller sequencing with actual shell/native CLI and synthetic RPCs.
//! Kubernetes UID/resourceVersion and process fences are enforced by the fixture;
//! synthetic proofs do not establish live Raft or provider-host recovery.

#[test]
fn shared_rollout_consumer_conflict_resume_and_retirement() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let output = std::process::Command::new("python3")
        .arg(root.join("scripts/maintenance_rollout_test.py"))
        .env("URSULA_CTL_BINARY", env!("CARGO_BIN_EXE_ursulactl"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
