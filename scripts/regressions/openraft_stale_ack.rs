// Appended to OpenRaft's existing update_matching_test module by the runner.
#[test]
fn ursula_stale_ack_after_error_does_not_commit() {
    let mut eng = eng();
    eng.testing_new_leader();
    eng.output.take_commands();
    let mut rh = eng.replication_handler();
    rh.leader.progress.update_data_with(&3, |data| {
        data.inflight = Inflight::logs_since(Some(log_id(1, 1, 1)), InflightId::new(41))
    });
    // STREAM_ID
    rh.update_progress(
        3,
        // STREAM_ARG
        Err("replication stream stopped".to_owned()),
        Some(InflightId::new(41)),
    );
    assert!(rh.leader.progress.get(&3).inflight.is_none());
    let before = rh.leader.progress.get(&3).matching().cloned();
    rh.update_progress(
        3,
        // STREAM_ARG
        Ok(crate::replication::response::ReplicationResult(Ok(Some(
            log_id(2, 1, 4),
        )))),
        Some(InflightId::new(41)),
    );
    assert_eq!(before.as_ref(), rh.leader.progress.get(&3).matching());
    assert_eq!(None, rh.state.committed());
}

#[test]
fn ursula_wrong_inflight_ack_does_not_advance_matching() {
    let mut eng = eng();
    eng.testing_new_leader();
    eng.output.take_commands();
    let mut rh = eng.replication_handler();
    rh.leader.progress.update_data_with(&3, |data| {
        data.inflight = Inflight::logs_since(Some(log_id(1, 1, 1)), InflightId::new(42))
    });
    let before = rh.leader.progress.get(&3).matching().cloned();
    rh.update_matching(3, Some(log_id(2, 1, 4)), Some(InflightId::new(41)));
    assert_eq!(before.as_ref(), rh.leader.progress.get(&3).matching());
    assert_eq!(None, rh.state.committed());
}

#[test]
fn ursula_removed_stream_conflict_does_not_rewind_readded_peer() {
    let mut eng = eng();
    eng.testing_new_leader();
    eng.output.take_commands();
    let mut rh = eng.replication_handler();
    rh.update_matching(3, Some(log_id(2, 1, 3)), None);
    rh.leader.progress.update_data_with(&3, |data| {
        data.inflight = Inflight::logs_since(Some(log_id(2, 1, 3)), InflightId::new(42));
        data.stream_id = crate::progress::stream_id::StreamId::new(42);
    });
    rh.update_progress(
        3,
        // REMOVED_STREAM_ARG
        Ok(crate::replication::response::ReplicationResult(Err(
            log_id(2, 1, 2),
        ))),
        Some(InflightId::new(41)),
    );
    assert_eq!(
        Some(&log_id(2, 1, 3)),
        rh.leader.progress.get(&3).matching()
    );
}
#[test]
fn ursula_removed_stream_ack_does_not_rewind_readded_peer() {
    let mut eng = eng();
    eng.testing_new_leader();
    eng.output.take_commands();
    let mut rh = eng.replication_handler();
    rh.update_matching(3, Some(log_id(2, 1, 3)), None);
    rh.leader.progress.update_data_with(&3, |data| {
        data.inflight = Inflight::logs_since(Some(log_id(2, 1, 3)), InflightId::new(42));
        data.stream_id = crate::progress::stream_id::StreamId::new(42);
    });
    rh.update_progress(
        3,
        // REMOVED_STREAM_ARG
        Ok(crate::replication::response::ReplicationResult(Ok(Some(
            log_id(2, 1, 2),
        )))),
        Some(InflightId::new(41)),
    );
    assert_eq!(
        Some(&log_id(2, 1, 3)),
        rh.leader.progress.get(&3).matching()
    );
}
