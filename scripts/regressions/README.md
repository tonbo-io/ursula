# OpenRaft stale replication responses

Run from the repository root with an unused scratch directory:

```sh
python3 scripts/regressions/openraft_stale_ack.py /tmp/openraft-stale-ack-proof
```

The runner downloads published OpenRaft alpha21 and alpha28 sources without
modifying Cargo's registry cache. It appends the same four handler tests to the
upstream test module and pins the matching runtime and macros. Alpha21 must fail
all four tests; alpha28 must pass all four. Logs remain in the scratch directory.
This is an upstream handler regression, not an end-to-end Ursula fault schedule.

The first case delivers a replication error, then an already queued success for
the same pipeline ID. Alpha21 clears `Inflight` on the error and panics when the
old ACK arrives. The second case delivers an old ACK while a different inflight
ID is active. Alpha21 ignores the ID inside `ack()` but still advances matching
outside it, potentially changing quorum progress. Both tests require unchanged
matching and committed positions after the obsolete ACK.

These events are reachable in the production worker: `LogsSince` pipelines emit
multiple responses with one inflight ID. A log-reversion/conflict or replication
error can clear that inflight state before a queued response is consumed.
Additionally alpha21's watch receiver used `borrow_watched()` without marking
the command seen, allowing `drain_events()` to consume the same delivered command
again. The official fix both rejects obsolete ACKs before updating progress and
uses `borrow_and_update()` in the worker. Ursula's simulation watch adapter must
implement the same seen-version semantics under the value lock.

Official fix: databendlabs/openraft commit
`cecb5de8d98f6c4acb15930c407bb6947fb16696`
(`fix: reject stale replication acks after log reversion`). Alpha28 is the first
published release containing it. The workspace pins that release exactly rather
than allowing a prerelease range to select alpha36 or a later API.

The other two cases deliver an ACK or conflict from a removed replication stream
after the same peer has a replacement stream and newer matching progress.
Alpha21 accepts both old notifications. The ACK regresses matching and trips
`update.rs:124`, while the conflict trips the log-reversion assertion at
`update.rs:91`. These are the two assertion sites observed in Ursula's lost-WAL
membership-replacement CI test. Alpha28 checks the stream generation before
handling either result, leaving the replacement stream's progress unchanged.
The test reconstructs the handler transition, not the CI scheduler interleaving.

Stream-generation fix: databendlabs/openraft commit
`369e5d1cc67773759429f277a5889194652ddf67`
(`fix: avoid blocking RaftCore on closed replication`). It carries stream IDs
through progress notifications and rejects responses from replaced streams.
Alpha28 includes this fix as well as the stale-inflight-ACK fix above.

The Ursula codec test `alpha21_persisted_envelopes_remain_byte_compatible` uses
named MessagePack bytes generated with published alpha21. It checks Vote, LogId,
joint StoredMembership (including a learner and addresses), and a membership
Entry, then requires byte-identical re-encoding. It does not assert that arbitrary
future OpenRaft releases or mixed-version cluster operation are compatible.
