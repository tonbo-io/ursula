//! How the meta plane's failures answer on the placement routes.
//!
//! Every [`PlacementApiError`] is built here, once per source, so each
//! route renders the same refusal the same way: the reason, whether the
//! write may have applied, and the rendered source chain as `message`.

use ursula_control::PlacementApiError;
use ursula_control::PlacementApiReason;
use ursula_proto::placement::WriteEffect;
use ursula_runtime::ErrorChain;

use super::confirm::MetaBarrierError;

/// A meta read barrier that confirmed no view applied nothing.
impl From<MetaBarrierError> for PlacementApiError {
    fn from(error: MetaBarrierError) -> Self {
        let reason = match &error {
            MetaBarrierError::NoLeader => PlacementApiReason::NoLeader,
            MetaBarrierError::Stopped => PlacementApiReason::Stopped,
            MetaBarrierError::Barrier { leader, .. } => {
                PlacementApiReason::ForwardFailed { leader: *leader }
            }
            MetaBarrierError::TimedOut { .. } | MetaBarrierError::Unconfirmed { .. } => {
                PlacementApiReason::Unconfirmed
            }
        };
        Self {
            reason,
            write_effect: WriteEffect::NotApplied,
            current: None,
            message: ErrorChain(&error).to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ursula_control::PlacementApiError;
    use ursula_control::PlacementApiReason;
    use ursula_control::UnconfirmedView;
    use ursula_control::samples;
    use ursula_proto::placement::WriteEffect;

    use super::MetaBarrierError;
    use crate::meta::confirm::BarrierFailure;

    /// Each barrier failure answers with its reason, applied nothing, and
    /// the whole source chain as its message.
    #[test]
    fn every_barrier_failure_answers_with_its_reason() {
        let cases = [
            (MetaBarrierError::NoLeader, PlacementApiReason::NoLeader),
            (MetaBarrierError::Stopped, PlacementApiReason::Stopped),
            (
                MetaBarrierError::Barrier {
                    leader: samples::node(3),
                    failure: BarrierFailure::Unreachable,
                },
                PlacementApiReason::ForwardFailed {
                    leader: samples::node(3),
                },
            ),
            (
                MetaBarrierError::TimedOut {
                    waited: Duration::from_secs(10),
                },
                PlacementApiReason::Unconfirmed,
            ),
            (
                MetaBarrierError::Unconfirmed {
                    source: UnconfirmedView::OtherHistory {
                        view: samples::meta(1, 1),
                        stamp: samples::meta(2, 4),
                    },
                },
                PlacementApiReason::Unconfirmed,
            ),
        ];
        for (error, reason) in cases {
            let rendered = PlacementApiError::from(error);
            assert_eq!(rendered.reason, reason);
            assert_eq!(rendered.write_effect, WriteEffect::NotApplied);
            assert_eq!(rendered.current, None);
        }
        let chained = PlacementApiError::from(MetaBarrierError::Unconfirmed {
            source: UnconfirmedView::Empty,
        });
        assert_eq!(
            chained.message,
            "the local placement view does not carry the meta barrier: the view applied no meta \
             history event"
        );
    }
}
