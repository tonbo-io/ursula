//! The outstanding request ([`Link`]): what to do with an answer that decides nothing about the
//! request itself (design §4.6, §4.7). It resends the identical request after backoff, doubling
//! from 25 ms to 5 s with full jitter (the driver's), never before `Retry-After` (capped at
//! 60 s); retries a blocked request every 30 s; and parks the request while a HEAD checks the
//! stream's token, cluster and limit. A HEAD naming a history that inherits the generation hands
//! the re-key to the replicator ([`HeadOutcome::ReKey`]).

use std::time::Duration;

use ursula_headers::Incarnation;

use super::Action;
use super::BlockedReason;
use super::Condition;
use super::Ctx;
use super::RequestId;
use super::ResetKind;
use super::answer::Common;
use super::answer::HeadDecision;
use super::answer::HeadReason;
use crate::transport::Attempt;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::Request;
use crate::wire::Target;

/// The first backoff.
pub(crate) const BACKOFF_START_MS: u64 = 25;
/// The longest backoff.
pub(crate) const BACKOFF_CAP_MS: u64 = 5_000;
/// The longest `Retry-After` obeyed.
pub(crate) const RETRY_AFTER_CAP_MS: u64 = 60_000;
/// How often a blocked request is sent again.
pub(crate) const BLOCKED_RETRY_MS: u64 = 30_000;
/// Redirects resent before the request is blocked `redirected`.
pub(crate) const REDIRECTS: u8 = 3;

/// The full-jitter span after `failures` consecutive failures.
pub(crate) fn backoff_ms(failures: u32) -> u64 {
    let doublings = failures.saturating_sub(1).min(16);
    BACKOFF_START_MS
        .saturating_mul(1u64 << doublings)
        .min(BACKOFF_CAP_MS)
}

/// Which request an answer belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hit {
    /// The outstanding request's current attempt.
    Main,
    /// The HEAD the request waits on.
    Head,
    /// Neither: a late or duplicate answer, ignored.
    None,
}

/// What the answer to the HEAD a request waits on leads to (design §4.7, §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeadOutcome {
    /// The request waits: retried after backoff, blocked, or the HEAD itself is retried.
    Waiting,
    /// The stream has another token whose history inherits the generation: the replicator
    /// re-keys to it, and this request goes.
    ReKey {
        to: Incarnation,
        from: Incarnation,
    },
    Reset(ResetKind),
}

impl From<Result<(), ResetKind>> for HeadOutcome {
    fn from(result: Result<(), ResetKind>) -> Self {
        match result {
            Ok(()) => Self::Waiting,
            Err(kind) => Self::Reset(kind),
        }
    }
}

/// The retry rules of one request, without the replicator's state: backoff doubling from 25 ms to
/// 5 s with the driver's full jitter, never before `Retry-After` (capped at 60 s), a blocked
/// condition every 30 s, one token reread after a `401`, a few resends of a redirect. [`Link`]
/// carries them out for the replicator, and the driver's open uses them for its own requests:
/// one policy.
#[derive(Debug, Clone, Default)]
pub(crate) struct Policy {
    failures: u32,
    token_refused: bool,
    redirects: u8,
}

/// What [`Policy`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Next {
    /// Send again at a time in `at_ms ..= at_ms + jitter_ms`, under `condition` meanwhile.
    Wait {
        at_ms: u64,
        jitter_ms: u64,
        attempt: Attempt,
        condition: Condition,
    },
    /// Send again at once (the token reread after a `401`).
    Resend(Attempt),
    /// HEAD before deciding.
    Head(HeadReason),
    Reset(ResetKind),
}

impl Policy {
    /// Backoff after one more failure.
    pub(crate) fn backoff(
        &mut self,
        retry_after: Option<Duration>,
        attempt: Attempt,
        now_ms: u64,
    ) -> Next {
        self.failures = self.failures.saturating_add(1);
        let floor = retry_after.map_or(0, |after| {
            u64::try_from(after.as_millis())
                .unwrap_or(u64::MAX)
                .min(RETRY_AFTER_CAP_MS)
        });
        Next::Wait {
            at_ms: now_ms.saturating_add(floor),
            jitter_ms: backoff_ms(self.failures),
            attempt,
            condition: Condition::Retrying,
        }
    }

    /// Blocked by `reason`: sent again every 30 s.
    pub(crate) fn block(reason: BlockedReason, now_ms: u64) -> Next {
        Next::Wait {
            at_ms: now_ms.saturating_add(BLOCKED_RETRY_MS),
            jitter_ms: 0,
            attempt: Attempt::default(),
            condition: Condition::Blocked(reason),
        }
    }

    /// What an answer without a producer outcome leads to. A typed `413` blocks
    /// `request_too_large`: lowering the limit is the caller's.
    pub(crate) fn decide(
        &mut self,
        common: Common,
        retry_after: Option<Duration>,
        now_ms: u64,
    ) -> Next {
        if !matches!(common, Common::Reauthorize) {
            self.token_refused = false;
        }
        match common {
            Common::Reset(kind) => Next::Reset(kind),
            Common::Resend { fresh, .. } => {
                let attempt = Attempt {
                    fresh_connection: fresh,
                    refresh_token: false,
                };
                self.backoff(retry_after, attempt, now_ms)
            }
            Common::Head(reason) => Next::Head(reason),
            Common::Reauthorize if self.token_refused => {
                Self::block(BlockedReason::Unauthorized, now_ms)
            }
            Common::Reauthorize => {
                self.token_refused = true;
                Next::Resend(Attempt {
                    fresh_connection: false,
                    refresh_token: true,
                })
            }
            Common::Redirect { location } => {
                self.redirects = self.redirects.saturating_add(1);
                if self.redirects > REDIRECTS {
                    Self::block(BlockedReason::Redirected { location }, now_ms)
                } else {
                    self.backoff(None, Attempt::default(), now_ms)
                }
            }
            Common::LowerLimit { .. } => Self::block(BlockedReason::RequestTooLarge, now_ms),
            Common::Blocked(reason) => Self::block(reason, now_ms),
        }
    }
}

#[derive(Debug, Clone)]
enum LinkState {
    InFlight(RequestId),
    Waiting { at_ms: u64, attempt: Attempt },
    Checking { head: Box<Link>, reason: HeadReason },
}

/// One request, from its first attempt to a definite answer.
#[derive(Debug, Clone)]
pub(crate) struct Link {
    request: Request,
    state: LinkState,
    policy: Policy,
}

impl Link {
    /// Sends the first attempt of `request`.
    pub(crate) fn send(request: Request, ctx: &mut Ctx, actions: &mut Vec<Action>) -> Self {
        let id = ctx.next_id();
        actions.push(Action::Send {
            id,
            request: request.clone(),
            attempt: Attempt::default(),
        });
        Self {
            request,
            state: LinkState::InFlight(id),
            policy: Policy::default(),
        }
    }

    pub(crate) fn hit(&self, id: RequestId) -> Hit {
        match &self.state {
            LinkState::InFlight(current) if *current == id => Hit::Main,
            LinkState::Checking { head, .. } if head.hit(id) == Hit::Main => Hit::Head,
            LinkState::InFlight(_) | LinkState::Waiting { .. } | LinkState::Checking { .. } => {
                Hit::None
            }
        }
    }

    /// Whether an attempt of the request itself is out (not waiting, not parked on a HEAD).
    pub(crate) fn in_flight(&self) -> bool {
        matches!(self.state, LinkState::InFlight(_))
    }

    /// Whether a resend is due at `now_ms`.
    #[cfg(test)]
    pub(crate) fn due(&self, now_ms: u64) -> bool {
        matches!(self.state, LinkState::Waiting { at_ms, .. } if now_ms >= at_ms)
    }

    /// Replaces the request the next attempt sends (the renumbering negative control).
    #[cfg(test)]
    pub(crate) fn replace(&mut self, request: Request) {
        self.request = request;
    }

    fn resend(&mut self, attempt: Attempt, ctx: &mut Ctx, actions: &mut Vec<Action>) {
        let id = ctx.next_id();
        actions.push(Action::Send {
            id,
            request: self.request.clone(),
            attempt,
        });
        self.state = LinkState::InFlight(id);
    }

    /// Carries out what the policy decided. `Err` is a reset.
    fn carry(
        &mut self,
        next: Next,
        ctx: &mut Ctx,
        actions: &mut Vec<Action>,
    ) -> Result<(), ResetKind> {
        match next {
            Next::Wait {
                at_ms,
                jitter_ms,
                attempt,
                condition,
            } => {
                ctx.set_condition(Some(condition), actions);
                actions.push(Action::Timer { at_ms, jitter_ms });
                self.state = LinkState::Waiting { at_ms, attempt };
            }
            Next::Resend(attempt) => self.resend(attempt, ctx, actions),
            Next::Head(reason) => {
                let head = Self::send(Request::new(Method::Head, Target::Stream), ctx, actions);
                self.state = LinkState::Checking {
                    head: Box::new(head),
                    reason,
                };
            }
            Next::Reset(kind) => return Err(kind),
        }
        Ok(())
    }

    /// Carries out what an answer without a producer outcome decided. `Err` is a reset.
    pub(crate) fn common(
        &mut self,
        common: Common,
        retry_after: Option<Duration>,
        now_ms: u64,
        ctx: &mut Ctx,
        actions: &mut Vec<Action>,
    ) -> Result<(), ResetKind> {
        if let Common::LowerLimit { bytes } = common {
            ctx.limits.named = Some(ctx.limits.named.map_or(bytes, |named| named.min(bytes)));
        }
        let next = self.policy.decide(common, retry_after, now_ms);
        self.carry(next, ctx, actions)
    }

    /// The answer to the HEAD this request waits on.
    pub(crate) fn head_answer(
        &mut self,
        exchange: &Exchange,
        now_ms: u64,
        ctx: &mut Ctx,
        actions: &mut Vec<Action>,
    ) -> HeadOutcome {
        let LinkState::Checking { head, reason } = &mut self.state else {
            return HeadOutcome::Waiting;
        };
        let reason = *reason;
        match ctx.head_decision(exchange) {
            HeadDecision::Rewound => HeadOutcome::Reset(ResetKind::Rewound),
            HeadDecision::Deleted => HeadOutcome::Reset(ResetKind::Deleted),
            HeadDecision::WrongCluster => self
                .carry(
                    Policy::block(BlockedReason::WrongCluster, now_ms),
                    ctx,
                    actions,
                )
                .into(),
            HeadDecision::ReKey {
                incarnation,
                from,
                max_request_bytes,
            } => {
                ctx.limits.head = max_request_bytes;
                ctx.limits.named = None;
                HeadOutcome::ReKey {
                    to: incarnation,
                    from,
                }
            }
            HeadDecision::Same { max_request_bytes } => {
                ctx.limits.head = max_request_bytes;
                ctx.limits.named = None;
                let next = match reason {
                    // Our token unchanged: the request is retried.
                    HeadReason::Precondition => {
                        self.policy.backoff(None, Attempt::default(), now_ms)
                    }
                    // No re-cut in M1, whatever the advertised limit.
                    HeadReason::TooLarge => Policy::block(BlockedReason::RequestTooLarge, now_ms),
                    // Our token unchanged: retention passed our tail.
                    HeadReason::Gone(kind) => return HeadOutcome::Reset(kind),
                };
                self.carry(next, ctx, actions).into()
            }
            HeadDecision::Common(common) => {
                // The HEAD's own failure: a HEAD never waits on another HEAD.
                let common = match common {
                    Common::Head(_) => Common::Blocked(BlockedReason::Rejected {
                        status: match exchange {
                            Exchange::Answered(answer) => answer.status,
                            Exchange::NoAnswer(_) => 0,
                        },
                    }),
                    other => other,
                };
                let retry_after = match exchange {
                    Exchange::Answered(answer) => answer.retry_after,
                    Exchange::NoAnswer(_) => None,
                };
                head.common(common, retry_after, now_ms, ctx, actions)
                    .into()
            }
        }
    }

    /// A tick: a due resend goes out, a timer not yet due is asked for again.
    pub(crate) fn tick(&mut self, now_ms: u64, ctx: &mut Ctx, actions: &mut Vec<Action>) {
        match &mut self.state {
            LinkState::InFlight(_) => {}
            LinkState::Waiting { at_ms, attempt } => {
                if now_ms >= *at_ms {
                    let attempt = *attempt;
                    self.resend(attempt, ctx, actions);
                } else {
                    actions.push(Action::Timer {
                        at_ms: *at_ms,
                        jitter_ms: 0,
                    });
                }
            }
            LinkState::Checking { head, .. } => head.tick(now_ms, ctx, actions),
        }
    }
}
