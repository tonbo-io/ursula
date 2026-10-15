//! The outstanding request ([`Link`]): what to do with an answer that decides nothing about the
//! request itself (design §4.6, §4.7). It resends the identical request after backoff, doubling
//! from 25 ms to 5 s with full jitter (the driver's), never before `Retry-After` (capped at
//! 60 s); retries a blocked request every 30 s; and parks the request while a HEAD checks the
//! stream's token, cluster and limit.

use std::time::Duration;

use super::Action;
use super::BlockedReason;
use super::Condition;
use super::Ctx;
use super::RequestId;
use super::ResetKind;
use super::answer::Common;
use super::answer::HeadDecision;
use super::answer::HeadReason;
use super::answer::decide_head;
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
    failures: u32,
    token_refused: bool,
    redirects: u8,
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
            failures: 0,
            token_refused: false,
            redirects: 0,
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

    fn wait(&mut self, at_ms: u64, jitter_ms: u64, attempt: Attempt, actions: &mut Vec<Action>) {
        actions.push(Action::Timer { at_ms, jitter_ms });
        self.state = LinkState::Waiting { at_ms, attempt };
    }

    fn backoff(
        &mut self,
        retry_after: Option<Duration>,
        attempt: Attempt,
        now_ms: u64,
        actions: &mut Vec<Action>,
    ) {
        self.failures = self.failures.saturating_add(1);
        let floor = retry_after.map_or(0, |after| {
            u64::try_from(after.as_millis())
                .unwrap_or(u64::MAX)
                .min(RETRY_AFTER_CAP_MS)
        });
        self.wait(
            now_ms.saturating_add(floor),
            backoff_ms(self.failures),
            attempt,
            actions,
        );
    }

    fn block(
        &mut self,
        reason: BlockedReason,
        now_ms: u64,
        ctx: &mut Ctx,
        actions: &mut Vec<Action>,
    ) {
        ctx.set_condition(Some(Condition::Blocked(reason)), actions);
        self.wait(
            now_ms.saturating_add(BLOCKED_RETRY_MS),
            0,
            Attempt::default(),
            actions,
        );
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
        if !matches!(common, Common::Reauthorize) {
            self.token_refused = false;
        }
        match common {
            Common::Reset(kind) => return Err(kind),
            Common::Resend { fresh, .. } => {
                ctx.set_condition(Some(Condition::Retrying), actions);
                let attempt = Attempt {
                    fresh_connection: fresh,
                    refresh_token: false,
                };
                self.backoff(retry_after, attempt, now_ms, actions);
            }
            Common::Head(reason) => {
                let head = Self::send(Request::new(Method::Head, Target::Stream), ctx, actions);
                self.state = LinkState::Checking {
                    head: Box::new(head),
                    reason,
                };
            }
            Common::Reauthorize if self.token_refused => {
                self.block(BlockedReason::Unauthorized, now_ms, ctx, actions);
            }
            Common::Reauthorize => {
                self.token_refused = true;
                let attempt = Attempt {
                    fresh_connection: false,
                    refresh_token: true,
                };
                self.resend(attempt, ctx, actions);
            }
            Common::Redirect { location } => {
                self.redirects = self.redirects.saturating_add(1);
                if self.redirects > REDIRECTS {
                    self.block(BlockedReason::Redirected { location }, now_ms, ctx, actions);
                } else {
                    ctx.set_condition(Some(Condition::Retrying), actions);
                    self.backoff(None, Attempt::default(), now_ms, actions);
                }
            }
            Common::LowerLimit { bytes } => {
                ctx.limits.named = Some(ctx.limits.named.map_or(bytes, |named| named.min(bytes)));
                self.block(BlockedReason::RequestTooLarge, now_ms, ctx, actions);
            }
            Common::Blocked(reason) => self.block(reason, now_ms, ctx, actions),
        }
        Ok(())
    }

    /// The answer to the HEAD this request waits on. `Err` is a reset.
    pub(crate) fn head_answer(
        &mut self,
        exchange: &Exchange,
        now_ms: u64,
        ctx: &mut Ctx,
        actions: &mut Vec<Action>,
    ) -> Result<(), ResetKind> {
        let LinkState::Checking { head, reason } = &mut self.state else {
            return Ok(());
        };
        let reason = *reason;
        match decide_head(exchange, &ctx.cluster_id, ctx.incarnation) {
            HeadDecision::Rewound => Err(ResetKind::Rewound),
            HeadDecision::Deleted => Err(ResetKind::Deleted),
            HeadDecision::WrongCluster => {
                self.block(BlockedReason::WrongCluster, now_ms, ctx, actions);
                Ok(())
            }
            HeadDecision::Same { max_request_bytes } => {
                ctx.limits.head = max_request_bytes;
                ctx.limits.named = None;
                match reason {
                    // Our token unchanged: the request is retried.
                    HeadReason::Precondition => {
                        ctx.set_condition(Some(Condition::Retrying), actions);
                        self.backoff(None, Attempt::default(), now_ms, actions);
                    }
                    // No re-cut in M1, whatever the advertised limit.
                    HeadReason::TooLarge => {
                        self.block(BlockedReason::RequestTooLarge, now_ms, ctx, actions);
                    }
                }
                Ok(())
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
