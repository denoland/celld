// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Lifetime of work created by one event and run during another event.
//!
//! A claim retains its origin context. Wait preparation subscribes before it
//! rechecks retirement, and the prepared value owns the wait priority. Keeping
//! those steps here prevents a caller from missing the final claim release.

use super::{InFlight, IoContext};
use crate::asyncrt;
use std::future::Future;
use std::sync::{Arc, Mutex};

/// Keeps the event that created a cross-entry block active while another
/// event runs the block. Dropping the claim is the single release path, so a
/// failed acquisition and a completed block cannot forget the lifecycle
/// update.
pub(super) struct CrossEntryGateClaim {
    origin: Arc<IoContext>,
}

impl Drop for CrossEntryGateClaim {
    fn drop(&mut self) {
        self.origin.release_cross_entry_gate_claim();
    }
}

/// Whether this event can still originate work that another event runs.
/// Retirement is terminal: a stale promise reaction cannot add a new claim
/// after the event has closed its resources.
enum CrossEntryGateLifecycle {
    Accepting {
        active: usize,
    },
    /// A lifecycle cancellation retired the event while claims still existed.
    /// The claims retain only the allocation until their owners drop them;
    /// they cannot keep the event or its resources active after shutdown.
    Retired {
        outstanding: usize,
    },
}

struct ClaimsState {
    lifecycle: CrossEntryGateLifecycle,
    activity: tokio::sync::watch::Sender<usize>,
}

impl Default for ClaimsState {
    fn default() -> Self {
        let (activity, _) = tokio::sync::watch::channel(0);
        Self {
            lifecycle: CrossEntryGateLifecycle::Accepting { active: 0 },
            activity,
        }
    }
}

#[derive(Default)]
pub(super) struct CrossEntryGateClaims {
    state: Mutex<ClaimsState>,
}

impl IoContext {
    /// Claim this event before a block can outlive its own turn. The lock
    /// makes the claim atomic with terminal retirement: either the event stays
    /// active, or the stale block is refused before it can acquire a gate.
    pub(super) fn claim_cross_entry_gate(self: &Arc<Self>) -> Option<CrossEntryGateClaim> {
        let mut claims = self.cross_entry_gates.state.lock().unwrap();
        let active = match &mut claims.lifecycle {
            CrossEntryGateLifecycle::Accepting { active } => {
                *active = active
                    .checked_add(1)
                    .expect("an event exhausted its cross-entry gate claims");
                *active
            }
            CrossEntryGateLifecycle::Retired { .. } => return None,
        };
        claims.activity.send_replace(active);
        drop(claims);
        Some(CrossEntryGateClaim {
            origin: Arc::clone(self),
        })
    }

    fn release_cross_entry_gate_claim(&self) {
        let mut claims = self.cross_entry_gates.state.lock().unwrap();
        let activity = match &mut claims.lifecycle {
            CrossEntryGateLifecycle::Accepting { active } => {
                *active = active
                    .checked_sub(1)
                    .expect("a cross-entry gate claim was released twice");
                *active
            }
            CrossEntryGateLifecycle::Retired { outstanding } => {
                *outstanding = outstanding
                    .checked_sub(1)
                    .expect("a retired cross-entry gate claim was released twice");
                0
            }
        };
        claims.activity.send_replace(activity);
    }

    pub(super) fn has_cross_entry_gate_claim(&self) -> bool {
        let claims = self.cross_entry_gates.state.lock().unwrap();
        matches!(
            claims.lifecycle,
            CrossEntryGateLifecycle::Accepting { active } if active > 0
        )
    }

    /// Read the claim count and subscribe while holding the same lock.
    /// A release cannot fall between the observation and the subscription.
    fn subscribe_cross_entry_gate_changes(&self) -> WaitPlan {
        let claims = self.cross_entry_gates.state.lock().unwrap();
        match claims.lifecycle {
            CrossEntryGateLifecycle::Accepting { active: 0 } => {
                WaitPlan::NoActiveClaims(claims.activity.subscribe())
            }
            CrossEntryGateLifecycle::Accepting { .. } => {
                WaitPlan::ActiveClaims(claims.activity.subscribe())
            }
            CrossEntryGateLifecycle::Retired { .. } => WaitPlan::Ordinary,
        }
    }

    fn prepare_cross_entry_gate_wait(
        &self,
        answered: bool,
        retirement_recheck: impl FnOnce() -> bool,
    ) -> PreparedWait {
        // Subscribe before checking retirement again. Moving the subscription
        // below the callback can miss a claim that starts and ends during the
        // check, leaving the event waiting without another source of progress.
        // A force-retired context has no subscription, but still needs the
        // recheck so a request without an ID cannot report false idle.
        let plan = self.subscribe_cross_entry_gate_changes();
        let plan = if retirement_recheck() {
            WaitPlan::RecheckRetirement
        } else {
            plan
        };
        PreparedWait { plan, answered }
    }

    pub(super) fn retire_without_cross_entry_gate(&self) -> bool {
        let mut claims = self.cross_entry_gates.state.lock().unwrap();
        match claims.lifecycle {
            CrossEntryGateLifecycle::Accepting { active: 0 } => {
                claims.lifecycle = CrossEntryGateLifecycle::Retired { outstanding: 0 };
                true
            }
            CrossEntryGateLifecycle::Accepting { .. } => false,
            CrossEntryGateLifecycle::Retired { .. } => true,
        }
    }

    /// Stop every cross-entry claim from extending this event's lifetime.
    /// A lifecycle cancellation deliberately closes the event even if a
    /// queued or running callback has not released its claim yet.
    pub(super) fn force_retire_cross_entry_gates(&self) {
        let mut claims = self.cross_entry_gates.state.lock().unwrap();
        let outstanding = match claims.lifecycle {
            CrossEntryGateLifecycle::Accepting { active } => active,
            CrossEntryGateLifecycle::Retired { .. } => return,
        };
        claims.lifecycle = CrossEntryGateLifecycle::Retired { outstanding };
        claims.activity.send_replace(0);
    }
}

impl InFlight {
    /// Prepare the next wait without exposing the subscription and recheck
    /// as separately callable operations.
    pub(crate) fn prepare_cross_entry_gate_wait(&self) -> PreparedWait {
        self.context
            .prepare_cross_entry_gate_wait(self.answered(), || self.finished())
    }
}

enum WaitPlan {
    Ordinary,
    RecheckRetirement,
    NoActiveClaims(tokio::sync::watch::Receiver<usize>),
    ActiveClaims(tokio::sync::watch::Receiver<usize>),
}

/// The subscription, retirement decision, and answer status travel together.
/// The runtime cannot construct the plan, replace its answer status, or
/// change its wait priority.
#[must_use]
pub(crate) struct PreparedWait {
    plan: WaitPlan,
    answered: bool,
}

pub(crate) enum WaitOutcome<T> {
    StateChanged,
    Ordinary(T),
}

impl PreparedWait {
    /// Combine the prepared claim wait with the runtime's ordinary wake.
    /// The runtime identifies its idle result; this module owns the choice
    /// between that result and a claim change.
    pub(crate) async fn wait<T>(
        self,
        ordinary_wake: impl Future<Output = T>,
        is_idle: impl FnOnce(&T) -> bool,
    ) -> WaitOutcome<T> {
        let mut activity = match self.plan {
            WaitPlan::Ordinary => return WaitOutcome::Ordinary(ordinary_wake.await),
            WaitPlan::RecheckRetirement => return WaitOutcome::StateChanged,
            WaitPlan::NoActiveClaims(mut activity) => {
                return asyncrt::select_biased! {
                    "a cross-entry claim change wins a tie with an ordinary wake";
                    changed = activity.changed() => {
                        let _ = changed;
                        WaitOutcome::StateChanged
                    },
                    wake = ordinary_wake => WaitOutcome::Ordinary(wake),
                };
            }
            WaitPlan::ActiveClaims(activity) => activity,
        };
        // Neither arm needs priority. An ordinary wake leaves retirement
        // visible on the next pass, and a release leaves the ordinary wake
        // ready in the operation set or the reply channel.
        let outcome = asyncrt::select! {
            wake = ordinary_wake => WaitOutcome::Ordinary(wake),
            released = activity.wait_for(|active| *active == 0) => {
                let _ = released;
                WaitOutcome::StateChanged
            },
        };
        let idle = match &outcome {
            WaitOutcome::Ordinary(wake) => is_idle(wake),
            WaitOutcome::StateChanged => false,
        };
        if idle && self.answered {
            // An answered event can have no operation of its own while a
            // block it created runs in another event. The claim release is
            // still a source of progress, so an empty local operation set
            // cannot end the event.
            let _ = activity.wait_for(|active| *active == 0).await;
            WaitOutcome::StateChanged
        } else {
            outcome
        }
    }
}

#[cfg(all(test, celld_internal_tests))]
mod input_gate_lifecycle_tests {
    include!(env!("CELLD_INTERNAL_INPUT_GATE_TESTS"));
}
