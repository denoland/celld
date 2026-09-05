// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The fleet drain token: one bucket object that serializes donors.
//!
//! `celld_logic::drain` owns the claim and settled decisions; this module
//! adds the bucket IO, the bounded acquisition wait, the renewal, and the
//! structured events. The token is advisory. A donor that cannot claim it
//! within its wait bound proceeds anyway, because the orchestrator grace is
//! finite and an unserialized handoff is strictly better than a forced
//! exit. A fresh node's first readiness reads the same object through
//! [`read`]. It treats a live foreign claim as an unsettled fleet and compares
//! recovery with the pre-drain restoration baseline retained after release.

use crate::bucket::Bucket;
use celld_logic::drain::DrainToken;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The well-known token key. It lives outside `nodes/` so lease listings
/// and dead-node GC never see it.
pub const TOKEN_KEY: &str = "drain/token.json";

/// How long one claim lives without a renewal. The drain loop renews well
/// before expiry, so only a dead holder lets the token lapse.
pub const TOKEN_TTL_MS: u64 = 120_000;

/// Delay between claim attempts while another donor holds the token.
const RETRY_DELAY: Duration = Duration::from_millis(1_000);

#[derive(Serialize, Deserialize)]
struct Wire {
    #[serde(default)]
    node: String,
    #[serde(default)]
    expires_ms: u64,
    #[serde(default)]
    restoration_baseline: Vec<RestorationBaselineWire>,
}

#[derive(Clone, Serialize, Deserialize)]
struct RestorationBaselineWire {
    node: String,
    restoring: u64,
}

/// The token object as read from the bucket. `token: None` with an etag is
/// an unparseable body: claimable, and settled for the ready gate.
pub struct Current {
    pub token: Option<DrainToken>,
    pub etag: String,
}

/// A held claim. The etag guards renewals and the release, so a claim that
/// another donor legitimately took over cannot be clobbered.
pub struct Hold {
    etag: String,
    expires_ms: u64,
    restoration_baseline: Vec<RestorationBaselineWire>,
}

/// The acquisition result carried into the drain loop.
pub enum Outcome {
    /// The claim succeeded; the drain owns the token until release.
    Acquired(Hold),
    /// The wait bound expired first; the drain proceeds unserialized.
    Bypassed,
    /// No fleet bucket, or the wait bound is configured to zero.
    Disabled,
}

/// Read the current token. `Ok(None)` is an absent object; an error is an
/// unreadable store, which the claim path treats as absent and the ready
/// gate treats as unsettled.
pub async fn read(bucket: &Bucket) -> anyhow::Result<Option<Current>> {
    let Some((bytes, etag)) = bucket.get(TOKEN_KEY).await? else {
        return Ok(None);
    };
    let token = serde_json::from_slice::<Wire>(&bytes)
        .ok()
        .map(|wire| DrainToken {
            node: wire.node,
            expires_ms: wire.expires_ms,
            restoration_baseline: wire
                .restoration_baseline
                .into_iter()
                .map(|baseline| celld_logic::drain::RestorationBaseline {
                    node: baseline.node,
                    restoring: baseline.restoring,
                })
                .collect(),
        });
    Ok(Some(Current { token, etag }))
}

/// One claim attempt. `None` means another donor holds a live claim or the
/// conditional write lost a race; the caller retries within its bound. An
/// ambiguous write also returns `None`: if it committed, the next attempt
/// reads this node's own record back and claims over it.
async fn try_claim(
    bucket: &Bucket,
    node: &str,
    now_ms: u64,
    restoration_baseline: &[RestorationBaselineWire],
) -> Option<Hold> {
    let current = read(bucket).await.ok().flatten();
    if !celld_logic::drain::may_claim(
        current.as_ref().and_then(|current| current.token.as_ref()),
        node,
        now_ms,
    ) {
        return None;
    }
    let expires_ms = now_ms + TOKEN_TTL_MS;
    let body = serde_json::to_vec(&Wire {
        node: node.to_string(),
        expires_ms,
        restoration_baseline: restoration_baseline.to_vec(),
    })
    .expect("encode drain token");
    let guard = current.as_ref().map(|current| current.etag.as_str());
    match bucket.put_cas(TOKEN_KEY, body, guard).await {
        Ok(Some(etag)) => Some(Hold {
            etag,
            expires_ms,
            restoration_baseline: restoration_baseline.to_vec(),
        }),
        Ok(None) | Err(_) => None,
    }
}

/// Claim the token, retrying until `wait` elapses. The wait is this
/// function's own budget: the caller's drain deadline never covers it.
pub async fn acquire(
    bucket: &Bucket,
    node: &str,
    wait: Duration,
    restoration_baseline: Vec<celld_logic::drain::RestorationBaseline>,
) -> Outcome {
    let restoration_baseline = restoration_baseline
        .into_iter()
        .map(|baseline| RestorationBaselineWire {
            node: baseline.node,
            restoring: baseline.restoring,
        })
        .collect::<Vec<_>>();
    let started_mono_ms = crate::asyncrt::mono_ms();
    let wait_ms = wait.as_millis() as u64;
    loop {
        let now_ms = crate::ownership_store::now_ms();
        if let Some(hold) = try_claim(bucket, node, now_ms, &restoration_baseline).await {
            tracing::info!(
                event = "drain_token_acquired",
                waited_ms = crate::asyncrt::mono_ms().saturating_sub(started_mono_ms),
                expires_ms = hold.expires_ms,
                "acquired the fleet drain token"
            );
            return Outcome::Acquired(hold);
        }
        let waited_ms = crate::asyncrt::mono_ms().saturating_sub(started_mono_ms);
        if waited_ms >= wait_ms {
            let holder = read(bucket)
                .await
                .ok()
                .flatten()
                .and_then(|current| current.token)
                .map(|token| token.node)
                .unwrap_or_default();
            tracing::warn!(
                event = "drain_token_bypassed",
                holder,
                waited_ms,
                "the drain token wait expired; handing off unserialized"
            );
            return Outcome::Bypassed;
        }
        let remaining = Duration::from_millis(wait_ms.saturating_sub(waited_ms));
        crate::asyncrt::sleep(RETRY_DELAY.min(remaining)).await;
    }
}

/// Whether a held claim is due for renewal.
pub fn renew_due(hold: &Hold, now_ms: u64) -> bool {
    now_ms + TOKEN_TTL_MS / 2 >= hold.expires_ms
}

/// Extend a held claim. `false` means another donor took the token over, so
/// the caller continues its drain without it.
pub async fn renew(bucket: &Bucket, node: &str, hold: &mut Hold) -> bool {
    let now_ms = crate::ownership_store::now_ms();
    let expires_ms = now_ms + TOKEN_TTL_MS;
    let body = serde_json::to_vec(&Wire {
        node: node.to_string(),
        expires_ms,
        restoration_baseline: hold.restoration_baseline.clone(),
    })
    .expect("encode drain token");
    match bucket.put_cas(TOKEN_KEY, body, Some(&hold.etag)).await {
        Ok(Some(etag)) => {
            hold.etag = etag;
            hold.expires_ms = expires_ms;
            true
        }
        Ok(None) => {
            tracing::warn!(
                event = "drain_token_renew_lost",
                "another donor took the drain token over; continuing unserialized"
            );
            false
        }
        // Ambiguous: the write may have committed. Read the object back; a
        // record naming this node adopts the new etag, anything else is a
        // lost claim.
        Err(_) => match read(bucket).await.ok().flatten() {
            Some(current)
                if current
                    .token
                    .as_ref()
                    .is_some_and(|token| token.node == node) =>
            {
                hold.expires_ms = current
                    .token
                    .as_ref()
                    .map(|token| token.expires_ms)
                    .unwrap_or(expires_ms);
                hold.etag = current.etag;
                true
            }
            _ => {
                tracing::warn!(
                    event = "drain_token_renew_lost",
                    "the drain token renewal was lost; continuing unserialized"
                );
                false
            }
        },
    }
}

/// Release a held claim by writing an expired record under the etag guard.
/// An unconditional delete could erase a successor's live claim; a rejected
/// release means exactly that and needs nothing further. An ambiguous
/// release is bounded by the TTL.
pub async fn release(bucket: &Bucket, node: &str, hold: Hold) {
    let body = serde_json::to_vec(&Wire {
        node: node.to_string(),
        expires_ms: 0,
        restoration_baseline: hold.restoration_baseline,
    })
    .expect("encode drain token");
    match bucket.put_cas(TOKEN_KEY, body, Some(&hold.etag)).await {
        Ok(Some(_)) => tracing::info!(
            event = "drain_token_released",
            "released the fleet drain token"
        ),
        Ok(None) => tracing::debug!(
            event = "drain_token_release_superseded",
            "the drain token already has a new holder"
        ),
        Err(error) => tracing::warn!(
            event = "drain_token_release_ambiguous",
            %error,
            "the drain token release did not confirm; the TTL bounds the residue"
        ),
    }
}
