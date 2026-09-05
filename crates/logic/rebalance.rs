// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Ownership balancing after the fleet membership changes.
//!
//! The densest node gives a bounded batch of idle cells to the peers below
//! their weighted share. This module only counts. Every move still goes
//! through the release CAS and the signed successor acquire, so a wrong count
//! here costs movement, never authority.

use crate::{CapacityPeer, NodeId};
use std::cmp::Ordering;

/// A receiver fills to this fraction below its target. A donor drains to its
/// target exactly, so the fleet cannot end short by one deadband per donor.
/// The receiver's margin absorbs the lease lag between a batch and its
/// publication: a stale count can overshoot by one batch, and the node it
/// came from has no room to take that batch straight back.
const DEADBAND_PERCENT: u128 = 2;

struct Member<'a> {
    peer: &'a CapacityPeer,
    owned: u128,
    weight: u128,
}

impl Member<'_> {
    /// Whether this member can take a donated cell from `self_node`. A
    /// draining node gives its own cells away, and a node with cold
    /// activations queued behind its ceiling would time the adoption out
    /// behind them: an adoption is a record read and a CAS through that
    /// same queue, and on the 2026-09-04 fleet a node that had just
    /// returned, waking nine hundred rooms, let a batch of 21 time out. The
    /// plan and the executor share this rule, so a donor never releases a
    /// cell it has nowhere to put.
    fn receives_from(&self, self_node: &str) -> bool {
        self.peer.node != self_node && !self.peer.draining && self.peer.restoring == 0
    }

    /// Owned cells per unit of weight, compared without division. The node
    /// id breaks ties, so one sample elects exactly one donor.
    fn denser(&self, other: &Self) -> Ordering {
        (self.owned * other.weight)
            .cmp(&(other.owned * self.weight))
            .then_with(|| other.peer.node.cmp(&self.peer.node))
    }
}

/// One consistent view of the live fleet.
struct Fleet<'a> {
    members: Vec<Member<'a>>,
    owned: u128,
    weight: u128,
}

impl<'a> Fleet<'a> {
    /// `None` when a live lease is stale, predates ownership publication or
    /// paced handoff, or carries an operator's pause: a mixed or paused
    /// fleet moves nothing. `None` as well when a lease was sampled at or
    /// before `since_ms`. The caller sets that to
    /// the instant its last batch could have reached every lease, so a plan
    /// never mixes counts from before a batch with counts from after it. A
    /// mixed view shrinks the total, lowers every target, and moves cells
    /// that then come straight back.
    fn observe(
        peers: &'a [CapacityPeer],
        self_node: &str,
        now_ms: u64,
        max_age_ms: u64,
        since_ms: u64,
    ) -> Option<Self> {
        let mut members = Vec::new();
        for peer in peers.iter().filter(|peer| peer.expires_ms > now_ms) {
            let fresh =
                peer.sampled_ms > since_ms && now_ms.saturating_sub(peer.sampled_ms) <= max_age_ms;
            if !fresh || !peer.paced_handoff || peer.rebalance_paused {
                return None;
            }
            members.push(Member {
                peer,
                owned: peer.owned_cells? as u128,
                weight: u128::from(peer.placement_weight?.max(1)),
            });
        }
        if !members.iter().any(|member| member.peer.node == self_node) {
            return None;
        }
        Some(Fleet {
            owned: members.iter().map(|member| member.owned).sum(),
            weight: members.iter().map(|member| member.weight).sum(),
            members,
        })
    }

    fn target(&self, member: &Member<'_>) -> u128 {
        (self.owned * member.weight).div_ceil(self.weight)
    }

    /// The cells a member can still take before it reaches the deadband.
    fn room(&self, member: &Member<'_>) -> u128 {
        let target = self.target(member);
        let deadband = (target * DEADBAND_PERCENT).div_ceil(100);
        target.saturating_sub(deadband).saturating_sub(member.owned)
    }
}

/// How many idle cells this node gives away now.
///
/// `Some` only on the densest live node, and only while it exceeds its
/// target and a peer has room. The count never exceeds `batch` or the
/// receivers' room, so one batch cannot push a peer above its own target.
pub fn surplus(
    peers: &[CapacityPeer],
    self_node: &str,
    now_ms: u64,
    max_age_ms: u64,
    since_ms: u64,
    batch: usize,
) -> Option<usize> {
    let fleet = Fleet::observe(peers, self_node, now_ms, max_age_ms, since_ms)?;
    let donor = fleet
        .members
        .iter()
        .max_by(|left, right| left.denser(right))?;
    if donor.peer.node != self_node {
        return None;
    }
    let surplus = donor.owned.saturating_sub(fleet.target(donor));
    let room: u128 = fleet
        .members
        .iter()
        .filter(|member| member.receives_from(self_node))
        .map(|member| fleet.room(member))
        .sum();
    let count = surplus.min(room).min(batch as u128);
    (count > 0).then_some(count as usize)
}

/// The peers that can take a donated cell, least dense first. Every entry
/// is below its target, so the executor never hands a cell to a node that
/// would give it straight back. The planner bounds the count by the room
/// under the deadband; the executor is more permissive by that deadband, so
/// a receiver that fills between the plan and the release still accepts the
/// planned cells instead of leaving them unowned.
pub fn receivers(
    peers: &[CapacityPeer],
    self_node: &str,
    now_ms: u64,
    max_age_ms: u64,
) -> Vec<NodeId> {
    let Some(fleet) = Fleet::observe(peers, self_node, now_ms, max_age_ms, 0) else {
        return Vec::new();
    };
    let mut receivers: Vec<&Member<'_>> = fleet
        .members
        .iter()
        .filter(|member| member.receives_from(self_node) && member.owned < fleet.target(member))
        .collect();
    receivers.sort_by(|left, right| left.denser(right));
    receivers
        .into_iter()
        .map(|member| member.peer.node.clone())
        .collect()
}
