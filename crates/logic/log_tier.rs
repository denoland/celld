// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Decision core for the in-fleet replicated log tier (design stage; not
//! yet wired into the engine).
//!
//! One write-ahead log per node, streamed to a small follower ensemble. A
//! durable write acknowledges when every member holds it — write-all,
//! ack-all — and the bucket record `log/<node>.json` is the CAS-guarded
//! root of truth for membership, epoch, and the tiered-through offset.
//! Nothing here performs I/O: the executor feeds these decisions facts and
//! performs the returned intents.
//!
//! The safety rules, each load-bearing:
//! - a follower's (ensemble, epoch) view changes only with the record,
//!   never on the leader's stream alone;
//! - a follower refuses appends at or below its persisted seal mark;
//! - reconfiguration force-tiers the open fragment through the new
//!   fragment base before the CAS, so an empty joiner is covered entirely
//!   by the bucket;
//! - recovery seals before it certifies, and certifies from a sealed
//!   member only;
//! - the record is created before the first fleet-durable ack and never
//!   deleted, so record absence proves the bucket is complete.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::Epoch;
use crate::NodeId;

pub type Offset = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogState {
    Open,
    Recovering,
    Sealed,
}

/// `log/<node>.json`. Every mutation is a conditional write; the executor
/// must treat a lost CAS as anothers' turn, never retry blindly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub epoch: Epoch,
    pub ensemble: BTreeSet<NodeId>,
    /// Offsets at or below this are durable in the bucket.
    pub tiered: Offset,
    pub state: LogState,
    /// The node recovering this log while `state` is `Recovering`, and the
    /// last instant it said so. A recovery reads every retained bundle of
    /// the dead session, which takes minutes on a loaded fleet, and every
    /// other node used to take the claim over after a fixed wait and repeat
    /// the same gather: on 2026-09-03 three peers and the restarting node
    /// fetched the same 209 bundles at once. A claimant that keeps its
    /// heartbeat fresh is left alone; only a stale one is taken over.
    pub claimant: Option<NodeId>,
    pub claimed_ms: Option<u64>,
}

/// Created before the first fleet-durable ack. The open fragment begins at
/// the bucket's current end, so every frame a member will ever be asked
/// about is above what the bucket already holds.
pub fn create_record(ensemble: BTreeSet<NodeId>, bucket_end: Offset) -> Option<LogRecord> {
    if ensemble.is_empty() {
        return None;
    }
    Some(LogRecord {
        epoch: 1,
        ensemble,
        tiered: bucket_end,
        state: LogState::Open,
        claimant: None,
        claimed_ms: None,
    })
}

/// One follower's copy of one node log: the open fragment `(base, end]` at
/// its fragment epoch, plus the persisted seal mark.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowerLog {
    pub fragment_epoch: Epoch,
    pub base: Offset,
    pub end: Offset,
    /// Highest epoch sealed. Appends at or below it are refused forever.
    pub sealed_to: Epoch,
}

impl FollowerLog {
    /// An empty joiner: covered by the bucket through `base`, holding
    /// nothing.
    pub fn join(fragment_epoch: Epoch, base: Offset) -> Self {
        FollowerLog {
            fragment_epoch,
            base,
            end: base,
            sealed_to: 0,
        }
    }

    /// Accept one frame. Refuses the wrong fragment epoch, anything at or
    /// below the seal mark, and gaps — a fragment is contiguous or it is
    /// not a fragment.
    pub fn accept_append(&mut self, epoch: Epoch, offset: Offset) -> bool {
        if epoch != self.fragment_epoch || epoch <= self.sealed_to || offset != self.end + 1 {
            return false;
        }
        self.end = offset;
        true
    }

    /// Persist a seal through `epoch` and report the final contiguous end.
    /// The fragment is frozen from here; write-all makes this one answer
    /// sufficient to certify every acknowledged frame.
    pub fn seal(&mut self, epoch: Epoch) -> Offset {
        if epoch > self.sealed_to {
            self.sealed_to = epoch;
        }
        self.end
    }

    /// Drop frames the record already proves tiered.
    pub fn truncate(&mut self, tiered: Offset) {
        if tiered > self.base {
            self.base = tiered;
        }
    }
}

/// The leader's believed (epoch, ensemble). Deliberately a belief: a fenced
/// leader still acts on it, and safety must come from seal marks and lost
/// CASes, not from wishful record reads on the ack path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderView {
    pub epoch: Epoch,
    pub ensemble: BTreeSet<NodeId>,
}

/// Write-all, ack-all: `next` may acknowledge only when every ensemble
/// member has confirmed it at the leader's fragment epoch. `ends` carries
/// each member's confirmed contiguous end; a member missing from it has
/// confirmed nothing.
pub fn ack_fleet_allowed(view: &LeaderView, ends: &BTreeMap<NodeId, Offset>, next: Offset) -> bool {
    !view.ensemble.is_empty()
        && view
            .ensemble
            .iter()
            .all(|member| ends.get(member).is_some_and(|end| *end >= next))
}

/// A reconfiguration the CAS may publish. `tier_through` must be durable in
/// the bucket before the CAS lands: the new members join empty, and the
/// bucket is the only thing covering their emptiness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigPlan {
    pub tier_through: Offset,
    pub record: LogRecord,
}

pub fn plan_reconfigure(
    current: &LogRecord,
    log_end: Offset,
    ensemble: BTreeSet<NodeId>,
) -> Option<ReconfigPlan> {
    if current.state != LogState::Open || ensemble.is_empty() || ensemble == current.ensemble {
        return None;
    }
    Some(ReconfigPlan {
        tier_through: log_end,
        record: LogRecord {
            epoch: current.epoch + 1,
            ensemble,
            tiered: log_end,
            state: LogState::Open,
            claimant: None,
            claimed_ms: None,
        },
    })
}

/// Recovery step one: fence via the record. S3 is the root of truth, so
/// this CAS is what makes every later tiering or reconfiguration attempt by
/// the old leader fail. Sealing followers comes after, never before.
pub fn start_recovery(record: &LogRecord, claimant: &str, now_ms: u64) -> Option<LogRecord> {
    if record.state != LogState::Open {
        return None;
    }
    Some(LogRecord {
        state: LogState::Recovering,
        claimant: Some(claimant.to_string()),
        claimed_ms: Some(now_ms),
        ..record.clone()
    })
}

/// Whether `claimant` owns the recovery in progress.
pub fn recovery_claimed_by(record: &LogRecord, claimant: &str) -> bool {
    record.state == LogState::Recovering && record.claimant.as_deref() == Some(claimant)
}

/// Whether another node's claim is fresh: its last heartbeat is younger
/// than `stale_after_ms`. A record from before claims carried a heartbeat
/// reads as stale, so an upgrade never waits on a claim it cannot judge.
pub fn recovery_claim_live(record: &LogRecord, now_ms: u64, stale_after_ms: u64) -> bool {
    record.state == LogState::Recovering
        && record.claimant.is_some()
        && record
            .claimed_ms
            .is_some_and(|claimed| now_ms.saturating_sub(claimed) < stale_after_ms)
}

/// The claimant's heartbeat. `None` when the claim is no longer this
/// node's: the recovery was taken over and this node must stop.
pub fn refresh_recovery(record: &LogRecord, claimant: &str, now_ms: u64) -> Option<LogRecord> {
    if !recovery_claimed_by(record, claimant) {
        return None;
    }
    Some(LogRecord {
        claimed_ms: Some(now_ms),
        ..record.clone()
    })
}

/// Take a recovery over from a claimant whose heartbeat went stale. `None`
/// while the claim is live or the log is not recovering.
pub fn take_over_recovery(
    record: &LogRecord,
    claimant: &str,
    now_ms: u64,
    stale_after_ms: u64,
) -> Option<LogRecord> {
    if record.state != LogState::Recovering || recovery_claim_live(record, now_ms, stale_after_ms) {
        return None;
    }
    Some(LogRecord {
        claimant: Some(claimant.to_string()),
        claimed_ms: Some(now_ms),
        ..record.clone()
    })
}

/// The certified cut: the final contiguous end of any sealed member, floored
/// at what is already tiered. Any one sealed member suffices for acked
/// frames (write-all); taking a longer end preserves acked-but-unconfirmed
/// frames, which is safe — delivering an unacknowledged write is allowed,
/// losing an acknowledged one is not.
pub fn certify(tiered: Offset, sealed_end: Offset) -> Offset {
    tiered.max(sealed_end)
}

/// Recovery final step: the certified tail is durable in the bucket, so the
/// record seals at the cut and every per-cell takeover may treat the bucket
/// as complete.
pub fn finish_recovery(record: &LogRecord, cert: Offset) -> Option<LogRecord> {
    if record.state != LogState::Recovering {
        return None;
    }
    Some(LogRecord {
        tiered: cert,
        state: LogState::Sealed,
        claimant: None,
        claimed_ms: None,
        ..record.clone()
    })
}

/// May a per-cell takeover of this node's cells treat the bucket as
/// complete? Absence is a proof, not a guess: the record is created before
/// the first fleet-durable ack and never deleted, so no record means the
/// node never acknowledged anything the bucket does not hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TakeoverGate {
    BucketComplete,
    RecoverFirst,
}

pub fn takeover_gate(record: Option<&LogRecord>) -> TakeoverGate {
    match record {
        None => TakeoverGate::BucketComplete,
        Some(record) if record.state == LogState::Sealed => TakeoverGate::BucketComplete,
        Some(_) => TakeoverGate::RecoverFirst,
    }
}

/// What the maintenance loop must do with its OWN session's record before
/// it may open a fleet ensemble. The record is keyed by (node,
/// generation), so the only record this session can ever read here is one
/// it wrote itself: absent on the session's first open, present for a
/// within-session reconfiguration. A predecessor session's record is a
/// dead session's record like any other, recovered by the sweep or the
/// takeover interlock, never stepped past here — which is what deleted the
/// RecoverFirst arm, the revive/reopen guard, and the recovery epoch pin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaintainStep {
    /// No record yet: create one before the first fleet-durable ack.
    CreateFresh,
    /// Our open (or, after a failed close, sealed) record: step the epoch
    /// and reopen with a fresh ensemble.
    Reopen(Epoch),
    /// Our own record is mid-recovery: a rival is fencing this session,
    /// which can only mean our lease looks expired from where it stands.
    /// Reopening would race its seal CAS — both outcomes are safe under
    /// write-all plus the seal marks, but the churn buys nothing. Hold
    /// this tick; the next pass finds Sealed and reopens, or our own
    /// lease loss fences us first.
    Wait,
}

pub fn maintain_step(record: Option<&LogRecord>) -> MaintainStep {
    match record {
        None => MaintainStep::CreateFresh,
        Some(record) if record.state == LogState::Recovering => MaintainStep::Wait,
        Some(record) => MaintainStep::Reopen(record.epoch + 1),
    }
}

/// The graceful shutdown's seal guard — the only seal producer besides
/// recovery. The record must still be Open at the closing shipper's
/// epoch, no batch may sit between capture and credit, and every shipped
/// frame must live in the PER-CELL layout: a sealed record tells every
/// future recovery there is nothing to gather, so credited bundle
/// coverage must never satisfy this guard — the bundle-resident acked
/// tail would be orphaned behind the seal. Refusal is always safe: an
/// Open record just means the next incarnation's recovery drains the
/// bundles.
pub fn graceful_seal_allowed(
    record: &LogRecord,
    epoch: Epoch,
    batch_in_flight: bool,
    per_cell_complete: bool,
) -> bool {
    record.state == LogState::Open && record.epoch == epoch && !batch_in_flight && per_cell_complete
}

/// A bundle flush may credit durability only if the record is still Open
/// at the shipper's epoch AFTER the PUT. The PUT itself is unconditional
/// and proves nothing: a healed zombie whose record a recoverer already
/// sealed can still land bundle objects, and crediting them would ack
/// rows no future takeover reads — the sealed record says the bucket is
/// complete, and takeovers read per-cell prefixes.
pub fn bundle_credit_allowed(record: Option<&LogRecord>, epoch: Epoch) -> bool {
    record.is_some_and(|record| record.epoch == epoch && record.state == LogState::Open)
}

/// A bundle object is deletable exactly when every row it carries is
/// covered by the per-cell layout — the watermark that keeps recovery's
/// whole-prefix gather bounded without ever deleting the only copy of an
/// acked row. `rows` pairs each row's TXID with the covered watermark of
/// its (cell, cell epoch).
pub fn bundle_deletable(rows: impl IntoIterator<Item = (u64, u64)>) -> bool {
    rows.into_iter().all(|(txid, covered)| txid <= covered)
}

/// A follower fragment is garbage exactly when its epoch is closed: the
/// record moved past it (a reconfiguration or a reopened incarnation), or
/// recovery sealed the record at it. An open current-epoch fragment is
/// the durability witness and must never be collected.
pub fn fragment_closed(record: &LogRecord, fragment_epoch: Epoch) -> bool {
    record.epoch > fragment_epoch
        || (record.epoch == fragment_epoch && record.state == LogState::Sealed)
}

/// maintain()'s reconfiguration barrier: the epoch may step only when no
/// batch sits between capture and credit (its frames are invisible to the
/// coverage counters) and every fleet-shipped frame is bucket-covered —
/// the precondition that makes the old fragments abandonable garbage and
/// the new members' emptiness sound.
pub fn may_reconfigure(batch_in_flight: bool, all_shipped_covered: bool) -> bool {
    !batch_in_flight && all_shipped_covered
}

/// The ship loop's truncation ledger: outstanding batches oldest-first,
/// and the covered watermark that rides the next append as the followers'
/// `truncate_to`. Sequences restart with the epoch, so the ledger resets
/// with it — a covered watermark surviving an epoch swap instructs fresh
/// followers to delete entries they just fsync'd. `Batch`
/// is whatever the executor needs to decide coverage; the ledger decides
/// only ordering, the watermark, and the reset.
#[derive(Debug)]
pub struct ShipLedger<Batch> {
    epoch: Epoch,
    covered_seq: Offset,
    outstanding: std::collections::VecDeque<(Offset, Batch)>,
}

impl<Batch> Default for ShipLedger<Batch> {
    fn default() -> Self {
        ShipLedger {
            epoch: 0,
            covered_seq: 0,
            outstanding: std::collections::VecDeque::new(),
        }
    }
}

impl<Batch> ShipLedger<Batch> {
    /// The stream identity check, before anything else each round: a new
    /// epoch is a new sequence space and the ledger starts over.
    pub fn observe_epoch(&mut self, epoch: Epoch) {
        if epoch != self.epoch {
            self.epoch = epoch;
            self.outstanding.clear();
            self.covered_seq = 0;
        }
    }

    /// A batch was acknowledged by every member through `last_seq`.
    pub fn shipped(&mut self, last_seq: Offset, batch: Batch) {
        self.outstanding.push_back((last_seq, batch));
    }

    /// Advance the covered watermark over every leading batch `covered`
    /// accepts. `covered_seq` is the one way to read the result — the
    /// followers' `truncate_to`.
    pub fn advance(&mut self, mut covered: impl FnMut(&Batch) -> bool) {
        while let Some((last_seq, batch)) = self.outstanding.front() {
            if covered(batch) {
                self.covered_seq = *last_seq;
                self.outstanding.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn covered_seq(&self) -> Offset {
        self.covered_seq
    }
}

/// Why a node that wants the fleet posture is not serving fleet
/// acknowledgements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shortfall {
    /// No live peer exists. An ordinary single-node fleet: the write waits
    /// for the bucket because there is nobody to replicate to.
    NoPeer,
    /// Live peers exist and none can be recruited. An operator can act on
    /// this; a single-node fleet is not a fault.
    NoEligiblePeer,
}

/// Should the node report its fleet shortfall now?
///
/// `maintain` runs on a ticker and returns as soon as it finds no member to
/// recruit, so a fleet that never formed an ensemble reported nothing at all:
/// `log ensemble degraded` fires only when an ensemble that existed was lost.
/// A single-node fleet therefore ran the default fleet posture on bucket
/// acknowledgements forever, with no line explaining the latency or naming
/// the fix. Legible failure is the point; silence is the defect.
///
/// The interval bounds the repetition, and `last_ms` of `None` reports at
/// once so the first tick after a start says what the node is doing.
pub fn fleet_shortfall(
    live_peers: usize,
    now_ms: u64,
    last_ms: Option<u64>,
    interval_ms: u64,
) -> Option<Shortfall> {
    if let Some(last) = last_ms {
        if now_ms.saturating_sub(last) < interval_ms {
            return None;
        }
    }
    Some(if live_peers == 0 {
        Shortfall::NoPeer
    } else {
        Shortfall::NoEligiblePeer
    })
}
