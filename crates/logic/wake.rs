// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Alarm-wake entry reconciliation, sans-IO — plus the wake key scheme.
//!
//! `WakeCore`'s `decide` is a pure transition; [`Reconcile`] holds the
//! ordering rules an executor must obey while performing it. Any executor —
//! production's async S3 flusher or a deterministic fake — asks for steps and
//! reports outcomes, so the rules cannot diverge between them. The key scheme
//! (`entry_key` / `parse_entry_key`) lives here so both sides share one
//! definition.
use super::Ms;
use std::collections::HashMap;
use std::collections::HashSet;

/// Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// The time bucket a due timestamp falls in: minute precision, UTC,
/// lexicographically ordered so the waker LISTs due buckets in order.
fn minute_bucket(due_ms: i64) -> String {
    let mins = due_ms.div_euclid(60_000);
    let (y, mo, d) = civil_from_days(mins.div_euclid(1440));
    let m = mins.rem_euclid(1440);
    format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}", m / 60, m % 60)
}

pub fn entry_key(due_ms: i64, cell: &str) -> String {
    format!("wake/{}/{}", minute_bucket(due_ms), cell)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// The due-minute floor a `YYYY-MM-DDTHH:MM` bucket names, in ms.
fn parse_minute(minute: &str) -> Option<i64> {
    if minute.len() != 16 {
        return None;
    }
    let y: i64 = minute.get(0..4)?.parse().ok()?;
    let mo: u32 = minute.get(5..7)?.parse().ok()?;
    let d: u32 = minute.get(8..10)?.parse().ok()?;
    let h: i64 = minute.get(11..13)?.parse().ok()?;
    let mi: i64 = minute.get(14..16)?.parse().ok()?;
    if minute.get(4..5)? != "-" || minute.get(10..11)? != "T" {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 1440 + h * 60 + mi) * 60_000)
}

/// Inverse of `entry_key`: (due minute floor in ms, cell scope).
///
/// The scope is the remainder of a bucket key, so it is the one place a scope
/// enters celld without passing a route. `due_scan` sends it as a `WakeHint`,
/// which reaches the ownership CAS and `Effect::Restore` exactly as a request
/// does, so it carries the same charset fence. An entry that fails the fence is
/// ignored rather than repaired: it cannot name a cell this node can serve, and
/// a bad entry left by an older node would otherwise replay on every tick.
pub fn parse_entry_key(key: &str) -> Option<(i64, String)> {
    let rest = key.strip_prefix("wake/")?;
    let (minute, cell) = rest.split_at(rest.char_indices().nth(16)?.0);
    let cell = cell.strip_prefix('/')?;
    if !crate::cell::valid_cell_scope(cell) {
        return None;
    }
    Some((parse_minute(minute)?, cell.to_string()))
}

/// The due minute a `wake/YYYY-MM-DDTHH:MM` bucket prefix names, in ms.
///
/// A delimiter listing answers with the buckets rather than the keys inside
/// them, so the waker needs the minute without a cell attached to decide which
/// buckets have come due.
pub fn parse_minute_prefix(prefix: &str) -> Option<i64> {
    parse_minute(prefix.strip_prefix("wake/")?)
}

/// What the bucket needs for one cell given its committed alarm — the pure
/// transition's output, performed by `reconcile`.
pub enum Op {
    Put { key: String, due_ms: Ms },
    Delete { key: String },
}

/// What the bucket is believed to hold for one cell.
struct Entry {
    due_ms: Ms,
    key: String,
    /// True once this core PUT the object and saw it succeed. An adopted entry
    /// is believed-present but unproven: it may be deleted when its alarm is
    /// consumed, but may never satisfy the fail-closed eviction gate.
    verified: bool,
    /// A consume-delete for this exact key has been DECIDED but not yet
    /// handed to the executor. In that window an arm must not ride on an
    /// entry that is about to vanish — that race acked alarms with no
    /// durable entry. An arm during the
    /// window cancels the delete instead.
    delete_pending: bool,
}

/// The reified `WakeFlusher`: owned state, no lock, no I/O.
#[derive(Default)]
pub struct WakeCore {
    flushed: HashMap<String, Entry>,
    /// Keys whose DELETE was HANDED OUT (`take_delete` passed) and whose
    /// store call has not reported back, per cell. Past the handout nothing
    /// can cancel the delete, so an arm must not ride on the key either — it
    /// pays a PUT, which the executor sequences after the delete settles
    /// (concurrent same-key writes have no order at the store).
    ///
    /// Per cell rather than per entry: a MOVE-delete targets the key the
    /// replacement PUT has already replaced in `flushed`, so the entry that
    /// could carry the flag is gone by the time the delete is handed out. It
    /// is exactly that delete which raced the arm PUT that re-tightened the
    /// alarm back onto the old minute, deleting the entry of an alarm this
    /// node had already acknowledged.
    deleting: HashMap<String, HashSet<String>>,
}

impl WakeCore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Is a DELETE of this exact key on the wire for this cell?
    fn deleting_key(&self, cell: &str, key: &str) -> bool {
        self.deleting
            .get(cell)
            .is_some_and(|keys| keys.contains(key))
    }

    /// Arm-time decision: the PUT that must land before this arm is acked to
    /// the application, or `None` when the durable bound already covers it. A
    /// proven entry at the same or an EARLIER minute suffices — a stale-early
    /// entry costs one spurious wake, never a lost one — so recurring re-arms
    /// within a covered minute and postponements are free; only tightening
    /// pays the synchronous PUT.
    pub fn arm(&mut self, cell: &str, next_alarm_ms: Ms) -> Option<Op> {
        if next_alarm_ms < 0 {
            return None; // deletes are never synchronous
        }
        let want = entry_key(next_alarm_ms, cell);
        let vanishing = self
            .flushed
            .get(cell)
            .is_some_and(|e| self.deleting_key(cell, &e.key));
        match self.flushed.get_mut(cell) {
            // The entry's delete is already on the wire: nothing can cancel
            // it, so the entry covers nothing. Pay the PUT; the executor
            // sequences it after the in-flight delete settles.
            Some(_) if vanishing => Some(Op::Put {
                key: want,
                due_ms: next_alarm_ms,
            }),
            // Riding on an entry a consume is about to delete would ack an
            // alarm that ends up entryless. Cancel that delete instead: this
            // arm now owns the entry, and `take_delete` will refuse it.
            Some(e) if e.verified && e.key <= want && e.delete_pending => {
                e.delete_pending = false;
                if e.key == want {
                    None
                } else {
                    // The entry is stale-early: keep it (it still covers) but
                    // re-assert at the wanted key so coverage is exact and the
                    // cancelled delete cannot strand a later postponement.
                    Some(Op::Put {
                        key: want,
                        due_ms: next_alarm_ms,
                    })
                }
            }
            Some(e) if e.verified && e.key <= want => None,
            _ => Some(Op::Put {
                key: want,
                due_ms: next_alarm_ms,
            }),
        }
    }

    /// Pure transition behind [`WakeCore::reconcile_plan`]. The entry key,
    /// not the exact due time, is an entry's identity: two alarms in one
    /// minute share it.
    ///
    /// Ordering: a moved entry PUTs the new key before deleting the old one,
    /// so no schedule point leaves an armed alarm entryless — a crash between
    /// the two ops strands one extra entry (one spurious wake), never zero.
    ///
    /// `consume_durable` gates the final delete of a consumed alarm: false
    /// while the consuming commit is not yet replicated, in which case the
    /// entry must outlive the local commit — deleting it and then losing the
    /// commit to the replication lag would leave replicated truth armed with
    /// no entry, the one unrecoverable state.
    pub fn decide(&mut self, cell: &str, next_alarm_ms: Ms, consume_durable: bool) -> Vec<Op> {
        if next_alarm_ms < 0 {
            let on_the_wire = self
                .flushed
                .get(cell)
                .is_some_and(|e| self.deleting_key(cell, &e.key));
            return match self.flushed.get_mut(cell) {
                // `!on_the_wire`: the delete is already on the wire; a second
                // one would race the first for nothing.
                Some(e) if consume_durable && !on_the_wire => {
                    // Arm the guard: an arm before this delete lands cancels
                    // it, and `take_delete` re-checks at execution time.
                    e.delete_pending = true;
                    vec![Op::Delete { key: e.key.clone() }]
                }
                _ => vec![],
            };
        }
        let held = self.flushed.get(cell);
        let want = entry_key(next_alarm_ms, cell);
        match held {
            None => vec![Op::Put {
                key: want,
                due_ms: next_alarm_ms,
            }],
            // The tracked entry is vanishing: whatever its key, the armed
            // alarm must be re-asserted. No delete op — one is in flight.
            Some(e) if self.deleting_key(cell, &e.key) => vec![Op::Put {
                key: want,
                due_ms: next_alarm_ms,
            }],
            Some(e) if e.key != want => vec![
                Op::Put {
                    key: want.clone(),
                    due_ms: next_alarm_ms,
                },
                Op::Delete { key: e.key.clone() },
            ],
            // adopted but never proven: assert it so the cell can be evicted
            Some(e) if !e.verified => {
                vec![Op::Put {
                    key: want,
                    due_ms: next_alarm_ms,
                }]
            }
            Some(_) => vec![],
        }
    }

    /// Take responsibility for the entry a restored alarm implies. Without
    /// this, a cell revived anywhere but the process that evicted it has no
    /// record to delete from: its entry outlives the alarm and re-wakes the
    /// cell on every lease lapse.
    pub fn adopt(&mut self, cell: &str, due_ms: Ms) {
        if due_ms < 0 {
            return;
        }
        self.flushed
            .entry(cell.to_string())
            .or_insert_with(|| Entry {
                due_ms,
                key: entry_key(due_ms, cell),
                verified: false,
                delete_pending: false,
            });
    }

    /// Is this exact committed alarm durably covered by a proven entry? The
    /// fail-closed gate: eviction of an alarm-bearing cell requires it.
    pub fn covered(&self, cell: &str, next_alarm_ms: Ms) -> bool {
        self.flushed.get(cell).is_some_and(|e| {
            e.verified
                && e.key == entry_key(next_alarm_ms, cell)
                && !self.deleting_key(cell, &e.key)
        })
    }

    /// Is any delete for this cell on the wire — a consume of the tracked
    /// entry or a move-delete of a key it no longer tracks? Any PUT for the
    /// cell must be sequenced after it settles: the store gives concurrent
    /// same-key writes no order, so a PUT racing the DELETE can lose and
    /// leave a confirmed belief with no entry.
    ///
    /// A caller that already knows its key must ask `key_delete_in_flight`
    /// instead. This question is the conservative one, and only a caller
    /// that cannot yet name its key needs it.
    pub fn delete_in_flight(&self, cell: &str) -> bool {
        self.deleting.get(cell).is_some_and(|keys| !keys.is_empty())
    }

    /// Is a delete of this exact key on the wire for this cell? The precise
    /// question an arm can ask: only a delete of the key it is about to PUT
    /// can race that PUT, so an arm gated on this waits for nothing else.
    pub fn key_delete_in_flight(&self, cell: &str, key: &str) -> bool {
        self.deleting_key(cell, key)
    }

    /// Entries whose cells this node evicted and whose due time has arrived.
    /// Sorted, unlike production's `HashMap` walk, so the schedule is
    /// reproducible — determinism the sans-IO core owes its executor.
    pub fn due_cells(&self, now_ms: Ms) -> Vec<String> {
        let mut due: Vec<String> = self
            .flushed
            .iter()
            .filter(|(_, e)| e.due_ms <= now_ms)
            .map(|(cell, _)| cell.clone())
            .collect();
        due.sort();
        due
    }

    pub fn tracks(&self, cell: &str) -> bool {
        self.flushed.contains_key(cell)
    }

    /// A PUT landed: the entry is now proven present. Called by the celld
    /// binary's async executor (`WakeFlusher`) after a successful S3 PUT — a
    /// separate crate, so this is `pub`.
    pub fn confirm_put(&mut self, cell: &str, due_ms: Ms, key: String) {
        self.flushed.insert(
            cell.to_string(),
            Entry {
                due_ms,
                key,
                verified: true,
                delete_pending: false,
            },
        );
    }

    /// Immediately before performing a delete: is it still wanted? False when
    /// an arm rode in during the async window and cancelled it — performing
    /// the delete then would strand that acked alarm.
    ///
    /// Every delete this answers for goes on the wire, so every one of them
    /// is recorded: the move-delete below is safe only in the sense that the
    /// replacement entry already exists, never in the sense that its key may
    /// be written concurrently.
    pub fn take_delete(&mut self, cell: &str, key: &str) -> bool {
        let perform = match self.flushed.get_mut(cell) {
            Some(e) if e.key == key && e.delete_pending => {
                e.delete_pending = false;
                true
            }
            // No entry (already retired) or a different key: the move-delete
            // path, which is always safe — a new entry was PUT first.
            None => true,
            Some(e) => e.key != key,
        };
        if perform {
            self.deleting
                .entry(cell.to_string())
                .or_default()
                .insert(key.to_string());
        }
        perform
    }

    /// Stop tracking this cell — after a delete, or when a wake resolved to a
    /// remote owner and its alarm is no longer ours.
    pub fn forget(&mut self, cell: &str) {
        self.flushed.remove(cell);
    }

    /// A delete of `key` settled — landed, or failed past its retries, which
    /// is the same thing here: it is off the wire either way, and a PUT of
    /// that key can no longer lose to it.
    ///
    /// Forget the cell only if that key is still the tracked entry: with
    /// put-before-delete ordering, a moved entry's confirm already replaced
    /// the tracked key, and deleting the old key must not drop belief in the
    /// new one.
    pub fn retire(&mut self, cell: &str, key: &str) {
        if let Some(keys) = self.deleting.get_mut(cell) {
            keys.remove(key);
            if keys.is_empty() {
                self.deleting.remove(cell);
            }
        }
        if self.flushed.get(cell).is_some_and(|e| e.key == key) {
            self.flushed.remove(cell);
        }
    }
}

/// One step of a reconcile the executor must perform.
///
/// `Put` carries its body so the entry's on-the-wire shape is decided once,
/// here, rather than formatted independently by every executor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Put {
        key: String,
        due_ms: Ms,
        body: String,
    },
    Delete {
        key: String,
    },
}

/// A reconcile in progress: the ordering rules, without the I/O.
///
/// The rules are small and both of them are easy to get wrong in a way
/// nothing notices until an alarm is lost. A failed PUT abandons the rest of
/// the batch, because a Delete later in it is only safe once the replacement
/// entry exists. A Delete is re-checked against the core immediately before
/// it is issued, because an arm may have cancelled it while it queued.
///
/// Holding them here means an executor cannot skip them: it asks for the next
/// step and reports what happened. Production drives this against S3 and the
/// simulation drives it against a fake store -- the same rules either way,
/// which is the point.
pub struct Reconcile {
    cell: String,
    steps: std::collections::VecDeque<Op>,
}

impl Reconcile {
    /// The next step to perform, or `None` when the batch is finished.
    ///
    /// Deletes the core has since cancelled are dropped here rather than
    /// handed out, so an executor that performs every step it is given is
    /// automatically correct.
    pub fn next(&mut self, core: &mut WakeCore) -> Option<Step> {
        while let Some(op) = self.steps.pop_front() {
            match op {
                Op::Put { key, due_ms } => {
                    let cell = &self.cell;
                    return Some(Step::Put {
                        body: format!("{{\"cell\":{cell:?},\"due_ms\":{due_ms}}}"),
                        key,
                        due_ms,
                    });
                }
                Op::Delete { key } => {
                    if core.take_delete(&self.cell, &key) {
                        return Some(Step::Delete { key });
                    }
                }
            }
        }
        None
    }

    /// The PUT landed: the entry is present.
    pub fn put_done(&mut self, core: &mut WakeCore, key: String, due_ms: Ms) {
        core.confirm_put(&self.cell, due_ms, key);
    }

    /// The PUT did not land. Whatever remains depended on it.
    pub fn put_failed(&mut self) {
        self.steps.clear();
    }

    /// The DELETE landed, or the object was already gone.
    pub fn delete_done(&mut self, core: &mut WakeCore, key: &str) {
        core.retire(&self.cell, key);
    }
}

impl WakeCore {
    /// Plan the reconcile for one cell. See [`Reconcile`].
    pub fn reconcile_plan(
        &mut self,
        cell: &str,
        next_alarm_ms: Ms,
        consume_durable: bool,
    ) -> Reconcile {
        Reconcile {
            cell: cell.to_string(),
            steps: self.decide(cell, next_alarm_ms, consume_durable).into(),
        }
    }
}

/// The sweep executor's per-cell hint decision, shared with production.
///
/// A cell revived by another process carries a wake entry this core never
/// PUT; the activation hint (the alarm it restored with) is adopted so a
/// consume deletes that entry instead of orphaning it. The hint is consumed
/// EXACTLY ONCE: once the consumed cell is no longer tracked, a still-latched
/// hint re-adopts a phantom entry every cycle and re-deletes it forever — an
/// op-quiescence violation.
pub fn should_adopt_hint(tracks: bool, hint_ms: Ms) -> bool {
    !tracks && hint_ms >= 0
}

/// May this node take the singleton waker-role lease — because it already holds
/// it, or the current holder's lease has expired? An exactly-expired lease MUST
/// be claimable (`<=`, not `<`): the waker is a SINGLE role, so a claim that
/// stalls on the boundary leaves every evicted cell's alarm unwoken until
/// some other node happens to reclaim.
pub fn waker_may_claim(held_by_us: bool, expires_ms: Ms, now_ms: Ms) -> bool {
    held_by_us || expires_ms <= now_ms
}

/// The elected waker's decision for one due entry once it has read the
/// cell's owner record: does this node need to send the `Fleet` hint?
///
/// A live owner fires its own alarms. A resident cell fires from the owner's
/// timer, and a dormant one wakes from the owner's own due scan, which sends
/// it an `Owned` hint. The elected waker's hint is for everything else: a
/// cell nobody owns, a cell whose owner's lease has run out, and a cell this
/// node owns itself. Hinting a live owner's cell risks nothing (the core never
/// steals), but it costs the elected node an activation permit and an owner
/// read per entry, for the whole fleet's due set, every tick.
pub fn elected_hint_needed(owner: Option<&str>, node: &str, owner_live: bool) -> bool {
    match owner {
        None => true,
        Some(owner) if owner == node => true,
        Some(_) => !owner_live,
    }
}
