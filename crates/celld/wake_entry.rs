// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The wake-entry gate: the durable discovery entry an armed alarm needs and
//! the service that holds it. Engine-neutral; `crate::js` re-exports every
//! item at its old path.
use crate::asyncrt;
#[cfg(celld_internal_tests)]
use std::collections::HashMap;
#[cfg(celld_internal_tests)]
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
#[cfg(celld_internal_tests)]
use std::sync::Mutex;
use std::sync::OnceLock;

/// The arm-time wake-entry gate, output-gate style (matching Durable
/// Objects): `setAlarm()` resolves optimistically on the committed local
/// write — it never yields the cell's event scheduling to remote I/O — and
/// the cell's response edge is withheld until every wake-entry PUT that this
/// event registered before its response boundary has landed. Invariant: an
/// arm the caller has OBSERVED acknowledged (received the response) is
/// covered by a durable entry.
pub struct ArmGate {
    pub bucket: crate::bucket::Bucket,
    pub flusher: Arc<crate::wake::WakeFlusher>,
}

pub(crate) type ArmGateRx = tokio::sync::oneshot::Receiver<Result<(), String>>;

#[derive(Default)]
pub(crate) struct WakeEntryService {
    pub(crate) gate: OnceLock<ArmGate>,
    #[cfg(celld_internal_tests)]
    pub(crate) test_pending: Mutex<HashMap<String, Vec<ArmGateRx>>>,
    #[cfg(celld_internal_tests)]
    pub(crate) scripted: Mutex<std::collections::VecDeque<ArmGateRx>>,
    #[cfg(celld_internal_tests)]
    pub(crate) scripted_by_cell: Mutex<HashMap<String, std::collections::VecDeque<ArmGateRx>>>,
    #[cfg(celld_internal_tests)]
    pub(crate) drop_next_gated_reply_task: AtomicBool,
    #[cfg(all(test, celld_internal_tests))]
    pub(crate) egress_gate_samples: Mutex<HashMap<(String, celld_logic::Channel), u64>>,
}

pub fn set_arm_gate(gate: ArmGate) {
    let _ = asyncrt::services().wake_entry().gate.set(gate);
}

/// Drop the coverage cache when this node gives up the writer.
pub fn forget_wake_entry(cell: &str) {
    let services = asyncrt::services();
    if let Some(gate) = services.wake_entry().gate.get() {
        gate.flusher.forget(cell);
    }
}

/// Ensure coverage only. Retirement additionally requires the host's bound
/// replication and ownership proof, which the Actor supplies separately.
pub(crate) async fn publish_observed_wake_entry(
    cell: &str,
    alarm: celld_logic::wake::AlarmSnapshot,
) -> anyhow::Result<()> {
    let services = asyncrt::services();
    if let Some(gate) = services.wake_entry().gate.get() {
        gate.flusher.publish(&gate.bucket, cell, alarm).await?;
    }
    Ok(())
}

pub(crate) async fn maintain_wake_entry(
    cell: &str,
    alarm: celld_logic::wake::AlarmSnapshot,
    host: &impl crate::wake::AlarmHost,
    ownership: &crate::actor::Ownership,
    node: &str,
) -> anyhow::Result<()> {
    let services = asyncrt::services();
    if let Some(gate) = services.wake_entry().gate.get() {
        gate.flusher
            .maintain(&gate.bucket, cell, alarm, host, ownership, node)
            .await?;
    }
    Ok(())
}

/// Launch the durable PUT and return the response edge that observes it.
/// Registration is separate because production binds the receiver to a V8
/// event, while a scripted host binds it to its simulated request.
pub(crate) fn launch_arm_gate(
    cell: &str,
    alarm: celld_logic::wake::AlarmSnapshot,
) -> Option<ArmGateRx> {
    let services = asyncrt::services();
    #[cfg(celld_internal_tests)]
    if let Some(rx) = services
        .wake_entry()
        .scripted_by_cell
        .lock()
        .unwrap()
        .get_mut(cell)
        .and_then(std::collections::VecDeque::pop_front)
    {
        return Some(rx);
    }
    #[cfg(celld_internal_tests)]
    if let Some(rx) = services.wake_entry().scripted.lock().unwrap().pop_front() {
        return Some(rx);
    }
    services.wake_entry().gate.get()?;
    alarm.at_ms()?;
    let cell = cell.to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    asyncrt::spawn(async move {
        let gate = services.wake_entry().gate.get().unwrap();
        let result = gate
            .flusher
            .publish(&gate.bucket, &cell, alarm)
            .await
            .map_err(|error| format!("setAlarm wake entry: {error:#}"));
        let _ = tx.send(result);
    })
    .detach();
    Some(rx)
}
