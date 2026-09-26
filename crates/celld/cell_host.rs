// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The Actor's one boundary to whichever engine runs cells.

use crate::engine_api::StopMode;
use crate::generation::GenerationId;
use crate::runtime::RuntimeManager;

/// The Actor's cell-runtime boundary: the engine's runtime, or the scripted
/// host the deterministic worlds drive.
#[derive(Clone)]
pub(crate) enum CellHost {
    Engine(RuntimeManager),
    #[cfg(all(test, celld_internal_tests))]
    Scripted(crate::conformance_sim_cell_host::SimCellHost),
}

impl From<RuntimeManager> for CellHost {
    fn from(runtime: RuntimeManager) -> Self {
        Self::Engine(runtime)
    }
}

impl CellHost {
    pub(crate) fn local_reload_cells(&self) -> anyhow::Result<Vec<celld_logic::LocalCell>> {
        match self {
            Self::Engine(runtime) => runtime.local_reload_cells(),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.local_reload_cells(),
        }
    }

    pub(crate) async fn restore_cell(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
    ) -> anyhow::Result<celld_logic::RestoreOutcome> {
        match self {
            Self::Engine(runtime) => runtime.restore_cell(cell, spec).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.restore_cell(cell, spec).await,
        }
    }

    /// Start a cell's runtime, reporting the isolate that took its realm and
    /// the application generation that isolate belongs to.
    pub(crate) async fn start_cell(
        &self,
        cell: String,
        epoch: u64,
        fresh: bool,
    ) -> anyhow::Result<(celld_logic::isolate::HeapId, GenerationId)> {
        match self {
            Self::Engine(runtime) => runtime.start_cell(cell, epoch, fresh).await,
            // The scripted host maps its deterministic isolate pool to heap
            // identities on the generation a node boots with; no scripted
            // world adopts another.
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime
                .start_cell(cell, epoch, fresh)
                .await
                .map(|isolate| (isolate, crate::generation::FIRST_GENERATION)),
        }
    }

    /// Take a cell out of its isolate for a generation swap: close its
    /// application database and give the placement back, and touch nothing
    /// else. Ownership, the epoch, the replica, and the wake entry all stay.
    pub(crate) async fn swap_out_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        match self {
            Self::Engine(runtime) => runtime.swap_out_cell(cell, epoch).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.swap_out_cell(cell, epoch).await,
        }
    }

    pub(crate) fn publish_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        match self {
            Self::Engine(runtime) => runtime.publish_cell(cell, epoch),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.publish_cell(cell, epoch),
        }
    }

    pub(crate) async fn ensure_durable(
        &self,
        cell: &str,
        epoch: u64,
        revocable: bool,
    ) -> anyhow::Result<()> {
        match self {
            Self::Engine(runtime) => runtime.ensure_durable(cell, epoch, revocable).await,
            // The scripted host owns a real replicator over a real object
            // store, so it takes the same gate. Letting it skip the gate made
            // the gate unreachable from the deterministic worlds, which is
            // why no case noticed when the pre-close check was removed.
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.ensure_durable(cell, epoch, revocable).await,
        }
    }

    pub(crate) async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<(u64, celld_logic::ProofSource)> {
        match self {
            Self::Engine(runtime) => runtime.await_durable(cell, epoch, position).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.await_durable(cell, epoch, position).await,
        }
    }

    pub(crate) async fn stop_cell(
        &self,
        cell: &str,
        epoch: u64,
        mode: StopMode,
    ) -> anyhow::Result<()> {
        match self {
            Self::Engine(runtime) => runtime.stop_cell(cell, epoch, mode).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.stop_cell(cell, epoch, mode).await,
        }
    }

    pub(crate) fn abort_activity(&self, request_id: crate::js::RequestId) {
        match self {
            Self::Engine(_) => crate::js::abort_request_for_shutdown(request_id),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(_) => {}
        }
    }

    /// Keep the snapshot and its consumer under the host's reporter lock.
    /// An activity completion sent after a newer report can otherwise erase it.
    pub(crate) fn with_alarm<T>(
        &self,
        cell: &str,
        f: impl FnOnce(celld_logic::wake::AlarmSnapshot, bool) -> T,
    ) -> T {
        match self {
            Self::Engine(runtime) => runtime
                .with_alarm_snapshot(cell, |alarm| f(alarm, runtime.alarm_covered(cell, alarm))),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.with_alarm(cell, f),
        }
    }

    /// Legacy scripted drivers supply their own observations. A host connected
    /// to the activity path supplies them through the same boundary as an engine.
    pub(crate) fn alarm_observation(
        &self,
        cell: &str,
    ) -> Option<(celld_logic::wake::AlarmSnapshot, bool)> {
        match self {
            Self::Engine(_) => Some(self.with_alarm(cell, |alarm, covered| (alarm, covered))),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime
                .observes_activity_alarms()
                .then(|| self.with_alarm(cell, |alarm, covered| (alarm, covered))),
        }
    }

    /// Make an armed alarm discoverable before a graceful handoff continues.
    /// The runtime cache and the wake flusher share the reporter ordering, so
    /// re-reading after the awaited reconcile gives the core one observation
    /// that cannot overtake the bucket operation it describes.
    pub(crate) async fn refresh_handoff_alarm_coverage(
        &self,
        cell: &str,
        alarm: celld_logic::wake::AlarmSnapshot,
    ) -> Option<(celld_logic::wake::AlarmSnapshot, bool)> {
        #[cfg(all(test, celld_internal_tests))]
        if matches!(self, Self::Scripted(host) if !host.observes_activity_alarms()) {
            return None;
        }
        if crate::js::publish_observed_wake_entry(cell, alarm)
            .await
            .is_err()
        {
            return None;
        }
        self.alarm_observation(cell)
    }

    pub(crate) async fn fire_alarm(
        &self,
        op: celld_logic::OpId,
        cell: String,
        scheduled_ms: i64,
    ) -> anyhow::Result<(celld_logic::wake::AlarmSnapshot, bool, Option<u64>)> {
        match self {
            Self::Engine(runtime) => runtime.fire_alarm(op, cell, scheduled_ms).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.fire_alarm(op, cell, scheduled_ms).await,
        }
    }

    pub(crate) fn abort_alarm(&self, cell: &str, op: celld_logic::OpId) {
        match self {
            Self::Engine(runtime) => runtime.abort_alarm(cell, op),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(_) => {}
        }
    }
}
