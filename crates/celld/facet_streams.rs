// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The replication streams of Durable Object facets, shared by both engines.
//!
//! A facet is a database and a replication stream of its own, as a sibling
//! file is in workerd's own server. Its stream nests under the root's
//! coordinates (`engine_api::facet_cell`), is activated on first use with the
//! restore its root's activation used, and stops with the root. A facet has
//! no ownership record and no fence of its own; the root's cover it, since a
//! facet runs only inside its root.

use anyhow::anyhow;
use anyhow::Context as _;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::ltx_replication::Replication;

/// The backoff of a facet stop that failed after its root stopped.
const FACET_STOP_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(50);
const FACET_STOP_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// A resident root object's facets.
struct FacetRoot {
    epoch: u64,
    spec: Option<celld_logic::RestoreSpec>,
    streams: BTreeSet<String>,
    /// The root began to stop. Its facets neither open nor delete until the
    /// stop fails before any facet stopped (`resume`): a facet stopped at
    /// this epoch would otherwise restart its lineage inside an epoch its
    /// eviction sealed, and an open that finished after the stop's snapshot
    /// would outlive the root.
    stopping: bool,
    /// Serializes the root's first opens: two activations of one stream
    /// would unlink the file the first one's connection writes.
    opening: Arc<tokio::sync::Mutex<()>>,
}

/// The facet streams of every resident root on this node.
#[derive(Clone, Default)]
pub(crate) struct FacetStreams(Arc<Mutex<HashMap<String, FacetRoot>>>);

impl FacetStreams {
    /// A root's activation: its facets activate later with `spec`.
    pub(crate) fn register(&self, root: &str, spec: &celld_logic::RestoreSpec) {
        self.0.lock().expect("facet streams poisoned").insert(
            root.to_string(),
            FacetRoot {
                epoch: spec.epoch,
                spec: Some(spec.clone()),
                streams: BTreeSet::new(),
                stopping: false,
                opening: Arc::default(),
            },
        );
    }

    /// Activate a facet's stream, once per root activation, and answer its
    /// database file and whether replication restored it from a replica.
    /// `db_path` is the engine's own placement of a cell's database.
    pub(crate) async fn open(
        &self,
        replication: Option<&Replication>,
        db_path: impl Fn(&str, u64) -> PathBuf,
        root: &str,
        epoch: u64,
        names: &[String],
    ) -> anyhow::Result<(PathBuf, bool)> {
        let facet = crate::engine_api::facet_cell(root, names);
        let path = db_path(&facet, epoch);
        let resident = |roots: &HashMap<String, FacetRoot>| {
            roots
                .get(root)
                .filter(|entry| entry.epoch == epoch && !entry.stopping)
                .map(|entry| {
                    (
                        entry.streams.contains(&facet),
                        entry.spec.clone(),
                        entry.opening.clone(),
                    )
                })
                .ok_or_else(|| anyhow!("{root} epoch {epoch} is not resident"))
        };
        let opening = {
            let roots = self.0.lock().expect("facet streams poisoned");
            let (open, _, opening) = resident(&roots)?;
            if open {
                return Ok((path, false));
            }
            opening
        };
        let _opening = opening.lock().await;
        let spec = {
            let roots = self.0.lock().expect("facet streams poisoned");
            let (open, spec, _) = resident(&roots)?;
            if open {
                return Ok((path, false));
            }
            spec
        };
        let restored = match (replication, spec) {
            (Some(replication), Some(mut spec)) => {
                // A clean reload resumes the files it closed. A facet that
                // was not open then has none, so it restores as any facet
                // of this epoch does.
                if spec.resume_local && crate::asyncrt::fs().metadata(&path).is_err() {
                    spec.resume_local = false;
                }
                let (restored_path, restored, _) =
                    replication.restore(&facet, &spec, false).await?;
                anyhow::ensure!(
                    restored_path == path,
                    "replication restored {} instead of {}",
                    restored_path.display(),
                    path.display()
                );
                restored
            }
            (Some(_), None) => anyhow::bail!("{root} was activated without a restore"),
            (None, _) => {
                let parent = path.parent().context("facet database has no parent")?;
                crate::asyncrt::fs()
                    .create_dir_all(parent)
                    .with_context(|| format!("create facet directory {}", parent.display()))?;
                false
            }
        };
        let mut roots = self.0.lock().expect("facet streams poisoned");
        match roots
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch && !entry.stopping)
        {
            Some(entry) => {
                entry.streams.insert(facet);
            }
            // The root stopped while the stream activated.
            None => {
                drop(roots);
                if let Some(replication) = replication {
                    replication.ltx().discard(&facet, epoch);
                }
                anyhow::bail!("{root} epoch {epoch} stopped while its facet opened");
            }
        }
        Ok((path, restored))
    }

    /// Delete a facet's stream and every stream below it, resident or not,
    /// locally and in the bucket. `local` is the facet's local directory.
    /// Only the root's resident owner deletes: a delete that runs after the
    /// root moved would remove what the new owner writes.
    pub(crate) async fn delete(
        &self,
        replication: Option<&Replication>,
        local: impl Fn(&str) -> PathBuf,
        root: &str,
        epoch: u64,
        names: &[String],
    ) -> anyhow::Result<()> {
        let facet = crate::engine_api::facet_cell(root, names);
        let below = format!("{facet}/");
        let doomed: Vec<String> = {
            let mut roots = self.0.lock().expect("facet streams poisoned");
            let entry = roots
                .get_mut(root)
                .filter(|entry| entry.epoch == epoch && !entry.stopping)
                .ok_or_else(|| anyhow!("{root} epoch {epoch} is not resident"))?;
            let doomed: Vec<String> = entry
                .streams
                .iter()
                .filter(|stream| **stream == facet || stream.starts_with(&below))
                .cloned()
                .collect();
            for stream in &doomed {
                entry.streams.remove(stream);
            }
            doomed
        };
        match replication {
            Some(replication) => {
                for stream in &doomed {
                    replication.ltx().discard(stream, epoch);
                }
                replication.ltx().delete_streams(&facet).await
            }
            None => {
                let local = local(&facet);
                match crate::asyncrt::fs().remove_dir_all(&local) {
                    Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                        Err(error).with_context(|| format!("remove facet {}", local.display()))
                    }
                    _ => Ok(()),
                }
            }
        }
    }

    /// The root begins to stop: see `FacetRoot::stopping`. Answers the
    /// streams the stop must stop, after which no open can add one.
    pub(crate) fn stopping(&self, root: &str, epoch: u64) -> Vec<String> {
        let mut roots = self.0.lock().expect("facet streams poisoned");
        match roots.get_mut(root).filter(|entry| entry.epoch == epoch) {
            Some(entry) => {
                entry.stopping = true;
                entry.streams.iter().cloned().collect()
            }
            None => Vec::new(),
        }
    }

    /// Stop a root and its facets. The root's stream stops first, and only
    /// while it is resident: a retry after the root stopped goes on to the
    /// facets that remain. A root stop that fails or is abandoned leaves
    /// every facet resident and resumes them, so a root that restarts in
    /// place keeps its facets. The facets stop after the root, each
    /// forgotten once it stopped. Nothing moves the root's ownership before
    /// this returns, so every facet still stops before a new owner starts.
    ///
    /// Once the root stopped, a facet stop that fails is retried here until
    /// it succeeds instead of returning: the facet's handoff snapshot is what
    /// carries its acknowledged tail into the bucket, and a stop that failed
    /// would let the caller restart a root whose stream is gone. The actor's
    /// release loop has no overall timeout for the same reason.
    pub(crate) async fn stop_root<R, F, FF>(
        &self,
        root: &str,
        epoch: u64,
        root_resident: bool,
        stop_root: impl FnOnce() -> R,
        mut stop_facet: F,
    ) -> anyhow::Result<()>
    where
        R: std::future::Future<Output = anyhow::Result<()>>,
        F: FnMut(String) -> FF,
        FF: std::future::Future<Output = anyhow::Result<()>>,
    {
        self.stopping(root, epoch);
        if root_resident {
            if let Err(error) = stop_root().await {
                self.resume(root, epoch);
                return Err(error);
            }
        }
        for facet in self.stopping(root, epoch) {
            let mut delay = FACET_STOP_RETRY_FIRST;
            while let Err(error) = stop_facet(facet.clone()).await {
                tracing::warn!(
                    root,
                    epoch,
                    facet,
                    %error,
                    "a facet stop failed after its root stopped; retrying"
                );
                crate::asyncrt::sleep(delay).await;
                delay = (delay * 2).min(FACET_STOP_RETRY_MAX);
            }
            self.stopped(root, epoch, &facet);
        }
        self.forget(root, epoch);
        Ok(())
    }

    /// The root's stop failed before any facet stopped, and the root
    /// restarts in place: its facets open and delete again.
    pub(crate) fn resume(&self, root: &str, epoch: u64) {
        if let Some(entry) = self
            .0
            .lock()
            .expect("facet streams poisoned")
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch)
        {
            entry.stopping = false;
        }
    }

    /// The facet streams of a resident root.
    pub(crate) fn resident(&self, root: &str, epoch: u64) -> Vec<String> {
        self.0
            .lock()
            .expect("facet streams poisoned")
            .get(root)
            .filter(|entry| entry.epoch == epoch)
            .map(|entry| entry.streams.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// A facet stream that stopped. The root's entry stays until the root
    /// itself stops, so a stop that fails and restarts the root in place
    /// can still open its facets.
    pub(crate) fn stopped(&self, root: &str, epoch: u64, stream: &str) {
        if let Some(entry) = self
            .0
            .lock()
            .expect("facet streams poisoned")
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch)
        {
            entry.streams.remove(stream);
        }
    }

    /// The root stopped: its entry goes.
    pub(crate) fn forget(&self, root: &str, epoch: u64) {
        let mut roots = self.0.lock().expect("facet streams poisoned");
        if roots.get(root).is_some_and(|entry| entry.epoch == epoch) {
            roots.remove(root);
        }
    }
}
