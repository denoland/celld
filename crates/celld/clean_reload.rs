// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The clean-reload certificate a node leaves for its successor.

use crate::asyncrt;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;

const CLEAN_RELOAD_MARKER: &str = ".clean-reload.json";

#[derive(Deserialize, Serialize)]
struct CleanReloadMarker<'a> {
    node: &'a str,
    generation: &'a str,
}

#[derive(Deserialize)]
struct OwnedCleanReloadMarker {
    node: String,
    generation: String,
}

/// Read the fixed-size certificate left by a clean local shutdown. The node
/// lease state machine still decides whether the named generation is live and
/// can be replaced; this local value grants no authority by itself.
pub fn take_clean_reload_generation(data_dir: &Path, node: &str) -> Option<String> {
    let path = data_dir.join(CLEAN_RELOAD_MARKER);
    let filesystem = asyncrt::fs();
    let bytes = filesystem.read(&path).ok()?;
    let _ = filesystem.remove_file(&path);
    let marker: OwnedCleanReloadMarker = serde_json::from_slice(&bytes).ok()?;
    (marker.node == node).then_some(marker.generation)
}

pub fn write_clean_reload_marker(
    data_dir: &Path,
    node: &str,
    generation: &str,
) -> anyhow::Result<()> {
    let filesystem = asyncrt::fs();
    filesystem.create_dir_all(data_dir)?;
    let marker = data_dir.join(CLEAN_RELOAD_MARKER);
    let temporary = data_dir.join(".clean-reload.tmp");
    let body = serde_json::to_vec(&CleanReloadMarker { node, generation })?;
    filesystem.write(&temporary, &body)?;
    filesystem.rename(&temporary, &marker)?;
    Ok(())
}
