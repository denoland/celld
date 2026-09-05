// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The cell CLI times an operator's listing outside the execution boundary.
#![allow(clippy::disallowed_methods)]
// The package macro policy keeps listing data behind `Output`, which is the
// boundary that this module exists to preserve.

//! `celld cell` — the operator's view of the Durable Object instances a
//! fleet holds.
//!
//! A fleet bucket can hold millions of cells, and one `LIST` request
//! returns at most a thousand children. So a listing's cost is set by how
//! many cells exist, not by how many the operator asked to see. The default
//! answer therefore costs one request, `--after` resumes, and `--all` is the
//! explicit request for the whole walk. [`crate::cli_output`] owns those
//! rules; this module supplies the rows.

use anyhow::bail;
use anyhow::Context;
use std::borrow::Cow;

use crate::cli_options::FleetFlags;
use crate::cli_options::FLEET_HELP;
use crate::cli_options::LISTING_HELP;
use crate::cli_output::list;
use crate::cli_output::Bounds;
use crate::cli_output::Format;
use crate::cli_output::Output;
use crate::cli_output::Page;
use crate::cli_output::Record;
use crate::cli_output::Resumable;
use crate::cli_output::Resume;
use crate::note;

/// One Durable Object instance.
struct Cell {
    scope: String,
}

impl Record for Cell {
    fn json(&self) -> serde_json::Value {
        // A scope carries no class when an application named a bare
        // instance. A null reads back as absent rather than as a class
        // named "", and the keys stay present either way so a reader that
        // infers a schema from the first line sees every column.
        let (class, id) = match self.scope.split_once(':') {
            Some((class, id)) => (serde_json::Value::from(class), serde_json::Value::from(id)),
            None => (
                serde_json::Value::Null,
                serde_json::Value::from(self.scope.as_str()),
            ),
        };
        // A fleet holds celld's own cells beside the application's: D1
        // databases, KV namespaces and Workflows are each a reserved class.
        // They are real cells that hold real bytes, so hiding them would
        // understate the bucket -- but an operator asking which Durable
        // Objects their application has does not mean these, so the row
        // says which it is and a reader can filter on it.
        let reserved = class.as_str().is_some_and(crate::deploy::is_reserved_class);
        serde_json::json!({
            "scope": self.scope,
            "class": class,
            "id": id,
            "reserved": reserved,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.scope)
    }
}

impl Resumable for Cell {
    fn cursor(&self) -> &str {
        &self.scope
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ListOptions {
    pub(crate) fleet: FleetFlags,
    pub(crate) bounds: Bounds,
    pub(crate) class: Option<String>,
    pub(crate) json: bool,
}

pub(crate) fn list_options_from_arguments(
    arguments: Vec<String>,
) -> anyhow::Result<Option<ListOptions>> {
    let mut options = ListOptions::default();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        // Rebuilt per iteration so the loop and the flag helpers can share
        // the iterator without holding two borrows at once.
        let mut value = |option: &str| {
            arguments
                .next()
                .ok_or_else(|| anyhow::anyhow!("{option} requires a value"))
        };
        match argument.as_str() {
            "-h" | "--help" => return Ok(None),
            "--json" => options.json = true,
            other => {
                if options.fleet.consume(other, &mut value)? {
                    continue;
                }
                if options.bounds.consume(other, &mut value)? {
                    continue;
                }
                // A class is the only positional, so a leading dash is a
                // mistyped option rather than a class named "--clas".
                if other.starts_with('-') {
                    bail!(
                        "unknown cell list option: {other}; run `celld cell list --help` for usage"
                    );
                }
                if let Some(existing) = options.class.as_deref() {
                    bail!("celld cell list takes one class, and already has {existing:?}");
                }
                options.class = Some(other.to_string());
            }
        }
    }
    options.bounds.validate()?;
    if let Some(class) = options.class.as_deref() {
        if class.contains(':') || !celld_logic::cell::valid_cell_scope(class) {
            bail!("a cell class must use ASCII letters, digits, and `_ - . $`, not {class:?}");
        }
    }
    if let Some(after) = options.bounds.after.as_deref() {
        if !celld_logic::cell::valid_cell_scope(after) {
            bail!("--after takes a cell scope this command printed, not {after:?}");
        }
    }
    Ok(Some(options))
}

pub(crate) fn help_text() -> String {
    format!(
        r#"List the Durable Object instances in the fleet bucket.

USAGE:
  celld cell list [CLASS] --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]

The listing is bounded by default, because a fleet can hold far more cells
than an operator wants to read and each request returns at most {page}
children. A bounded answer reports on stderr that more cells exist.

OPTIONS:
{FLEET_HELP}
{LISTING_HELP}
  -h, --help          Show this help

Output is in the store's key order, which is what makes --after resume
exactly where the previous answer stopped."#,
        page = Bounds::MAX_PAGE,
    )
}

/// The cell scopes in one page of children, in the order the store listed
/// them. `after` is dropped when it repeats: the store resumes from a key,
/// and every key below `cells/<after>/` sorts after that prefix, so the
/// resumed page lists the boundary child again.
pub(crate) fn cell_scopes_from_prefixes<'a>(
    prefixes: impl IntoIterator<Item = String> + 'a,
    class: Option<&'a str>,
    after: Option<&'a str>,
) -> impl Iterator<Item = String> + 'a {
    prefixes
        .into_iter()
        .filter_map(|prefix| prefix.strip_prefix("cells/").map(str::to_string))
        // The scope charset is the engine's storage fence. A prefix that
        // fails it was not written by a celld node, so listing it would
        // present foreign bucket content as a Durable Object instance.
        .filter(|cell| celld_logic::cell::valid_cell_scope(cell))
        .filter(move |cell| after != Some(cell.as_str()))
        .filter(move |cell| {
            class.is_none_or(|class| {
                cell.split_once(':')
                    .is_some_and(|(cell_class, _)| cell_class == class)
            })
        })
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let mut arguments = arguments.into_iter();
    match arguments.next().as_deref() {
        Some("list") => run_list(arguments.collect()).await,
        None | Some("-h") | Some("--help") | Some("help") => {
            Output::new(Format::Text).help(&help_text())
        }
        Some(other) => bail!("unknown cell command: {other}; celld cell list is the only one"),
    }
}

async fn run_list(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = list_options_from_arguments(arguments)? else {
        return Output::new(Format::Text).help(&help_text());
    };
    let storage = options.fleet.resolve("celld cell list")?;
    let store = storage.open().await?;

    let mut out = Output::new(if options.json {
        Format::Json
    } else {
        Format::Text
    });
    let started = std::time::Instant::now();
    let class = options.class.clone();
    let prefix = class
        .as_deref()
        .map_or_else(|| "cells/".to_string(), |class| format!("cells/{class}:"));

    let listed = list(&mut out, &options.bounds, |resume, want| {
        let store = &store;
        let class = class.clone();
        let prefix = prefix.clone();
        // The store resumes from a child of `cells/`, while the cursor an
        // operator sees is the scope. The two differ by the prefix, and
        // passing the scope raw silently matches nothing, which reads as a
        // cursor that works.
        let boundary = resume.boundary().map(str::to_string);
        let start_after = boundary.as_deref().map(|scope| format!("cells/{scope}"));
        let token = match resume {
            Resume::Token(token) => Some(token),
            Resume::From(_) => None,
        };
        async move {
            let page = store
                .common_prefixes_page(&prefix, start_after.as_deref(), token, want)
                .await
                .context("enumerate Durable Object instances")?;
            Ok(Page {
                rows: cell_scopes_from_prefixes(
                    page.prefixes,
                    class.as_deref(),
                    boundary.as_deref(),
                )
                .map(|scope| Cell { scope })
                .collect(),
                next: page.page_token,
            })
        }
    })
    .await?;

    out.finish()?;
    if listed.printed == 0 && !listed.abandoned {
        match options.class.as_deref() {
            Some(class) => note!("no cells of class {class}"),
            None => note!("no cells"),
        }
    } else if options.bounds.all {
        listed.report_all("cell", started.elapsed());
    } else {
        listed.report("cell", "--after");
    }
    Ok(())
}
