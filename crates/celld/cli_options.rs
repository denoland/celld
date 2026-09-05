// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Operator commands read the process environment for their fleet settings.
#![allow(clippy::disallowed_methods)]

//! The flags every operator command shares.
//!
//! `--bucket`, `--endpoint` and `--region` name the same fleet whichever
//! command reads them, and four commands parsed them separately before this
//! module existed. That is how `--bucket gs://name` came to be handled in
//! one place and not another: a rule spread across four loops is a rule
//! four authors have to remember.
//!
//! The environment fallbacks live here too, so `CELLD_BUCKET`,
//! `S3_ENDPOINT`, `AWS_REGION` and `AWS_DEFAULT_REGION` mean one thing
//! across the CLI.

use anyhow::Context;

/// A non-empty environment variable, or nothing.
///
/// An empty variable is how a shell spells "unset" in a script that always
/// exports, so it must not beat a flag or a default.
fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

pub(crate) fn bucket_from_environment() -> Option<String> {
    env("CELLD_BUCKET").map(|value| value.trim().to_string())
}

/// Which fleet a command acts on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FleetFlags {
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

/// A fleet's resolved storage location: everything [`crate::fleet`] needs to
/// open a bucket.
///
/// Returned as one value because a bucket without its region opens against
/// the wrong endpoint, and separate accessors let a caller take one and
/// forget the other.
pub struct Storage {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
}

impl FleetFlags {
    /// Consume one fleet flag, or report that it is not one.
    pub fn consume(
        &mut self,
        argument: &str,
        value: &mut impl FnMut(&str) -> anyhow::Result<String>,
    ) -> anyhow::Result<bool> {
        match argument {
            // `Bucket::open` is the one parser for the complete spec. Keeping
            // the spelling intact here prevents CLI paths from disagreeing
            // about scheme case or changing where the prefix begins.
            "--bucket" => self.bucket = Some(value("--bucket")?),
            "--endpoint" => self.endpoint = Some(value("--endpoint")?),
            "--region" => self.region = Some(value("--region")?),
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Fill anything the flags left unset from the environment.
    pub fn with_environment(mut self) -> Self {
        if self.bucket.is_none() {
            self.bucket = bucket_from_environment();
        }
        if self.endpoint.is_none() {
            self.endpoint = env("S3_ENDPOINT");
        }
        if self.region.is_none() {
            self.region = env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION"));
        }
        self
    }

    /// The region a bucket opens against, whether or not one was named.
    fn region(&self) -> String {
        self.region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_string())
    }

    /// Resolve to a complete storage location, or explain what is missing.
    ///
    /// `command` names the caller so the message says which invocation to
    /// fix, rather than making an operator guess which of several commands
    /// in a script wanted a bucket.
    pub fn resolve(self, command: &str) -> anyhow::Result<Storage> {
        let resolved = self.with_environment();
        let region = resolved.region();
        let bucket = resolved
            .bucket
            .filter(|bucket| !bucket.is_empty())
            .with_context(|| {
                format!(
                    "{command} requires --bucket s3://NAME, gs://NAME or az://CONTAINER \
                     (or CELLD_BUCKET)"
                )
            })?;
        Ok(Storage {
            bucket,
            endpoint: resolved.endpoint,
            region,
        })
    }
}

impl Storage {
    /// Open the fleet bucket and prove it is reachable.
    pub async fn open(&self) -> anyhow::Result<crate::bucket::Bucket> {
        let store =
            crate::fleet::bucket_client(&self.bucket, self.endpoint.as_deref(), &self.region)?;
        crate::fleet::validate_bucket(&store).await?;
        Ok(store)
    }
}

/// The shared fleet-flag help, so every command documents them identically.
pub const FLEET_HELP: &str = "  --bucket NAME       Fleet bucket (or CELLD_BUCKET)
  --endpoint URL      Optional S3-compatible endpoint (or S3_ENDPOINT)
  --region REGION     Storage region (default: AWS_REGION or us-east-1)";

/// The shared listing help, for a command that bounds its output.
pub const LISTING_HELP: &str = "  --limit N           Maximum rows to print (default: 1000)
  --after CURSOR      Resume after this row
  --all               Print every row; this reads the whole listing
  --json              Print one JSON object per line instead of text";
