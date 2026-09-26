// Copyright 2026 Deno Land Inc. Apache-2.0 license.

#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

//! Effect adapters for the clean-sheet core.
//!
//! The executable owns one serial actor which is the only caller of
//! `celld_logic::on_event`. Adapter futures never borrow core state; they send
//! versioned completion events back through its mailbox.

// This implementation is substantially adapted from Tokio 1.53.1 select.rs.
// Copyright (c) Tokio Contributors. MIT license. See ../../LICENSE.tokio.
//
// Every `tokio::` item re-exported below is `doc(hidden)` upstream, so none of
// them carries a compatibility guarantee. Tokio can rename, reshape, or delete
// any of them in a patch release without breaking its own semver promise, and
// the `tokio = "1"` caret range in ../../Cargo.toml does not prevent that. A
// Tokio version bump must therefore re-check this facade against the select.rs
// of the new version.
//
// The failure mode is a compile error in celld, not a behavior change, so
// nothing broken ships and no test passes wrongly. Without this note the person
// who runs `cargo update` sees a macro that no longer resolves and gets no
// pointer to the cause.
//
// Rejected: narrowing the `tokio` constraint to the range this facade is known
// to work with. That blocks unrelated Tokio fixes for the whole dependency tree
// to guard against a break the compiler already makes un-missable. Do not
// "fix" this by pinning.
#[doc(hidden)]
pub mod __asyncrt_select_support {
    pub use std::future::{poll_fn, Future, IntoFuture};
    pub use std::pin::Pin;
    pub use std::task::{ready, Poll};
    pub use tokio::macros::support::poll_budget_available;
    pub use tokio::{
        count, count_field, select_priv_clean_pattern, select_priv_declare_output_enum,
        select_variant,
    };
}

#[macro_export]
macro_rules! __celld_domain_select {
    (@reason $reason:literal) => {{
        const _: () = {
            let reason: &str = $reason;
            assert!(
                !reason.is_empty(),
                "select_biased! requires a non-empty reason string"
            );
        };
    }};
    (@start fair $branches:expr) => {
        $crate::asyncrt::select_start($branches)
    };
    (@start (biased $reason:literal) $_branches:expr) => {{
        $crate::__celld_domain_select!(@reason $reason);
        0_u32
    }};
    (@ {
        mode=$mode:tt;
        ( $($count:tt)* )
        $( ( $($skip:tt)* ) $bind:pat = $future:expr, if $condition:expr => $handler:expr, )+
        ; $else:expr
    }) => {{
        #[doc(hidden)]
        mod __celld_select_util {
            $crate::__asyncrt_select_support::select_priv_declare_output_enum!(
                ( $($count)* )
            );
        }

        const BRANCHES: u32 = $crate::__asyncrt_select_support::count!($($count)*);
        let mut disabled: __celld_select_util::Mask = Default::default();

        $(
            if !$condition {
                let mask: __celld_select_util::Mask =
                    1 << $crate::__asyncrt_select_support::count!($($skip)*);
                disabled |= mask;
            }
        )*

        #[allow(unused_mut)]
        let mut output = {
            let futures_init = ($($future,)+);
            let mut futures = ($(
                $crate::__asyncrt_select_support::IntoFuture::into_future(
                    $crate::__asyncrt_select_support::count_field!(
                        futures_init.$($skip)*
                    )
                ),
            )+);
            #[allow(unused_mut)]
            let mut futures = &mut futures;

            $crate::__asyncrt_select_support::poll_fn(|context| {
                $crate::__asyncrt_select_support::ready!(
                    $crate::__asyncrt_select_support::poll_budget_available(context)
                );

                let mut is_pending = false;
                let start = $crate::__celld_domain_select!(@start $mode BRANCHES);

                for offset in 0..BRANCHES {
                    let branch;
                    #[allow(clippy::modulo_one)]
                    {
                        branch = (start + offset) % BRANCHES;
                    }
                    match branch {
                        $(
                            #[allow(unreachable_code)]
                            $crate::__asyncrt_select_support::count!($($skip)*) => {
                                let mask = 1 << branch;
                                if disabled & mask == mask {
                                    continue;
                                }

                                let ($($skip,)* future, ..) = &mut *futures;
                                // SAFETY: The tuple stays on the stack and no future moves.
                                let future = unsafe {
                                    $crate::__asyncrt_select_support::Pin::new_unchecked(future)
                                };
                                let value = match $crate::__asyncrt_select_support::Future::poll(
                                    future,
                                    context,
                                ) {
                                    $crate::__asyncrt_select_support::Poll::Ready(value) => value,
                                    $crate::__asyncrt_select_support::Poll::Pending => {
                                        is_pending = true;
                                        continue;
                                    }
                                };

                                disabled |= mask;
                                #[allow(unreachable_patterns, unused_variables, unused_mut)]
                                match &value {
                                    $crate::__asyncrt_select_support::select_priv_clean_pattern!(
                                        $bind
                                    ) => {}
                                    _ => continue,
                                }

                                return $crate::__asyncrt_select_support::Poll::Ready(
                                    $crate::__asyncrt_select_support::select_variant!(
                                        __celld_select_util::Out,
                                        ($($skip)*)
                                    )(value),
                                );
                            }
                        )*
                        _ => unreachable!("the select branch index is out of range"),
                    }
                }

                if is_pending {
                    $crate::__asyncrt_select_support::Poll::Pending
                } else {
                    $crate::__asyncrt_select_support::Poll::Ready(
                        __celld_select_util::Out::Disabled,
                    )
                }
            })
            .await
        };

        #[allow(unreachable_patterns)]
        match output {
            $(
                $crate::__asyncrt_select_support::select_variant!(
                    __celld_select_util::Out,
                    ($($skip)*) ($bind)
                ) => $handler,
            )*
            __celld_select_util::Out::Disabled => $else,
            _ => unreachable!("the select output does not match a branch"),
        }
    }};

    (@ { mode=$mode:tt; $($tokens:tt)* }) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            $($tokens)*;
            panic!("all branches are disabled and there is no else branch")
        })
    };
    (@ { mode=$mode:tt; $($tokens:tt)* } else => $else:expr $(,)?) => {
        $crate::__celld_domain_select!(@ { mode=$mode; $($tokens)*; $else })
    };
    (@ { mode=$mode:tt; ($($skip:tt)*) $($tokens:tt)* }
        $pattern:pat = $future:expr, if $condition:expr => $handler:block, $($rest:tt)*) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            ($($skip)* _)
            $($tokens)*
            ($($skip)*) $pattern = $future, if $condition => $handler,
        } $($rest)*)
    };
    (@ { mode=$mode:tt; ($($skip:tt)*) $($tokens:tt)* }
        $pattern:pat = $future:expr => $handler:block, $($rest:tt)*) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            ($($skip)* _)
            $($tokens)*
            ($($skip)*) $pattern = $future, if true => $handler,
        } $($rest)*)
    };
    (@ { mode=$mode:tt; ($($skip:tt)*) $($tokens:tt)* }
        $pattern:pat = $future:expr, if $condition:expr => $handler:block $($rest:tt)*) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            ($($skip)* _)
            $($tokens)*
            ($($skip)*) $pattern = $future, if $condition => $handler,
        } $($rest)*)
    };
    (@ { mode=$mode:tt; ($($skip:tt)*) $($tokens:tt)* }
        $pattern:pat = $future:expr => $handler:block $($rest:tt)*) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            ($($skip)* _)
            $($tokens)*
            ($($skip)*) $pattern = $future, if true => $handler,
        } $($rest)*)
    };
    (@ { mode=$mode:tt; ($($skip:tt)*) $($tokens:tt)* }
        $pattern:pat = $future:expr, if $condition:expr => $handler:expr) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            ($($skip)* _)
            $($tokens)*
            ($($skip)*) $pattern = $future, if $condition => $handler,
        })
    };
    (@ { mode=$mode:tt; ($($skip:tt)*) $($tokens:tt)* }
        $pattern:pat = $future:expr => $handler:expr) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            ($($skip)* _)
            $($tokens)*
            ($($skip)*) $pattern = $future, if true => $handler,
        })
    };
    (@ { mode=$mode:tt; ($($skip:tt)*) $($tokens:tt)* }
        $pattern:pat = $future:expr, if $condition:expr => $handler:expr, $($rest:tt)*) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            ($($skip)* _)
            $($tokens)*
            ($($skip)*) $pattern = $future, if $condition => $handler,
        } $($rest)*)
    };
    (@ { mode=$mode:tt; ($($skip:tt)*) $($tokens:tt)* }
        $pattern:pat = $future:expr => $handler:expr, $($rest:tt)*) => {
        $crate::__celld_domain_select!(@ {
            mode=$mode;
            ($($skip)* _)
            $($tokens)*
            ($($skip)*) $pattern = $future, if true => $handler,
        } $($rest)*)
    };

    (biased; $($tokens:tt)*) => {
        compile_error!(
            "select! is fair and does not accept `biased;`; use select_biased! with a reason"
        )
    };
    (else => $else:expr $(,)?) => {{ $else }};
    ($pattern:pat = $($tokens:tt)*) => {
        $crate::__celld_domain_select!(@ {
            mode=fair;
            ()
        } $pattern = $($tokens)*)
    };
    () => {
        compile_error!("select! requires at least one branch")
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __celld_domain_select_biased {
    ($reason:literal; else => $else:expr $(,)?) => {{
        $crate::__celld_domain_select!(@reason $reason);
        $else
    }};
    ($reason:literal; $pattern:pat = $($tokens:tt)*) => {{
        $crate::__celld_domain_select!(@ {
            mode=(biased $reason);
            ()
        } $pattern = $($tokens)*)
    }};
    ($($tokens:tt)*) => {
        compile_error!("select_biased! requires a non-empty reason string as its first token")
    };
}

pub mod actor;
#[cfg(all(test, celld_internal_tests))]
mod conformance_core_loop_tests {
    include!(env!("CELLD_CONFORMANCE_CORE_LOOP_TESTS"));
}
#[cfg(all(test, celld_internal_tests))]
mod conformance_facet_failure_tests {
    include!(env!("CELLD_CONFORMANCE_FACET_FAILURE_TESTS"));
}
pub mod assets;
#[cfg(not(celld_internal_tests))]
pub mod asyncrt;
// The internal test flag alone selects the simulated asyncrt, so an external
// test harness built with the flag sees the same simulated world the
// in-crate suites see. `test` must not be part of the gate:
// a dependency never has it, and the harness binary builds unflagged,
// so the shipped and harness builds keep the real asyncrt.
#[cfg(celld_internal_tests)]
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
pub mod asyncrt {
    include!(env!("CELLD_INTERNAL_ASYNCRT"));
}
#[cfg(all(test, celld_internal_tests))]
mod asyncrt_contract_tests {
    include!(env!("CELLD_INTERNAL_ASYNCRT_TESTS"));
}
pub mod bucket;
pub mod cell_cli;
mod cell_host;
pub mod cell_runtime;
pub mod clean_reload;
pub mod cli_options;
pub mod cli_output;
pub mod container;
pub mod control_plane;
pub mod d1_cli;
pub mod dead_node_gc;
pub mod deploy;
pub mod dev;
pub mod docker;
pub mod drain_token;
pub mod engine_api;
pub mod env_vars;
#[cfg(celld_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod fault {
    include!(env!("CELLD_INTERNAL_SQLITE_FAULT"));
}
pub(crate) mod facet_streams;
pub mod fleet;
pub mod generation;
pub mod host_channels;
pub mod host_services;
pub mod http_streams;
pub mod js;
pub mod kv_blob;
pub mod kv_cli;
pub(crate) mod local_store;
pub mod ltx_repl;
pub mod ltx_replication;
pub mod machine;
pub mod memory;
pub mod node_log;
pub(crate) mod operator_cell;
#[doc(hidden)]
pub mod otlp;
pub mod ownership_store;
pub mod peer_auth;
pub mod peer_probe;
pub mod pool;
pub mod protocol;
pub(crate) mod queue_batching;
pub mod queue_cli;
pub mod queue_policy;
pub mod r2_cli;
pub(crate) mod r2_store;
pub mod replication;
pub mod runtime;
pub mod startup;
pub mod storage;
pub mod telemetry;
pub mod wake;
pub mod wake_entry;
pub mod wake_format;
pub mod ws_client;
pub mod ws_registry;

#[cfg(all(test, celld_internal_tests))]
mod composed_simulation {
    include!(env!("CELLD_INTERNAL_COMPOSED_SIMULATION"));
}

#[cfg(all(test, celld_internal_tests))]
mod simulation_reproduction {
    include!(env!("CELLD_INTERNAL_SIMULATION_REPRODUCTION"));
}

#[cfg(all(test, celld_internal_tests))]
mod token_lifecycle_tests {
    include!(env!("CELLD_INTERNAL_TOKEN_PROBE"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_world_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s2_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S2_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_o3_oracle {
    include!(env!("CELLD_CONFORMANCE_O3_ORACLE_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5a_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S5A_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5c_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S5C_TESTS"));
}

#[cfg(celld_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod conformance_sim_store {
    include!(env!("CELLD_CONFORMANCE_SIM_STORE_TESTS"));
}

#[cfg(celld_internal_tests)]
#[allow(clippy::disallowed_methods)]
#[doc(hidden)]
pub mod conformance_sim_cell_host {
    include!(env!("CELLD_CONFORMANCE_SIM_CELL_HOST_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_world_s1_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S1_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_world_coverage {
    include!(env!("CELLD_CONFORMANCE_WORLD_COVERAGE_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s3_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S3_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5b_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S5B_TESTS"));
}

/// Completion token for a resident-isolate reservation made by the decision
/// core. Dropping the queued/running job reports that the cell is idle again;
/// the token contains no selection or lifecycle policy of its own.
pub struct CellActivityGuard {
    finish: Option<Box<dyn FnOnce() + Send>>,
}

impl CellActivityGuard {
    pub fn new(finish: impl FnOnce() + Send + 'static) -> Self {
        Self {
            finish: Some(Box::new(finish)),
        }
    }
}

impl Drop for CellActivityGuard {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            finish();
        }
    }
}

/// A stateless entrypoint selected for one fetch request.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerFetchEntrypoint {
    pub name: String,
    /// The caller's `ctx.props` as V8 structured-clone bytes. An empty vector
    /// represents an omitted or `undefined` props value.
    pub props: Vec<u8>,
}

/// Resource limits selected by a Worker Loader stub for one invocation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInvocationLimits {
    pub cpu_ms: Option<u32>,
    pub sub_requests: Option<u32>,
}

/// One operation on a Worker entrypoint. A property read has no arguments,
/// while a call owns its complete receiver path and structured-clone payload.
pub enum WorkerRpcOperation {
    Get { path: Vec<String> },
    Call { path: Vec<String>, args: Vec<u8> },
}

pub enum WorkerJob {
    Fetch {
        queued_at: std::time::Instant,
        /// An entrypoint dispatcher target, or the direct default export when
        /// absent.
        entrypoint: Option<WorkerFetchEntrypoint>,
        invocation_limits: Option<WorkerInvocationLimits>,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        request_id: Option<js::RequestId>,
        /// Receives one completed invocation report for Dynamic Worker tails.
        /// Ordinary fetches leave this empty.
        tail_report: Option<tokio::sync::oneshot::Sender<String>>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<js::HttpResponse>>,
    },
    Rpc {
        entrypoint: String,
        operation: WorkerRpcOperation,
        /// The caller's `ctx.props` as V8 structured-clone bytes, the same
        /// encoding as `args`. It is empty when the caller sent none, which no
        /// encoded value can be. A Worker Loader entrypoint or a transferred
        /// Service Binding can set it.
        props: Vec<u8>,
        invocation_limits: Option<WorkerInvocationLimits>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<Vec<u8>>>,
    },
    Queue {
        queued_at: std::time::Instant,
        batch: js::QueueBatch,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<js::QueueDispatchResult>>,
    },
}

/// Temporary host seam required by the verbatim JS adapter. The runtime
/// adapter will construct the real shared Worker queue; lifecycle policy does
/// not move into this type.
/// Compatibility switches copied with the JS adapter. These are runtime
/// semantics, not lifecycle decisions.
pub fn worker_compat(metadata: &serde_json::Value) -> js::Compat {
    let flags = metadata
        .get("compatibility_flags")
        .and_then(serde_json::Value::as_array);
    let has_flag = |name: &str| {
        flags.is_some_and(|flags| {
            flags
                .iter()
                .any(|flag| flag.as_str().is_some_and(|flag| flag == name))
        })
    };
    let date = metadata
        .get("compatibility_date")
        .and_then(serde_json::Value::as_str);
    let switch = |enable: &str, disable: &str, since: &str| {
        if has_flag(enable) {
            return true;
        }
        if has_flag(disable) {
            return false;
        }
        date.is_some_and(|date| date >= since)
    };
    js::Compat {
        delete_all_deletes_alarm: switch(
            "delete_all_deletes_alarm",
            "delete_all_preserves_alarm",
            "2026-02-24",
        ),
        js_rpc: has_flag("js_rpc"),
        fetcher_get_put_delete: !switch(
            "fetcher_no_get_put_delete",
            "fetcher_has_get_put_delete",
            "2024-03-26",
        ),
        sqlite_vec: has_flag("sqlite_vec"),
        websocket_standard_binary_type: has_flag("websocket_standard_binary_type"),
        queue_json_messages: switch("queue_json_messages", "queue_v8_messages", "2024-03-18"),
    }
}
