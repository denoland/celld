// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Process services that an engine incarnation can observe.
//!
//! Production installs one service set. A deterministic domain installs one
//! set per simulated node, so co-hosted incarnations do not share counters or
//! host measurements.

use crate::ownership_store::LiveLoad;
use std::sync::{Arc, Mutex, OnceLock};

/// One resource observation used by the pressure path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HostMetricsSample {
    pub cpu_percent_x100: u64,
    pub rss_bytes: u64,
    pub in_use_bytes: u64,
    pub cgroup_working_set_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
}

enum MetricsBackend {
    Production(Mutex<ProcessLoadSampler>),
    #[cfg(celld_internal_tests)]
    Scripted(Mutex<HostMetricsSample>),
}

/// All process-like services owned by one execution domain.
pub struct HostServices {
    domain: OnceLock<crate::asyncrt::DomainToken>,
    node_load: OnceLock<Arc<LiveLoad>>,
    wake_entry: crate::js::WakeEntryService,
    websockets: crate::js::WebSocketService,
    http_streams: Arc<crate::js::HttpStreamService>,
    metrics: MetricsBackend,
}

impl HostServices {
    pub(crate) fn production() -> Self {
        Self {
            domain: OnceLock::new(),
            node_load: OnceLock::new(),
            wake_entry: crate::js::WakeEntryService::default(),
            websockets: crate::js::WebSocketService::default(),
            http_streams: Arc::new(crate::js::HttpStreamService::default()),
            metrics: MetricsBackend::Production(Mutex::new(ProcessLoadSampler::default())),
        }
    }

    #[cfg(celld_internal_tests)]
    pub fn scripted() -> Self {
        Self {
            domain: OnceLock::new(),
            node_load: OnceLock::new(),
            wake_entry: crate::js::WakeEntryService::default(),
            websockets: crate::js::WebSocketService::default(),
            http_streams: Arc::new(crate::js::HttpStreamService::default()),
            metrics: MetricsBackend::Scripted(Mutex::new(HostMetricsSample::default())),
        }
    }

    pub fn set_node_load(&self, load: Arc<LiveLoad>) {
        let _ = self.node_load.set(load);
    }

    pub(crate) fn bind_domain(&self, domain: crate::asyncrt::DomainToken) {
        if let Err(candidate) = self.domain.set(domain) {
            assert!(
                self.domain
                    .get()
                    .is_some_and(|bound| bound.same_owner(&candidate)),
                "HostServices cannot belong to more than one execution domain"
            );
        }
        self.http_streams.bind_domain(
            self.domain
                .get()
                .expect("a HostServices domain was just installed")
                .clone(),
        );
    }

    pub(crate) fn domain_token(&self) -> Option<crate::asyncrt::DomainToken> {
        self.domain.get().cloned()
    }

    pub fn node_load(&self) -> Option<Arc<LiveLoad>> {
        self.node_load.get().cloned()
    }

    pub(crate) fn wake_entry(&self) -> &crate::js::WakeEntryService {
        &self.wake_entry
    }

    pub(crate) fn websockets(&self) -> &crate::js::WebSocketService {
        &self.websockets
    }

    pub(crate) fn http_streams(&self) -> Arc<crate::js::HttpStreamService> {
        if let Some(domain) = self.domain_token() {
            self.http_streams.bind_domain(domain);
        }
        self.http_streams.clone()
    }

    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn quarantine_http_streams(&self) {
        self.http_streams.quarantine();
    }

    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn close_http_streams(&self) {
        self.http_streams.close();
    }

    pub fn sample_metrics(&self) -> HostMetricsSample {
        match &self.metrics {
            MetricsBackend::Production(sampler) => {
                let mut sampler = sampler.lock().unwrap();
                let memory = crate::memory::sample();
                HostMetricsSample {
                    cpu_percent_x100: sampler.sample_cpu_percent_x100(),
                    rss_bytes: memory.rss_bytes,
                    in_use_bytes: memory.in_use_bytes,
                    cgroup_working_set_bytes: memory.cgroup_working_set_bytes,
                    cgroup_current_bytes: memory.cgroup_current_bytes,
                }
            }
            #[cfg(celld_internal_tests)]
            MetricsBackend::Scripted(sample) => *sample.lock().unwrap(),
        }
    }

    #[cfg(celld_internal_tests)]
    pub fn set_scripted_metrics(&self, sample: HostMetricsSample) {
        let MetricsBackend::Scripted(current) = &self.metrics else {
            panic!("scripted metrics require a simulation HostServices instance");
        };
        *current.lock().unwrap() = sample;
    }
}

#[derive(Default)]
struct ProcessLoadSampler {
    previous_cpu_ticks: Option<u64>,
    previous_sample: Option<std::time::Instant>,
}

impl ProcessLoadSampler {
    // This is the production metrics arm. A simulation HostServices returns a
    // scripted sample and never calls this host-clock sampler.
    #[allow(clippy::disallowed_methods)]
    fn sample_cpu_percent_x100(&mut self) -> u64 {
        let Some(ticks) = process_cpu_ticks() else {
            return 0;
        };
        let now = std::time::Instant::now();
        let value = match (self.previous_cpu_ticks, self.previous_sample) {
            (Some(previous_ticks), Some(previous_sample)) => {
                let elapsed = previous_sample.elapsed().as_secs_f64();
                let ticks_per_second = clock_ticks_per_second() as f64;
                if elapsed > 0.0 && ticks_per_second > 0.0 {
                    (((ticks.saturating_sub(previous_ticks)) as f64 / ticks_per_second / elapsed)
                        * 10_000.0) as u64
                } else {
                    0
                }
            }
            _ => 0,
        };
        self.previous_cpu_ticks = Some(ticks);
        self.previous_sample = Some(now);
        value
    }
}

#[cfg(target_os = "linux")]
#[allow(clippy::disallowed_methods)] // `/proc` is host telemetry, not node storage.
fn process_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat
        .get(stat.rfind(')')? + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ticks() -> Option<u64> {
    None
}

#[cfg(unix)]
fn clock_ticks_per_second() -> u64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u64::try_from(ticks)
        .ok()
        .filter(|ticks| *ticks > 0)
        .unwrap_or(100)
}

#[cfg(not(unix))]
fn clock_ticks_per_second() -> u64 {
    100
}
