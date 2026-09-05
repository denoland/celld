// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This module owns the ambient production primitives which the boundary lint
// prohibits elsewhere.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

//! The production execution facade for celld.
//!
//! This module delegates tasks and timers to Tokio and obtains nondeterministic
//! process values from the host. A cfg-gated build can replace this module
//! with another execution backend.

use crate::host_services::HostServices;
use rand::{CryptoRng, RngCore};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static PROCESS_DOMAIN: OnceLock<ProductionDomain> = OnceLock::new();
static FALLBACK_SERVICES: OnceLock<Arc<HostServices>> = OnceLock::new();
static RUNTIME_SERVICES: OnceLock<Mutex<HashMap<tokio::runtime::Id, Weak<HostServices>>>> =
    OnceLock::new();
static SELECT_STATE: OnceLock<AtomicU64> = OnceLock::new();

thread_local! {
    static SPAWNS: RefCell<Vec<(u64, OpFuture, OpLifetime)>> = const { RefCell::new(Vec::new()) };
}

struct ProductionDomain {
    owner: Arc<ProductionDomainOwner>,
    services: Arc<HostServices>,
    filesystem: Arc<dyn celld_ltx::FileSystem>,
    next_core_request: AtomicU64,
    next_async_op: AtomicU64,
    process_tag: u64,
}

impl ProductionDomain {
    fn new(handle: tokio::runtime::Handle) -> Self {
        let owner = Arc::new(ProductionDomainOwner::new(handle));
        let token = DomainToken::new(&owner);
        let services = Arc::new(HostServices::production());
        services.bind_domain(token.clone());
        Self {
            owner,
            services,
            filesystem: Arc::new(celld_ltx::DirectFileSystem),
            next_core_request: AtomicU64::new(1),
            next_async_op: AtomicU64::new(1),
            process_tag: u64::from(std::process::id()),
        }
    }
}

fn current_domain() -> &'static ProductionDomain {
    PROCESS_DOMAIN.get_or_init(|| ProductionDomain::new(tokio::runtime::Handle::current()))
}

struct ProductionDomainOwner {
    handle: tokio::runtime::Handle,
    started_at: Instant,
}

impl ProductionDomainOwner {
    fn new(handle: tokio::runtime::Handle) -> Self {
        Self {
            handle,
            started_at: Instant::now(),
        }
    }
}

/// A cloneable capability for scheduling work on one execution domain.
#[derive(Clone)]
pub(crate) struct DomainToken {
    inner: Weak<ProductionDomainOwner>,
}

/// The execution domain no longer accepts work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DomainClosed;

impl fmt::Display for DomainClosed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("execution domain is closed")
    }
}

impl std::error::Error for DomainClosed {}

impl DomainToken {
    fn new(owner: &Arc<ProductionDomainOwner>) -> Self {
        Self {
            inner: Arc::downgrade(owner),
        }
    }

    fn owner(&self) -> Result<Arc<ProductionDomainOwner>, DomainClosed> {
        self.inner.upgrade().ok_or(DomainClosed)
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn mono_ms(&self) -> Result<u64, DomainClosed> {
        Ok(self.owner()?.started_at.elapsed().as_millis() as u64)
    }

    pub(crate) fn sleep_until(&self, deadline_ms: u64) -> Result<Sleep, DomainClosed> {
        let owner = self.owner()?;
        let now_ms = owner.started_at.elapsed().as_millis() as u64;
        let wait = Duration::from_millis(deadline_ms.saturating_sub(now_ms));
        // Tokio binds a sleep to the current timer driver when it constructs
        // the future. Enter this token's runtime so an ambient runtime cannot
        // capture a timer that belongs to another execution domain.
        let _entered = owner.handle.enter();
        Ok(Box::pin(tokio::time::sleep(wait)))
    }

    #[track_caller]
    pub(crate) fn spawn_detached<T, F>(
        &self,
        _root: impl Into<String>,
        future: F,
    ) -> Result<TaskHandle<T>, DomainClosed>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let owner = self.owner()?;
        Ok(TaskHandle {
            inner: owner.handle.spawn(future),
        })
    }
}

/// A normalized task panic returned by a typed task handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskPanic {
    pub message: String,
}

impl fmt::Display for TaskPanic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TaskPanic {}

/// A typed join handle for one production task.
pub struct TaskHandle<T> {
    inner: tokio::task::JoinHandle<T>,
}

impl<T> Unpin for TaskHandle<T> {}

impl<T> TaskHandle<T> {
    /// Detach the task from its join handle.
    pub fn detach(self) {}
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, TaskPanic>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(context)
            .map(|result| {
                result.map_err(|error| TaskPanic {
                    message: error.to_string(),
                })
            })
    }
}

/// Install the process runtime handle.
pub fn set_host_handle(handle: tokio::runtime::Handle) {
    let _ = PROCESS_DOMAIN.set(ProductionDomain::new(handle));
}

/// Return the Tokio handle for the V8 arm.
pub fn op_handle() -> tokio::runtime::Handle {
    current_domain().owner.handle.clone()
}

pub fn spawn<T, F>(future: F) -> TaskHandle<T>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    TaskHandle {
        inner: current_domain().owner.handle.spawn(future),
    }
}

pub fn blocking<T, F>(operation: F) -> TaskHandle<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    TaskHandle {
        inner: current_domain().owner.handle.spawn_blocking(operation),
    }
}

pub fn block_on<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    current_domain().owner.handle.block_on(future)
}

/// Return the local filesystem for the process.
pub fn fs() -> Arc<dyn celld_ltx::FileSystem> {
    current_domain().filesystem.clone()
}

/// Return the maximum component length for the filesystem at `path`.
pub fn filesystem_name_max(path: &std::path::Path) -> std::io::Result<Option<i64>> {
    let directory = std::fs::File::open(path)?;
    nix::unistd::fpathconf(&directory, nix::unistd::PathconfVar::NAME_MAX).map_err(Into::into)
}

pub fn wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn mono_ms() -> u64 {
    current_domain().owner.started_at.elapsed().as_millis() as u64
}

/// Return monotonic process time in microseconds for timing instrumentation.
pub fn mono_us() -> u64 {
    current_domain().owner.started_at.elapsed().as_micros() as u64
}

pub fn rng(consumer: &'static str) -> rng::Stream {
    let _ = consumer;
    rng::Stream::production()
}

fn mix_select(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Return the first branch index for one unbiased select poll.
pub fn select_start(branches: u32) -> u32 {
    debug_assert!(branches > 0);
    // A select can run before the process installs its Tokio handle. Keep its
    // process-wide state separate, so branch ordering cannot capture a
    // short-lived runtime that another operation later tries to use.
    let state = SELECT_STATE
        .get_or_init(|| AtomicU64::new(rand::rngs::OsRng.next_u64()))
        .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    (mix_select(state) % u64::from(branches)) as u32
}

pub fn process_tag() -> u64 {
    current_domain().process_tag
}

pub fn next_core_request() -> u64 {
    current_domain()
        .next_core_request
        .fetch_add(1, Ordering::Relaxed)
}

pub fn services() -> Arc<HostServices> {
    PROCESS_DOMAIN
        .get()
        .map(|domain| domain.services.clone())
        .unwrap_or_else(|| {
            FALLBACK_SERVICES
                .get_or_init(|| Arc::new(HostServices::production()))
                .clone()
        })
}

/// Return services bound to the current runtime without installing a process domain.
pub(crate) fn runtime_services() -> Arc<HostServices> {
    if let Some(domain) = PROCESS_DOMAIN.get() {
        return domain.services.clone();
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return services();
    };
    let mut runtimes = RUNTIME_SERVICES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    // The map owns each runtime ID even after its weak service expires. A
    // caller can also retain a service after its runtime anchor closes, which
    // leaves a live Arc with a dead owner token. Prune both states before an ID
    // can resolve to services from an earlier runtime.
    runtimes.retain(|_, services| {
        services.upgrade().is_some_and(|services| {
            services
                .domain_token()
                .is_some_and(|domain| domain.mono_ms().is_ok())
        })
    });
    if let Some(services) = runtimes.get(&handle.id()).and_then(Weak::upgrade) {
        return services;
    }
    let owner = Arc::new(ProductionDomainOwner::new(handle.clone()));
    let token = DomainToken::new(&owner);
    let services = Arc::new(HostServices::production());
    services.bind_domain(token);
    let anchor_services = services.clone();
    let anchor_owner = owner;
    handle.spawn(async move {
        std::future::pending::<()>().await;
        drop((anchor_services, anchor_owner));
    });
    runtimes.insert(handle.id(), Arc::downgrade(&services));
    services
}

pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

pub fn sleep_until(deadline_ms: u64) -> Sleep {
    let wait = Duration::from_millis(deadline_ms.saturating_sub(mono_ms()));
    Box::pin(tokio::time::sleep(wait))
}

pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(mono_ms().saturating_add(duration.as_millis() as u64))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}

pub async fn timeout_at<F>(deadline_ms: u64, future: F) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut deadline = sleep_until(deadline_ms);
    poll_fn(move |context| {
        if let Poll::Ready(output) = future.as_mut().poll(context) {
            return Poll::Ready(Ok(output));
        }
        if mono_ms() >= deadline_ms {
            return Poll::Ready(Err(Elapsed));
        }
        if deadline.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(Elapsed));
        }
        Poll::Pending
    })
    .await
}

pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    timeout_at(
        mono_ms().saturating_add(duration.as_millis() as u64),
        future,
    )
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissedTickBehavior {
    Burst,
    Delay,
    Skip,
}

pub struct Interval {
    next_ms: u64,
    period_ms: u64,
    behavior: MissedTickBehavior,
}

impl Interval {
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }

    pub async fn tick(&mut self) {
        sleep_until(self.next_ms).await;
        let now = mono_ms();
        self.next_ms = match self.behavior {
            MissedTickBehavior::Burst => self.next_ms.saturating_add(self.period_ms),
            MissedTickBehavior::Delay => now.saturating_add(self.period_ms),
            MissedTickBehavior::Skip => {
                let missed = now.saturating_sub(self.next_ms) / self.period_ms.max(1);
                self.next_ms
                    .saturating_add(missed.saturating_add(1).saturating_mul(self.period_ms))
            }
        };
    }
}

pub fn interval(period: Duration) -> Interval {
    Interval {
        next_ms: mono_ms(),
        period_ms: period.as_millis().max(1) as u64,
        behavior: MissedTickBehavior::Burst,
    }
}

pub fn interval_at(start_ms: u64, period: Duration) -> Interval {
    Interval {
        next_ms: start_ms,
        period_ms: period.as_millis().max(1) as u64,
        behavior: MissedTickBehavior::Burst,
    }
}

/// The output of one asynchronous JavaScript operation.
pub enum OpOut {
    Str(String),
    Bytes(Vec<u8>),
}

impl From<String> for OpOut {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<Vec<u8>> for OpOut {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

pub type OpFuture = Pin<Box<dyn Future<Output = Result<OpOut, String>> + Send>>;

#[derive(Clone, Copy)]
enum OpLifetime {
    Handler,
    IoContext,
}

fn enqueue_with_lifetime<T: Into<OpOut>>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
    lifetime: OpLifetime,
) -> u64 {
    let id = current_domain()
        .next_async_op
        .fetch_add(1, Ordering::Relaxed);
    let future: OpFuture = Box::pin(async move { future.await.map(Into::into) });
    SPAWNS.with(|spawns| spawns.borrow_mut().push((id, future, lifetime)));
    id
}

/// Register an asynchronous operation. The request driver polls the operation.
pub fn enqueue<T: Into<OpOut>>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> u64 {
    enqueue_with_lifetime(future, OpLifetime::Handler)
}

/// Register an operation that keeps its `IoContext` alive after the handler ends.
pub(crate) fn enqueue_io_context<T: Into<OpOut>>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> u64 {
    enqueue_with_lifetime(future, OpLifetime::IoContext)
}

pub fn drain_spawns() -> Vec<(u64, OpFuture, bool)> {
    SPAWNS.with(|spawns| {
        spawns
            .borrow_mut()
            .drain(..)
            .map(|(id, future, lifetime)| (id, future, matches!(lifetime, OpLifetime::IoContext)))
            .collect()
    })
}

pub use crate::__celld_domain_select as select;
pub use crate::__celld_domain_select_biased as select_biased;

pub mod rng {
    use super::*;

    /// A random stream for one production consumer.
    pub struct Stream {
        _private: (),
    }

    impl Stream {
        pub(super) fn production() -> Self {
            Self { _private: () }
        }
    }

    impl RngCore for Stream {
        fn next_u32(&mut self) -> u32 {
            rand::rngs::OsRng.next_u32()
        }

        fn next_u64(&mut self) -> u64 {
            rand::rngs::OsRng.next_u64()
        }

        fn fill_bytes(&mut self, destination: &mut [u8]) {
            rand::rngs::OsRng.fill_bytes(destination);
        }

        fn try_fill_bytes(&mut self, destination: &mut [u8]) -> Result<(), rand::Error> {
            self.fill_bytes(destination);
            Ok(())
        }
    }

    impl CryptoRng for Stream {}
}
