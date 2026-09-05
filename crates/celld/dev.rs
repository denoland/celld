// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The local application stack behind `celld dev`.
//!
//! Development still uses the standalone deployment, ownership, and LTX
//! paths. This module supplies only the infrastructure those paths require:
//! one persisted local object store and one supervised celld node.

use crate::bucket::Bucket;
use crate::{deploy, fleet};
use anyhow::{bail, Context as _};
use glob::{MatchOptions, Pattern};
use notify::{RecursiveMode, Watcher as _};
use nu_ansi_term::{Color, Style};
use sha2::{Digest as _, Sha256};
use std::io::IsTerminal as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

pub const DEFAULT_PORT: u16 = 9876;
const BUCKET: &str = "celld-dev";

/// The narrow bridge used by the supervised binary process. The public fleet
/// parser never receives this path, so no regular subcommand can select it.
#[doc(hidden)]
pub fn open_local_bucket(database: &Path) -> anyhow::Result<Bucket> {
    Bucket::open_dev(database)
}

#[derive(Debug)]
struct Options {
    project: Option<PathBuf>,
    clean: bool,
    stack: StackOptions,
}

/// The local state directory of one project.
///
/// `--clean` deletes this directory and everything below it. The path is
/// private and the only constructor takes the project directory that `run`
/// resolved, so a command-line value can never become the target of the
/// delete. A newtype carries that rule instead of a comment, because a
/// `PathBuf` field would let a later caller pass any path and lose nothing at
/// compile time.
#[derive(Debug)]
struct DevState {
    path: PathBuf,
}

impl DevState {
    /// The project-relative location of the local state, and also the suffix
    /// that `discard` requires before it deletes.
    const DIRECTORY: &'static str = ".celld/dev";

    fn for_project(project: &Path) -> Self {
        Self {
            path: project.join(Self::DIRECTORY),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// Delete the local state so the next run starts from nothing. The answer
    /// reports whether a directory was present, because the console line for
    /// an empty project must not claim that data was removed.
    ///
    /// An absent directory is the wanted result of this command, so it is not
    /// an error.
    #[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
    fn discard(&self) -> anyhow::Result<bool> {
        // The constructor already guarantees this. The check stays because it
        // is the tripwire for a future constructor that takes a path: a
        // recursive delete must never widen by accident.
        anyhow::ensure!(
            self.path.ends_with(Self::DIRECTORY),
            "refusing to discard {}, which is not a celld dev state directory",
            self.path.display()
        );
        match std::fs::remove_dir_all(&self.path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error)
                .with_context(|| format!("discard local state directory {}", self.path.display())),
        }
    }
}

#[derive(Debug)]
struct StackOptions {
    listener: SocketAddr,
    logs: bool,
    watch: bool,
    watch_ignores: Vec<Pattern>,
}

struct Store {
    database: PathBuf,
}

#[derive(Clone, Copy)]
struct Console {
    color: bool,
}

impl Console {
    fn new() -> Self {
        let force = std::env::var_os("FORCE_COLOR").is_some_and(|value| value != "0");
        let color =
            std::env::var_os("NO_COLOR").is_none() && (force || std::io::stdout().is_terminal());
        Self { color }
    }

    fn paint(&self, style: Style, value: &str) -> String {
        if self.color {
            style.paint(value).to_string()
        } else {
            value.to_string()
        }
    }

    fn header(&self) {
        crate::note!("{}", self.paint(Color::Cyan.bold(), "celld dev"));
    }

    fn detail(&self, label: &str, value: &str) {
        crate::note!(
            "  {}  {value}",
            self.paint(Style::new().dimmed(), &format!("{label:<7}"))
        );
    }

    fn progress(&self, message: &str) {
        crate::note!("  {} {message}", self.paint(Color::Yellow.normal(), "●"));
    }

    fn ready(&self, origin: &str) {
        crate::note!(
            "  {}  {}",
            self.paint(Color::Green.bold(), "ready"),
            self.paint(Color::Cyan.bold(), origin)
        );
    }

    fn log(&self, line: &str) {
        let style = if line.contains(" ERROR ") {
            Color::Red.normal()
        } else if line.contains(" WARN ") {
            Color::Yellow.normal()
        } else {
            Style::new().dimmed()
        };
        crate::note!("{}", self.paint(style, line));
    }

    fn failure(&self, message: &str) {
        crate::note!("  {} {message}", self.paint(Color::Red.bold(), "error"));
    }
}

struct ProjectWatcher {
    project: PathBuf,
    ignored: Vec<Pattern>,
    _watcher: notify::RecommendedWatcher,
    changes: mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
}

impl ProjectWatcher {
    fn new(project: &Path, ignored: Vec<Pattern>) -> anyhow::Result<Self> {
        let (sender, changes) = mpsc::unbounded_channel();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(event);
        })
        .context("create the project watcher")?;
        watcher
            .watch(project, RecursiveMode::Recursive)
            .with_context(|| format!("watch the project directory {}", project.display()))?;
        Ok(Self {
            project: project.to_path_buf(),
            ignored,
            _watcher: watcher,
            changes,
        })
    }

    #[allow(clippy::disallowed_methods)] // Host-side debounce time is outside the Actor domain.
    async fn changed(&mut self) -> anyhow::Result<()> {
        self.next_relevant_change().await?;
        loop {
            // An editor can replace one save through several writes and a
            // rename. Wait for one quiet interval so esbuild never reads the
            // temporary middle of that transaction.
            match tokio::time::timeout(Duration::from_millis(150), self.next_relevant_change())
                .await
            {
                Ok(result) => result?,
                Err(_) => return Ok(()),
            }
        }
    }

    async fn next_relevant_change(&mut self) -> anyhow::Result<()> {
        loop {
            match self
                .changes
                .recv()
                .await
                .context("the project watcher stopped")?
            {
                Ok(event) if relevant_project_event(&self.project, &event, &self.ignored) => {
                    return Ok(())
                }
                Ok(_) => {}
                Err(error) => return Err(error).context("watch the project directory"),
            }
        }
    }
}

fn relevant_project_event(project: &Path, event: &notify::Event, ignored: &[Pattern]) -> bool {
    // A read is not a change. The inotify backend reports `Access(Open(Any))`
    // for every file that the bundler and the deploy step read, so a rebuild
    // produces the events that request the next rebuild and the supervisor
    // never settles. The path filters cannot close this loop, because the
    // files that the rebuild reads are the project sources it must watch. A
    // write still arrives as a `Modify` event, and a `Create` or a `Remove`
    // event keeps its own kind, so no real change becomes invisible.
    if matches!(event.kind, notify::EventKind::Access(_)) {
        return false;
    }
    event
        .paths
        .iter()
        .any(|path| !ignored_project_path(project, path, ignored))
}

fn ignored_project_path(project: &Path, path: &Path, ignored: &[Pattern]) -> bool {
    const IGNORED_DIRECTORIES: &[&str] = &[
        ".cache",
        ".celld",
        ".git",
        ".hg",
        ".next",
        ".nuxt",
        ".parcel-cache",
        ".pnpm-store",
        ".svn",
        ".turbo",
        ".wrangler",
        ".yarn",
        "bower_components",
        "coverage",
        "node_modules",
        "target",
    ];
    let relative = path.strip_prefix(project).unwrap_or(path);
    relative
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(name) => Some(name),
            _ => None,
        })
        .any(|name| IGNORED_DIRECTORIES.iter().any(|ignored| name == *ignored))
        || relative.ancestors().any(|candidate| {
            !candidate.as_os_str().is_empty()
                && ignored.iter().any(|pattern| {
                    let options = MatchOptions {
                        case_sensitive: true,
                        require_literal_separator: true,
                        require_literal_leading_dot: true,
                    };
                    pattern.matches_path_with(candidate, options)
                        // A directory creation event arrives before its child
                        // path. Probe one child so `generated/**` also ignores
                        // the event that creates `generated` itself.
                        || pattern.matches_path_with(
                            &candidate.join("__celld_watch_descendant__"),
                            options,
                        )
                })
        })
}

#[cfg(all(test, celld_internal_tests))]
mod dev_contract {
    include!(env!("CELLD_INTERNAL_DEV_TESTS"));
}

struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn install() -> anyhow::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                interrupt: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::interrupt(),
                )?,
                terminate: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn received(&mut self) {
        #[cfg(unix)]
        crate::asyncrt::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub fn print_help() -> anyhow::Result<()> {
    crate::cli_output::Output::new(crate::cli_output::Format::Text).help(
        &format!("celld dev — run an application with persistent local storage\n\n\
USAGE:\n  celld dev [PROJECT] [--host IP] [--port PORT] [--logs] [--no-watch] [--clean]\n\n\
PROJECT is a directory or a Wrangler config. It defaults to the current\n\
directory. celld stores all local state in PROJECT/.celld/dev, and it keeps\n\
that state across a restart. A configuration change does not migrate the\n\
state, so a cell can keep a value that the new configuration rejects. Use\n\
--clean to start from an empty local state.\n\n\
OPTIONS:\n  --host IP              Worker listener host (default: 127.0.0.1)\n  --port PORT            Worker listener port (default: {DEFAULT_PORT})\n  --clean                Delete PROJECT/.celld/dev before the server starts\n  --logs                 Show the node warning and information logs\n  --no-watch             Do not rebuild when a project file changes\n  --watch-ignore PATTERN Ignore a project-relative glob; repeat as needed\n  -h, --help             Show this help"
        ),
    )
}

fn options_from_arguments(arguments: Vec<String>) -> anyhow::Result<Option<Options>> {
    let mut project = None;
    let mut host = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let mut port = DEFAULT_PORT;
    let mut clean = false;
    let mut logs = false;
    let mut watch = true;
    let mut watch_ignores = Vec::new();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--clean" => clean = true,
            "--logs" => logs = true,
            "--no-watch" => watch = false,
            "--watch-ignore" => {
                let value = arguments
                    .next()
                    .context("--watch-ignore requires a value")?;
                watch_ignores.push(
                    Pattern::new(&value)
                        .with_context(|| format!("invalid --watch-ignore pattern {value:?}"))?,
                );
            }
            "--host" => {
                let value = arguments.next().context("--host requires a value")?;
                host = value
                    .parse::<IpAddr>()
                    .with_context(|| format!("invalid --host value {value:?}"))?;
            }
            "--port" => {
                let value = arguments.next().context("--port requires a value")?;
                port = value
                    .parse::<u16>()
                    .with_context(|| format!("invalid --port value {value:?}"))?;
                if port == 0 {
                    bail!("--port must be between 1 and 65535");
                }
            }
            other if other.starts_with('-') => bail!("unknown argument for `celld dev`: {other}"),
            value if project.is_none() => project = Some(PathBuf::from(value)),
            value => bail!("celld dev accepts one PROJECT, but also received {value:?}"),
        }
    }
    if !watch && !watch_ignores.is_empty() {
        bail!("--watch-ignore cannot be used with --no-watch");
    }
    Ok(Some(Options {
        project,
        clean,
        stack: StackOptions {
            listener: SocketAddr::new(host, port),
            logs,
            watch,
            watch_ignores,
        },
    }))
}

#[allow(clippy::disallowed_methods)] // Project resolution uses the operator's host filesystem.
pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = options_from_arguments(arguments)? else {
        print_help()?;
        return Ok(());
    };
    let config = deploy::resolve_config(options.project)?;
    let config = std::fs::canonicalize(&config)
        .with_context(|| format!("resolve Wrangler config {}", config.display()))?;
    let project = config
        .parent()
        .context("the Wrangler config has no project directory")?;
    let state = DevState::for_project(project);
    let console = Console::new();
    console.header();
    console.detail("Project", &config.display().to_string());
    console.detail("State", &state.path().display().to_string());
    // The delete runs before the directory is recreated, and the console names
    // the outcome. A developer who reaches for this flag is already unsure
    // which state the run uses, so a silent delete would answer nothing.
    if options.clean {
        let discarded = state.discard()?;
        console.detail(
            "Clean",
            if discarded {
                "discarded the existing local state"
            } else {
                "no local state to discard"
            },
        );
    }
    prepare_state(state.path())?;
    if !options.stack.logs {
        console.detail("Logs", "hidden (use --logs)");
    }
    if !options.stack.watch {
        console.detail("Watch", "disabled");
    }

    let project_hash = format!(
        "{:x}",
        Sha256::digest(project.as_os_str().as_encoded_bytes())
    );
    let store = open_store(state.path(), &console).await?;
    run_stack(
        &config,
        state.path(),
        options.stack,
        &project_hash,
        &store,
        &console,
    )
    .await
}

#[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
fn prepare_state(state: &Path) -> anyhow::Result<()> {
    for directory in [state.to_path_buf(), state.join("runtime")] {
        std::fs::create_dir_all(&directory)
            .with_context(|| format!("create local state directory {}", directory.display()))?;
    }
    Ok(())
}

#[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
async fn open_store(state: &Path, console: &Console) -> anyhow::Result<Store> {
    console.progress("opening the local storage");
    let database = std::fs::canonicalize(state)
        .context("resolve the local state directory")?
        .join("objects.sqlite3");
    let bucket = open_local_bucket(&database)?;
    fleet::validate_bucket(&bucket).await?;
    bucket.probe_cas().await?;
    Ok(Store { database })
}

async fn deploy_project(config: &Path, store: &Store, logs: bool) -> anyhow::Result<()> {
    let bucket = open_local_bucket(&store.database)?;
    let built = deploy::build(&deploy::Options {
        config: Some(config.to_path_buf()),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
    })?;
    if logs {
        built.report();
    }
    deploy::write(&bucket, &built).await
}

async fn run_stack(
    config: &Path,
    state: &Path,
    options: StackOptions,
    project_hash: &str,
    store: &Store,
    console: &Console,
) -> anyhow::Result<()> {
    // Install both handlers before the child can become ready. A caller can
    // send SIGTERM as soon as the readiness request answers, and installing a
    // handler afterwards leaves the new node orphaned under the default Unix
    // signal action.
    let mut signals = ShutdownSignals::install()?;
    let project = config
        .parent()
        .context("the Wrangler config has no project directory")?;
    let mut watcher = options
        .watch
        .then(|| ProjectWatcher::new(project, options.watch_ignores))
        .transpose()?;

    console.progress("building the application");
    deploy_project(config, store, options.logs).await?;
    let mut running = start_node(
        state,
        options.listener,
        options.logs,
        project_hash,
        store,
        console,
    )
    .await?;

    loop {
        match wait_for_node_event(&mut running.child, &mut signals, watcher.as_mut()).await? {
            NodeEvent::Exited(status) => {
                let _ = running.output.await;
                if status.success() {
                    return Ok(());
                }
                bail!("the local celld node exited with {status}");
            }
            NodeEvent::Shutdown => {
                stop_running_node(running).await;
                return Ok(());
            }
            NodeEvent::Reload => {
                console.progress("change detected; rebuilding the application");
                if let Err(error) = deploy_project(config, store, options.logs).await {
                    console.failure(&format!("reload failed: {error:#}"));
                    continue;
                }
                console.progress("restarting the application");
                stop_running_node(running).await;
                running = start_node(
                    state,
                    options.listener,
                    options.logs,
                    project_hash,
                    store,
                    console,
                )
                .await?;
            }
        }
    }
}

struct RunningNode {
    child: Child,
    internal: String,
    output: tokio::task::JoinHandle<()>,
}

enum NodeEvent {
    Exited(std::process::ExitStatus),
    Reload,
    Shutdown,
}

#[allow(clippy::disallowed_methods)] // Child supervision uses real host process and time APIs.
async fn start_node(
    state: &Path,
    listener: SocketAddr,
    logs: bool,
    project_hash: &str,
    store: &Store,
    console: &Console,
) -> anyhow::Result<RunningNode> {
    let executable = std::env::current_exe().context("locate the celld executable")?;
    let listen = listener.to_string();
    let mut command = Command::new(executable);
    // The supervisor owns the child's complete storage and listener topology.
    // Inherited fleet settings can otherwise override that private topology or
    // make its guard reject a valid `celld dev` invocation.
    for variable in [
        "AWS_DEFAULT_REGION",
        "AWS_REGION",
        "CELLD_ADDR",
        "CELLD_ADVERTISE",
        "CELLD_BUCKET",
        "CELLD_CLOUD",
        "CELLD_INTERNAL_ADDR",
        "CELLD_STORAGE_PROBE",
        "CELLD_TEST_BUCKET",
        "CELLD_TRUST_FORWARDED_HEADERS",
        "CELLD_UNSAFE_PUBLIC_ADVERTISE",
        "S3_ENDPOINT",
    ] {
        command.env_remove(variable);
    }
    command
        .args([
            "--no-control-plane",
            "--bucket",
            BUCKET,
            "--listen",
            &listen,
            "--internal-listen",
            "127.0.0.1:0",
        ])
        .env("CELLD_INTERNAL_DEV_STORE", &store.database)
        .env("CELLD_WATCH", state.join("runtime"))
        .env("CELLD_NODE", format!("dev-{}", &project_hash[..12]))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // `run_stack` normally consumes `RunningNode` through the graceful stop
    // path. An error or cancellation can instead drop it, so the child must
    // not outlive the value that proves the supervisor still owns it.
    command.kill_on_drop(true);
    if !logs {
        // The supervisor still forwards ERROR records and the child's stderr.
        // INFO and WARN records stay hidden until the operator asks for them.
        command.env("RUST_LOG", "error");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        // A terminal sends Ctrl-C to its foreground process group. Keep the
        // node in a child group so only this supervisor selects the shutdown
        // mode; otherwise the node can begin a slow fleet handoff before the
        // supervisor asks for the fast same-node preserve path.
        command.as_std_mut().process_group(0);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt as _;

        let supervisor = unsafe { libc::getpid() };
        // Drop cannot run after SIGKILL or a supervisor crash. Ask the kernel
        // to kill the node in that case. The parent check closes the race in
        // which the supervisor exits between `fork` and `prctl`.
        unsafe {
            command.as_std_mut().pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != supervisor {
                    return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
                }
                Ok(())
            });
        }
    }
    let mut child = command.spawn().context("start the local celld node")?;

    let stdout = child
        .stdout
        .take()
        .expect("the local node was configured with piped output");
    let (internal_tx, internal_rx) = oneshot::channel();
    let output_console = *console;
    let output = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut internal_tx = Some(internal_tx);
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(origin) = internal_origin(&line) {
                if let Some(sender) = internal_tx.take() {
                    let _ = sender.send(origin);
                }
            }
            if logs || line.contains(" ERROR ") {
                output_console.log(&line);
            }
        }
    });
    let internal = match tokio::time::timeout(Duration::from_secs(30), internal_rx).await {
        Ok(Ok(internal)) => internal,
        result => {
            stop_child(&mut child, None).await;
            let _ = output.await;
            match result {
                Err(_) => bail!("the local node did not announce its operator listener"),
                Ok(Err(_)) => {
                    bail!("the local node exited before it announced its operator listener")
                }
                Ok(Ok(_)) => unreachable!(),
            }
        }
    };

    if let Err(error) = wait_for_node(&mut child, listener).await {
        stop_child(&mut child, Some(&internal)).await;
        let _ = output.await;
        return Err(error);
    }
    console.ready(&format!("http://{listener}"));
    Ok(RunningNode {
        child,
        internal,
        output,
    })
}

fn internal_origin(line: &str) -> Option<String> {
    let address = line
        .strip_prefix("celld internal listening on ")?
        .split_once(' ')?
        .0;
    Some(format!("http://{address}"))
}

#[allow(clippy::disallowed_methods)] // Readiness waits on a real child and wall-clock deadline.
async fn wait_for_node(child: &mut Child, listener: SocketAddr) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(200))
        .timeout(Duration::from_secs(1))
        .build()?;
    let readiness_host = match listener.ip() {
        IpAddr::V4(address) if address.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(address) if address.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        address => address,
    };
    let url = format!(
        "http://{}/.well-known/celld/health",
        SocketAddr::new(readiness_host, listener.port())
    );
    let mut last = "the node did not answer".to_string();
    for _ in 0..300 {
        if let Some(status) = child.try_wait().context("read the local node status")? {
            bail!("the local celld node exited during startup with {status}");
        }
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => last = format!("the readiness endpoint returned {}", response.status()),
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("the local celld node did not become ready: {last}")
}

async fn wait_for_node_event(
    child: &mut Child,
    signals: &mut ShutdownSignals,
    watcher: Option<&mut ProjectWatcher>,
) -> anyhow::Result<NodeEvent> {
    crate::asyncrt::select! {
        status = child.wait() => {
            let status = status.context("wait for the local celld node")?;
            Ok(NodeEvent::Exited(status))
        }
        _ = signals.received() => Ok(NodeEvent::Shutdown),
        changed = wait_for_project_change(watcher) => {
            changed?;
            Ok(NodeEvent::Reload)
        }
    }
}

async fn wait_for_project_change(watcher: Option<&mut ProjectWatcher>) -> anyhow::Result<()> {
    match watcher {
        Some(watcher) => watcher.changed().await,
        None => std::future::pending().await,
    }
}

async fn stop_running_node(mut running: RunningNode) {
    stop_child(&mut running.child, Some(&running.internal)).await;
    let _ = running.output.await;
}

#[allow(clippy::disallowed_methods)] // Shutdown deadlines govern a real child process.
async fn stop_child(child: &mut Child, internal: Option<&str>) {
    let requested = if let Some(internal) = internal {
        match reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
        {
            Ok(client) => client
                .post(format!("{internal}/shutdown?handoff=preserve"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success()),
            Err(_) => false,
        }
    } else {
        false
    };
    #[cfg(unix)]
    if !requested {
        if let Some(id) = child.id() {
            // The child installs SIGTERM as its graceful durability handoff. The
            // supervisor sends that signal instead of Child::kill, which is a
            // SIGKILL on Unix and can leave the last local transaction unflushed.
            unsafe {
                libc::kill(id as libc::pid_t, libc::SIGTERM);
            }
        }
    }
    let waited = tokio::time::timeout(Duration::from_secs(35), child.wait()).await;
    if waited.is_err() {
        let _ = child.kill().await;
    }
}
