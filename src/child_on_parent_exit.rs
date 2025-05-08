use nix::fcntl::{fcntl, FcntlArg, FdFlag};
use nix::sys::signal::{kill, Signal};
use nix::sys::wait::waitpid;
use nix::unistd::{fork, pipe, read, ForkResult, Pid};
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::io::AsRawFd;
use std::process::Command;
use tracing::info;

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

/// A child process that will be terminated if its parent dies.
///
/// On Linux this uses `prctl(PR_SET_DEATHSIG, SIGTERM)`;
/// on other Unix platforms it forks a watcher that monitors
/// the parent via a pipe and kills the real child when
/// the pipe's write end is closed.
///
/// Windows is not supported.
#[derive(Debug)]
pub struct ChildOnParentExit {
  watcher_pid: Pid,
  death_pipe_w: Option<OwnedFd>,
}

impl ChildOnParentExit {
  /// Spawn a subprocess that will receive SIGTERM if/when the parent exits.
  #[must_use = "the returned guard must be kept alive to prevent the child process from being killed prematurely"]
  pub fn spawn(mut cmd: Command) -> io::Result<Self> {
    #[cfg(target_os = "linux")]
    {
      use nix::sys::prctl::set_pdeathsig;
      cmd.before_exec(|| {
        set_pdeathsig(Some(Signal::SIGTERM))
          .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
      });

      let child = cmd.spawn()?;
      Ok(ChildOnParentExit {
        watcher_pid: Pid::from_raw(child.id() as i32),
        death_pipe_w: None,
      })
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
      // create a pipe; parent holds w, watcher holds r
      let (r, w) =
        pipe().map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
      let flags = FdFlag::FD_CLOEXEC;
      fcntl(r.as_raw_fd(), FcntlArg::F_SETFD(flags))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

      match unsafe {
        fork().map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
      } {
        ForkResult::Parent { child } => {
          // proxy parent: close read, keep write
          drop(r); // Close read end
          Ok(ChildOnParentExit {
            watcher_pid: child,
            death_pipe_w: Some(w),
          })
        }
        ForkResult::Child => {
          // watcher child: close write, spawn real child
          drop(w); // Close write end
          let mut real_child = cmd.spawn()?;

          // block until parent exits
          let mut buf = [0u8; 1];
          let _ = read(r.as_raw_fd(), &mut buf);

          // parent gone → kill real child
          let _ = kill(Pid::from_raw(real_child.id() as i32), Signal::SIGINT);
          let _ = real_child.wait();
          std::process::exit(0);
        }
      }
    }
  }

  /// Send SIGTERM to the watcher (and thus the real child on macOS).
  pub fn kill(&self, sig: Signal) {
    let _ = kill(self.watcher_pid, sig);
  }

  /// Get the PID of the child process
  pub fn pid(&self) -> Option<i32> {
    Some(self.watcher_pid.as_raw())
  }

  /// Close the pipe (triggering kill on macOS) and wait for the watcher.
  pub fn wait(mut self) {
    if let Some(w) = self.death_pipe_w.take() {
      drop(w); // Close the pipe by dropping OwnedFd
    }
    let _ = waitpid(self.watcher_pid, None);
  }
}

impl Drop for ChildOnParentExit {
  fn drop(&mut self) {
    if let Some(w) = self.death_pipe_w.take() {
      drop(w); // Close the pipe by dropping OwnedFd
    }
    let _ = waitpid(self.watcher_pid, None);
  }
}

#[cfg(all(test, unix))]
mod tests {
  use super::*;
  use std::process::Command;
  use std::time::{Duration, Instant};

  #[test]
  fn test_kill_long_running_process() {
    let mut cmd = Command::new("sleep");
    cmd.arg("60");
    let guard = ChildOnParentExit::spawn(cmd).expect("failed to spawn process");

    let start = Instant::now();
    guard.kill(Signal::SIGTERM);
    guard.wait();
    let elapsed = start.elapsed();

    assert!(
      elapsed < Duration::from_secs(5),
      "process did not exit in time: {:?}",
      elapsed
    );
  }

  #[test]
  fn test_wait_on_short_process() {
    let cmd = Command::new("true");
    let guard = ChildOnParentExit::spawn(cmd).expect("failed to spawn process");

    let start = Instant::now();
    guard.wait();
    let elapsed = start.elapsed();

    assert!(
      elapsed < Duration::from_secs(1),
      "wait took too long for quick process: {:?}",
      elapsed
    );
  }
}
