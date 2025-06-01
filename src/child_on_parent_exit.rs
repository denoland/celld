use nix::sys::signal::{kill, Signal};
use nix::sys::wait::waitpid;
use nix::sys::wait::WaitPidFlag;
use nix::sys::wait::WaitStatus;
use nix::unistd::write;
use nix::unistd::Pid;
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::process::ExitStatusExt;
use std::process::Command;
use std::process::ExitStatus;

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
  reaped_status: Option<(Pid, i32)>,
}

impl ChildOnParentExit {
  /// Spawn a subprocess that will receive SIGTERM if/when the parent exits.
  #[must_use = "the returned guard must be kept alive to prevent the child process from being killed prematurely"]
  pub fn spawn(mut cmd: Command) -> io::Result<Self> {
    #[cfg(target_os = "linux")]
    {
      use nix::sys::prctl::set_pdeathsig;
      unsafe {
        cmd.pre_exec(|| {
          set_pdeathsig(Some(Signal::SIGTERM)).map_err(io::Error::other)?;
          Ok(())
        });
      }

      let child = cmd.spawn()?;
      Ok(ChildOnParentExit {
        watcher_pid: Pid::from_raw(child.id() as i32),
        death_pipe_w: None,
        reaped_status: None,
      })
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
      use nix::fcntl::{fcntl, FcntlArg, FdFlag};
      use nix::sys::signal::{sigaction, SaFlags, SigAction, SigHandler};
      use nix::unistd::{fork, pipe, read, ForkResult};
      use std::sync::atomic::{AtomicBool, Ordering};

      // Global atomic flag to track if child has terminated
      static CHILD_TERMINATED_FLAG: AtomicBool = AtomicBool::new(false);

      // SIGCHLD signal handler function
      extern "C" fn handle_sigchld(_: i32) {
        CHILD_TERMINATED_FLAG.store(true, Ordering::SeqCst);
      }

      // create a pipe; parent holds w, watcher holds r
      let (r, w) = pipe().map_err(io::Error::other)?;
      let flags = FdFlag::FD_CLOEXEC;
      fcntl(&r, FcntlArg::F_SETFD(flags)).map_err(io::Error::other)?;

      match unsafe { fork().map_err(io::Error::other)? } {
        ForkResult::Parent { child } => {
          // proxy parent: close read, keep write
          drop(r); // Close read end
          Ok(ChildOnParentExit {
            watcher_pid: child,
            death_pipe_w: Some(w),
            reaped_status: None,
          })
        }
        ForkResult::Child => {
          // watcher child: close write, spawn real child
          drop(w); // Close write end

          // Set up SIGCHLD handler before spawning real child
          let sa = SigAction::new(
            SigHandler::Handler(handle_sigchld),
            SaFlags::SA_NOCLDSTOP,
            nix::sys::signal::SigSet::empty(),
          );
          unsafe {
            sigaction(Signal::SIGCHLD, &sa)
              .expect("Failed to set SIGCHLD handler");
          }

          let mut real_child = cmd.spawn()?;
          let real_child_pid = Pid::from_raw(real_child.id() as i32);

          // Main read loop with EINTR handling
          loop {
            // Reset flag before reading
            CHILD_TERMINATED_FLAG.store(false, Ordering::SeqCst);

            let mut buf = [0u8; 1]; // One byte for signal
            let result = read(&r, &mut buf);

            match result {
              Ok(0) => {
                // EOF - parent closed pipe
                let _ = kill(real_child_pid, Signal::SIGTERM);
                let status = real_child.wait().unwrap();
                let exit_code = status.code().unwrap_or(1);
                std::process::exit(exit_code);
              }
              Ok(1) => {
                // Got signal byte from parent
                let signal =
                  Signal::try_from(buf[0] as i32).unwrap_or(Signal::SIGTERM);
                let _ = kill(real_child_pid, signal);
                let status = real_child.wait().unwrap();
                let exit_code = status.code().unwrap_or(1);
                std::process::exit(exit_code);
              }
              Ok(_) => unreachable!("Read more than one byte from pipe"),
              Err(nix::Error::EINTR) => {
                // Interrupted by signal - check if child terminated
                if CHILD_TERMINATED_FLAG.load(Ordering::SeqCst) {
                  CHILD_TERMINATED_FLAG.store(false, Ordering::SeqCst);
                  match real_child.try_wait() {
                    Ok(Some(status)) => {
                      // Child has terminated
                      let exit_code = status.code().unwrap_or(1);
                      // TODO: Pass exit status to watcher's parent via new pipe
                      std::process::exit(exit_code);
                    }
                    Ok(None) => {
                      // Child still running, continue loop
                      continue;
                    }
                    Err(_) => {
                      // Error checking child status, kill and exit
                      let _ = kill(real_child_pid, Signal::SIGTERM);
                      let _ = real_child.wait();
                      std::process::exit(1);
                    }
                  }
                } else {
                  // Some other signal interrupted us, continue
                  continue;
                }
              }
              Err(_e) => {
                // Fatal error with pipe
                let _ = kill(real_child_pid, Signal::SIGTERM);
                let status = real_child.wait().unwrap();
                let exit_code = status.code().unwrap_or(1);
                std::process::exit(exit_code);
              }
            }
          }
        }
      }
    }
  }

  /// Send signal to the child process by writing it to the death pipe.
  pub fn kill(&self, sig: Signal) {
    if let Some(ref w) = self.death_pipe_w {
      // Write signal as a single byte to the pipe
      let sig_val = sig as u8;
      let r = write(w, &[sig_val]).unwrap();
      assert_eq!(r, 1, "Failed to write signal to pipe");
    } else {
      // On Linux, death_pipe_w is None, so directly kill the process
      let _ = kill(self.watcher_pid, sig);
    }
  }

  /// Get the PID of the child process
  pub fn pid(&self) -> Option<i32> {
    Some(self.watcher_pid.as_raw())
  }

  /// Close the pipe (triggering kill on macOS) and wait for the watcher.
  pub fn wait(mut self) {
    if self.reaped_status.is_some() {
      assert!(self.death_pipe_w.is_none(), "Pipe should be closed on drop");
      return;
    }

    if let Some(w) = self.death_pipe_w.take() {
      drop(w); // Close the pipe by dropping OwnedFd
    }
    let _ = waitpid(self.watcher_pid, None);
  }

  pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
    if let Some((_pid, exit_code)) = self.reaped_status {
      return Ok(Some(ExitStatus::from_raw(exit_code)));
    }

    match waitpid(self.watcher_pid, Some(WaitPidFlag::WNOHANG)) {
      Ok(status) => match status {
        WaitStatus::StillAlive => {
          Ok(None) // Not exited yet
        }
        WaitStatus::Signaled(_, _, _) => {
          todo!()
        }
        WaitStatus::Exited(pid, exit_code) => {
          // Watcher (or child on Linux) has terminated. Store status.
          self.reaped_status = Some((pid, exit_code));
          // If the watcher exited, its job regarding the pipe is done.
          // Close our end of the pipe.
          if self.death_pipe_w.is_some() {
            self.death_pipe_w.take();
          }
          Ok(Some(ExitStatus::from_raw(exit_code)))
        }
        // Other statuses (Stopped, Continued, PtraceEvent, etc.) mean the process
        // is still alive in some form. For try_wait's purpose of checking termination,
        // these are treated as "not exited yet".
        _ => Ok(None),
      },
      Err(nix_err) => {
        // ECHILD means the PID is unknown or already reaped by someone else.
        // If self.reaped_status is None, this is an unexpected state from our perspective.
        Err(io::Error::from_raw_os_error(nix_err as i32))
      }
    }
  }
}

impl Drop for ChildOnParentExit {
  fn drop(&mut self) {
    if self.reaped_status.is_some() {
      assert!(self.death_pipe_w.is_none(), "Pipe should be closed on drop");
      return;
    }

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

  #[test]
  fn test_try_wait_on_running_process() {
    let mut cmd = Command::new("sleep");
    cmd.arg("1");
    let mut guard =
      ChildOnParentExit::spawn(cmd).expect("failed to spawn process");

    let status = guard.try_wait().expect("try_wait failed");
    assert!(status.is_none(), "Process should be running");

    std::thread::sleep(Duration::from_millis(1500)); // Wait for sleep 1 to finish

    let status_after_exit = guard.try_wait().expect("try_wait failed").unwrap();
    assert_eq!(0, status_after_exit.code().unwrap());

    // Call wait to consume the guard and ensure cleanup (though try_wait reaped it)
    guard.wait();
  }
}
