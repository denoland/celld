use nix::sys::signal::{kill, Signal};
use nix::sys::wait::waitpid;
use nix::unistd::Pid;
use std::io;
use std::io::Error;
use std::os::fd::OwnedFd;
use std::process::Command;

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
  actual_child_pid: Pid, // PID of the 'real' child process (e.g., Deno)
  death_pipe_w: Option<OwnedFd>,
  #[cfg(all(unix, not(target_os = "linux")))]
  watcher_pid: Pid,
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
          set_pdeathsig(Some(Signal::SIGTERM)).map_err(Error::other)?;
          Ok(())
        });
      }

      let child = cmd.spawn()?;
      Ok(ChildOnParentExit {
        actual_child_pid: Pid::from_raw(child.id() as i32),
        death_pipe_w: None,
      })
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
      use nix::fcntl::{fcntl, FcntlArg, FdFlag};
      use nix::unistd::{close, fork, pipe, read, write, ForkResult};

      // Pipe for parent death detection (main process -> watcher)
      let (death_pipe_r, death_pipe_w) = pipe().map_err(Error::other)?;
      // Pipe for watcher to send real child's PID to parent (watcher -> main celld process)
      let (pid_exchange_r, pid_exchange_w) = pipe().map_err(Error::other)?;

      // Set CLOEXEC on all pipe ends that will be held by ChildOnParentExit or passed around
      fcntl(&death_pipe_r, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
      fcntl(&death_pipe_w, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
      fcntl(&pid_exchange_r, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
      fcntl(&pid_exchange_w, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;

      match unsafe { fork()? } {
        ForkResult::Parent {
          child: watcher_process_pid,
        } => {
          close(death_pipe_r)?;
          close(pid_exchange_w)?;

          let mut pid_buf = [0u8; 4]; // Buffer for i32 PID
          match read(&pid_exchange_r, &mut pid_buf) {
            Ok(n) if n == pid_buf.len() => {
              // Successfully read the PID
              let real_child_pid_val = i32::from_ne_bytes(pid_buf);
              close(pid_exchange_r)?; // Close read end after use

              Ok(ChildOnParentExit {
                actual_child_pid: Pid::from_raw(real_child_pid_val),
                watcher_pid: watcher_process_pid,
                death_pipe_w: Some(death_pipe_w),
              })
            }
            Ok(_) => Err(Error::new(
              io::ErrorKind::Other,
              "Failed to read full PID from watcher",
            )),
            Err(e) => Err(Error::other(e)),
          }
        }
        ForkResult::Child => {
          // This is the watcher process
          close(death_pipe_w)?;
          close(pid_exchange_r)?;

          let mut real_child = cmd.spawn()?;
          let real_child_pid_val = real_child.id() as i32;

          // Send child pid to parent
          match write(&pid_exchange_w, &real_child_pid_val.to_ne_bytes()) {
            Ok(n) if n == 4 => {
              // Successfully wrote PID
            }
            _ => {
              eprintln!(
                "ChildOnParentExit: Watcher failed to send PID to parent"
              );
              // Handle error or partial write
              let _ = kill(Pid::from_raw(real_child_pid_val), Signal::SIGKILL);
              real_child.wait().ok();
              std::process::exit(1); // Watcher exits with error
            }
          }
          close(pid_exchange_w)?; // Close write end after sending PID

          let mut buf = [0u8; 1];
          let _ = read(&death_pipe_r, &mut buf); // Blocks until pipe is closed or error
          let _ = real_child.wait();
          std::process::exit(0);
        }
      }
    }
  }

  /// Send SIGTERM to the watcher (and thus the real child on macOS).
  pub fn kill(&self, sig: Signal) {
    let _ = kill(self.actual_child_pid, sig);
    #[cfg(all(unix, not(target_os = "linux")))]
    {
      let _ = kill(self.watcher_pid, Signal::SIGKILL);
    }
  }

  /// Get the PID of the child process
  pub fn pid(&self) -> Option<i32> {
    Some(self.actual_child_pid.as_raw())
  }

  /// Close the pipe (triggering kill on macOS) and wait for the watcher.
  #[allow(dead_code)]
  pub fn wait(mut self) {
    if let Some(w) = self.death_pipe_w.take() {
      drop(w); // Close the pipe by dropping OwnedFd
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
      let _ = waitpid(self.watcher_pid, None);
    }
    let _ = waitpid(self.actual_child_pid, None);
  }
}

impl Drop for ChildOnParentExit {
  fn drop(&mut self) {
    if let Some(w) = self.death_pipe_w.take() {
      drop(w); // Close the pipe by dropping OwnedFd
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
      let _ = waitpid(self.watcher_pid, None);
    }
    let _ = waitpid(self.actual_child_pid, None);
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
