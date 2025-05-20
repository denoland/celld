use std::{
  io::{BufRead as _, BufReader},
  process::{Child, Command},
  sync::{Arc, Mutex},
  thread::JoinHandle,
};

/// A wrapper around [`Child`] that captures its stdout and stderr and emits them when the test fails.
pub struct CapturedSubprocess {
  program: String,
  child: Child,
  captured_stdout: Arc<Mutex<String>>,
  captured_stderr: Arc<Mutex<String>>,
  stdout_relay_handle: Option<JoinHandle<()>>,
  stderr_relay_handle: Option<JoinHandle<()>>,
}

impl CapturedSubprocess {
  pub fn new(mut cmd: Command, setup_cmd: impl FnOnce(&mut Command)) -> Self {
    let program = cmd.get_program().to_string_lossy().to_string();

    setup_cmd(&mut cmd);
    cmd
      .stdout(std::process::Stdio::piped())
      .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();

    let captured_stdout = Arc::new(Mutex::new(String::new()));
    let stdout = child.stdout.take().unwrap();
    let stdout_relay_handle = std::thread::spawn({
      let captured_stdout = captured_stdout.clone();
      move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
          let line = line.unwrap();
          captured_stdout.lock().unwrap().push_str(&line);
        }
      }
    });

    let captured_stderr = Arc::new(Mutex::new(String::new()));
    let stderr = child.stderr.take().unwrap();
    let stderr_relay_handle = std::thread::spawn({
      let captured_stderr = captured_stderr.clone();
      move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines() {
          let line = line.unwrap();
          captured_stderr.lock().unwrap().push_str(&line);
        }
      }
    });

    Self {
      program,
      child,
      captured_stdout,
      captured_stderr,
      stdout_relay_handle: Some(stdout_relay_handle),
      stderr_relay_handle: Some(stderr_relay_handle),
    }
  }

  pub fn child(&self) -> &Child {
    &self.child
  }

  pub fn child_mut(&mut self) -> &mut Child {
    &mut self.child
  }
}

impl Drop for CapturedSubprocess {
  fn drop(&mut self) {
    if self.child.try_wait().is_err() {
      self.child.kill().unwrap();
    }

    if let Some(stdout_relay_handle) = self.stdout_relay_handle.take() {
      stdout_relay_handle.join().unwrap();
    }

    if let Some(stderr_relay_handle) = self.stderr_relay_handle.take() {
      stderr_relay_handle.join().unwrap();
    }

    if std::thread::panicking() {
      println!(
        "---- {} stdout ----\n{}",
        self.program,
        self.captured_stdout.lock().unwrap()
      );
      eprintln!(
        "---- {} stderr ----\n{}",
        self.program,
        self.captured_stderr.lock().unwrap()
      );
    }
  }
}
