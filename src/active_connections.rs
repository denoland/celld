/// Cross‑platform abstraction for counting active TCP connections for a process
///
/// Count only meaningful connection states:
/// - ESTABLISHED: fully active, data can flow in/out
/// - CLOSE_WAIT: peer closed, local side may still read
///
/// Exclude:
/// - FIN_WAIT_1/2, CLOSING, LAST_ACK: teardown in progress, no useful I/O
/// - TIME_WAIT: fully closed, waiting for socket reuse
pub fn count(pid: u32) -> usize {
  #[cfg(target_os = "linux")]
  {
    linux::count(pid)
  }
  #[cfg(target_os = "macos")]
  {
    macos::count(pid)
  }
  #[cfg(not(any(target_os = "linux", target_os = "macos")))]
  {
    unimplemented!("unsupported OS");
  }
}

#[cfg(target_os = "linux")]
mod linux {
  use std::{
    fs::File,
    io::{self, BufRead, BufReader},
    path::PathBuf,
  };

  const TCP_FILES: [&str; 2] = ["tcp", "tcp6"];

  pub fn count(pid: u32) -> usize {
    let mut total_count = 0;

    for entry in TCP_FILES.iter() {
      let mut p = PathBuf::from("/proc");
      p.push(pid.to_string());
      p.push("net");
      p.push(entry);

      // Try to open the file, handle ENOENT gracefully
      let f = match File::open(&p) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
        Err(e) => panic!("Failed to open {}: {}", p.display(), e),
      };

      total_count += count_connections(f);
    }
    total_count
  }

  /// Parses a `/proc/<pid>/net/tcp*` file and counts active connections.
  ///
  /// Count only meaningful connection states:
  /// - 01: ESTABLISHED - fully active, data can flow in/out
  /// - 08: CLOSE_WAIT - peer closed, local side may still read
  pub fn count_connections<R: BufRead>(reader: R) -> usize {
    let mut count = 0;

    for (idx, line) in reader.lines().enumerate() {
      let l = match line {
        Ok(l) => l,
        Err(e) => panic!("Failed to read line from /proc tcp file: {}", e),
      };

      if idx == 0 {
        // Header
        continue;
      }

      // Get the connection state from the 4th column
      let state = l.split_whitespace().nth(3);

      // Count ESTABLISHED (01) and CLOSE_WAIT (08) states
      if state == Some("01") || state == Some("08") {
        count += 1;
      }
    }
    count
  }

  #[test]
  fn test_linux_count_connections() {
    // Test file with:
    // - 2 ESTABLISHED connections (state 01)
    // - 1 CLOSE_WAIT connection (state 08)
    // - 1 FIN_WAIT connection (state 04) which should be ignored
    let sample = b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n   46: 010310AC:91D5 020310AC:01BB 01 00000000:00000000 00:00000000 00000000   1000        0 123456789 1 0000000000000000 100 0 0 10 0\n   47: 010310AC:91D6 020310AC:01BC 01 00000000:00000000 00:00000000 00000000   1000        0 123456790 1 0000000000000000 100 0 0 10 0\n   48: 010310AC:91D7 020310AC:01BD 08 00000000:00000000 00:00000000 00000000   1000        0 123456791 1 0000000000000000 100 0 0 10 0\n   49: 010310AC:91D8 020310AC:01BE 04 00000000:00000000 00:00000000 00000000   1000        0 123456792 1 0000000000000000 100 0 0 10 0\n";
    let cursor = std::io::Cursor::new(sample);
    // Should count 2 ESTABLISHED + 1 CLOSE_WAIT = 3 active connections
    assert_eq!(linux::count_connections(cursor), 3);
  }
}

#[cfg(target_os = "macos")]
mod macos {
  use std::{io, process::Command};

  pub fn count(pid: u32) -> usize {
    let pid_str = pid.to_string();
    let output = match Command::new("lsof")
      .args(["-n", "-P", "-p", &pid_str])
      .output()
    {
      Ok(output) => output,
      Err(e) => {
        if e.kind() == io::ErrorKind::NotFound {
          panic!(
            "lsof command not found. Please install lsof to use this feature."
          );
        } else {
          panic!("Failed to execute lsof: {}", e);
        }
      }
    };

    if !output.status.success() {
      // Non‑zero exit usually means "no matches"
      return 0;
    }

    // Check if any line contains a TCP connection in an active state
    let s = String::from_utf8_lossy(&output.stdout);

    // Count only meaningful connection states:
    // - ESTABLISHED: fully active, data can flow in/out
    // - CLOSE_WAIT: peer closed, local side may still read
    // Exclude:
    // - FIN_WAIT_1/2, CLOSING, LAST_ACK: teardown in progress, no useful I/O
    // - TIME_WAIT: fully closed, waiting for socket reuse
    println!("lsof output:\n{}", s);
    s.lines()
      .filter(|line| {
        line.contains("TCP")
          && (line.contains("ESTABLISHED") || line.contains("CLOSE_WAIT"))
      })
      .count()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Write;

  // Relies on `nc` (netcat) being available in PATH.
  // 1. Spawn `nc` in server-listen mode (no established connections yet).
  // 2. Verify watcher reports 0 active connections.
  // 3. Open a client TcpStream from the test process to that server.
  // 4. Verify watcher reports 1 active connection for the nc process.
  #[test]
  fn test_count() {
    use std::net::TcpStream;
    use std::process::Command;
    use std::thread::sleep;
    use std::time::Duration;

    // Check if nc is available
    if Command::new("nc").arg("-h").output().is_err() {
      panic!("netcat (nc) command not found. Please install netcat to run this test.");
    }

    // Pick an OS-assigned port via a temp listener, then close it.
    let port = {
      use std::net::TcpListener;
      let l = TcpListener::bind("127.0.0.1:0").expect("bind temp");
      l.local_addr().unwrap().port()
    };

    // Start nc server: `nc -l 127.0.0.1 <port>`
    // Inherit stdio from parent process to prevent early FIN
    let mut server = Command::new("nc")
      .arg("-l")
      .arg("127.0.0.1")
      .arg(port.to_string())
      .spawn()
      .expect("Failed to start netcat server");

    // Give nc time to bind
    sleep(Duration::from_millis(100));

    // No established connections yet
    assert_eq!(count(server.id()), 0);

    // Connect from this process and keep the connection active with writes
    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    client.write_all(b"keep connection active\n").unwrap();

    // Should have exactly one active connection
    assert_eq!(count(server.id()), 1);

    // Cleanup
    let _ = server.kill();
    let _ = server.wait();
  }
}
