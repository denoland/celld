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
    collections::HashSet,
    fs::{self, File},
    io::{self, BufRead, BufReader},
    path::PathBuf,
  };

  /// Returns the number of TCP connections in ESTABLISHED or CLOSE_WAIT state
  /// that are actively held open by the given PID.
  pub fn count(pid: u32) -> usize {
    let inode_set = match list_socket_inodes(pid) {
      Ok(set) => set,
      Err(_) => return 0,
    };

    let tcp_files = ["tcp", "tcp6"];
    let mut total_count = 0;

    for entry in tcp_files.iter() {
      let mut p = PathBuf::from("/proc");
      p.push(pid.to_string());
      p.push("net");
      p.push(entry);

      let f = match File::open(&p) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
        Err(e) => panic!("Failed to open {}: {}", p.display(), e),
      };

      total_count += count_filtered_connections(BufReader::new(f), &inode_set);
    }

    total_count
  }

  /// Collects socket inodes currently open in `/proc/<pid>/fd`
  fn list_socket_inodes(pid: u32) -> io::Result<HashSet<u64>> {
    let mut inodes = HashSet::new();
    let fd_path = format!("/proc/{}/fd", pid);
    for entry in fs::read_dir(fd_path)? {
      let path = entry?.path();
      if let Ok(target) = fs::read_link(&path) {
        if let Some(s) = target.to_str() {
          if let Some(inode_str) =
            s.strip_prefix("socket:[").and_then(|s| s.strip_suffix("]"))
          {
            if let Ok(inode) = inode_str.parse::<u64>() {
              inodes.insert(inode);
            }
          }
        }
      }
    }
    Ok(inodes)
  }

  /// Parses `/proc/<pid>/net/tcp*` and counts only entries in state 01 (ESTABLISHED) or 08 (CLOSE_WAIT)
  /// *and* whose inode is actively held by the process (present in `fd/`).
  fn count_filtered_connections<R: BufRead>(
    reader: R,
    inode_set: &HashSet<u64>,
  ) -> usize {
    let mut count = 0;

    for (idx, line) in reader.lines().enumerate() {
      let l = match line {
        Ok(l) => l,
        Err(_) => continue,
      };

      if idx == 0 {
        continue; // header
      }

      let fields: Vec<&str> = l.split_whitespace().collect();
      if fields.len() < 10 {
        continue;
      }

      let state = fields[3];
      let inode = fields[9].parse::<u64>().unwrap();

      if (state == "01" || state == "08") && inode_set.contains(&inode) {
        count += 1;
      }
    }
    count
  }

  #[cfg(test)]
  #[test_log::test]
  fn test_count_filtered_connections_empty() {
    let sample = b" sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 3600007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000   991        0 6423 1 0000000000000000 100 0 0 10 5
   1: 0100007F:B179 00000000:0000 0A 00000000:00000000 00:00000000 00000000   501        0 9493 1 0000000000000000 100 0 0 10 0
   2: 00000000:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 29920 1 0000000000000000 100 0 0 10 0
   3: 00000000:15B3 00000000:0000 0A 00000000:00000000 00:00000000 00000000   501        0 509276 1 0000000000000000 100 0 0 10 0
   4: 0100007F:80A1 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 40493 1 0000000000000000 100 0 0 10 0
   5: 3500007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000   991        0 6421 1 0000000000000000 100 0 0 10 5
   6: 0F05A8C0:0016 0205A8C0:7148 01 00000000:00000000 02:00012903 00000000     0        0 7151 3 0000000000000000 20 5 31 10 -1";
    let cursor = std::io::Cursor::new(sample);
    let mut inode_set = HashSet::new();
    inode_set.insert(509276); // server socket
    assert_eq!(count_filtered_connections(cursor, &inode_set), 0);
  }

  #[cfg(test)]
  #[test_log::test]
  fn test_count_filtered_connections_one() {
    let sample = b"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
       0: 3600007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000   991        0 6423 1 0000000000000000 100 0 0 10 5
          1: 0100007F:B179 00000000:0000 0A 00000000:00000000 00:00000000 00000000   501        0 9493 1 0000000000000000 100 0 0 10 0
             2: 00000000:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 29920 1 0000000000000000 100 0 0 10 0
                3: 00000000:15B3 00000000:0000 0A 00000000:00000000 00:00000000 00000000   501        0 511703 1 0000000000000000 100 0 0 10 0
                   4: 0100007F:80A1 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 40493 1 0000000000000000 100 0 0 10 0
                      5: 3500007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000   991        0 6421 1 0000000000000000 100 0 0 10 5
                         6: 0F05A8C0:0016 0205A8C0:7148 01 00000000:00000000 02:000AF11E 00000000     0        0 7151 4 0000000000000000 20 4 31 10 -1
                            7: 0100007F:AC86 0100007F:15B3 01 00000000:00000000 00:00000000 00000000   501        0 510773 1 0000000000000000 20 0 0 10 -1
                               8: 0100007F:15B3 0100007F:AC86 01 00000000:00000006 00:00000000 00000000   501        0 511704 1 0000000000000000 20 4 30 10 -1";
    let cursor = std::io::Cursor::new(sample);
    let mut inode_set = HashSet::new();
    inode_set.insert(511703);
    inode_set.insert(511704);
    assert_eq!(count_filtered_connections(cursor, &inode_set), 1);
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
    // println!("lsof output:\n{}", s);
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
  use std::io::Read;
  use std::io::Write;
  use std::net::TcpListener;
  use std::net::TcpStream;
  use std::process::Command;
  use std::process::Stdio;
  use std::thread::sleep;
  use std::time::Duration;

  fn nc_error(e: std::io::Error) -> ! {
    if e.kind() == std::io::ErrorKind::NotFound {
      panic!("netcat (nc) command not found. Please install netcat to run this test.");
    }
    panic!("could not spawn nc: {}", e);
  }

  #[test_log::test]
  fn test_count_server() {
    // Pick an OS-assigned port via a temp listener, then close it.
    let port = {
      let l = TcpListener::bind("127.0.0.1:0").unwrap();
      l.local_addr().unwrap().port()
    };

    // Start nc server: `nc -l 127.0.0.1 <port>`
    // Inherit stdio from parent process to prevent early FIN
    let mut server = match Command::new("nc")
      .arg("-l")
      .arg("127.0.0.1")
      .arg(port.to_string())
      .stdin(std::process::Stdio::piped())
      .stdout(Stdio::piped())
      .spawn()
    {
      Ok(c) => c,
      Err(e) => nc_error(e),
    };

    // Give nc time to bind
    sleep(Duration::from_secs(1));

    // No established connections yet
    assert_eq!(count(server.id()), 0);

    // Connect from this process and keep the connection active with writes
    let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();

    client.write_all(b"ping").unwrap();

    // ensure server received something
    let mut buf = [0u8; 4];
    let _ = server.stdout.as_mut().unwrap().read_exact(&mut buf);
    assert_eq!(&buf, b"ping");

    // Should have exactly one active connection
    assert_eq!(count(server.id()), 1);

    drop(client); // Prevent early drop
    let _ = server.kill();
    let _ = server.wait();
  }

  #[test_log::test]
  fn test_count_client() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx, rx) = std::sync::mpsc::channel();

    // Start accepting connections in another thread
    let _handle = std::thread::spawn(move || {
      let (mut stream, _) = listener.accept().unwrap();
      let mut buf = [0u8; 1];
      let _ = stream.read_exact(&mut buf); // read one byte
      let _ = tx.send(()); // notify main thread
      let _ = stream.read_to_end(&mut Vec::new()); // continue draining
    });

    sleep(Duration::from_millis(300));

    let mut client = match Command::new("nc")
      .arg("127.0.0.1")
      .arg(addr.port().to_string())
      .stdin(Stdio::piped())
      .spawn()
    {
      Ok(c) => c,
      Err(e) => nc_error(e),
    };

    // Not sure if this is needed, but it seems to help
    if let Some(ref mut stdin) = client.stdin {
      stdin.write_all(b"keep connection active\n").unwrap();
    }

    // Wait until the server confirms it has accepted and read
    rx.recv_timeout(Duration::from_secs(2))
      .expect("server did not confirm accept/read");

    // Should have exactly one active connection for the client process
    assert_eq!(count(client.id()), 1);

    // Cleanup
    let _ = client.kill();
    let _ = client.wait();
  }
}
