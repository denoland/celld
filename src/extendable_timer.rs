/// Spawns a task that will run `work` at `initial_deadline`
/// but can be extended by calling `handle.extend(..)`.
pub fn spawn_extendable_timer<F>(
  initial_deadline: tokio::time::Instant,
  work: F,
) -> TimerHandle
where
  F: std::future::Future + Send + 'static,
  F::Output: Send + 'static,
{
  // Control channel: send the *absolute* new deadline.
  // We use a watch chennel here because only the most recently requested
  // deadline matters.
  let (tx, mut rx) = tokio::sync::watch::channel(initial_deadline);

  let task_handle = tokio::spawn(async move {
    let mut sleeper = std::pin::pin!(tokio::time::sleep_until(*rx.borrow()));

    loop {
      tokio::select! {
          // Timer finished – do the work and leave.
          _ = &mut sleeper => {
              work.await;
              break;
          }

          // Somebody sent a later deadline → push the timer out.
          Ok(_) = rx.changed() => {
              let new_deadline = *rx.borrow_and_update();
              sleeper.as_mut().reset(new_deadline);
          }
      }
    }
  });

  TimerHandle { tx, task_handle }
}

/// Returned to the caller so they can extend the timer.
pub struct TimerHandle {
  tx: tokio::sync::watch::Sender<tokio::time::Instant>,
  task_handle: tokio::task::JoinHandle<()>,
}

impl TimerHandle {
  /// Tell the timer to extend to the given deadline.
  pub fn extend(&self, new_deadline: tokio::time::Instant) {
    let _ = self.tx.send(new_deadline);
  }
}
