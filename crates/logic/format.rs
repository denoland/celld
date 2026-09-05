//! The bucket format a node can read, and the fleet-wide gate that keeps a
//! node from writing what a live peer cannot read.
//!
//! A rolling update runs two releases against one bucket for the length of
//! the rollout. Anything the newer release writes that the older one cannot
//! restore is a cell that goes dark when it fails over to an old node, and a
//! cell that cannot come back on a downgrade. The rule that prevents both is
//! structural: each node publishes the newest format it reads, and a node
//! writes a format only while every live lease reads it.

use crate::CapacityPeer;

/// The newest bucket format this release reads.
///
/// - `1`: every epoch opens with a whole-database snapshot, so a restore
///   reads one epoch. Every release before paged restore reads this.
/// - `2`: an epoch can page in and continue its predecessor's chain from a
///   marker at the cut. A format-1 reader finds no snapshot in that epoch and
///   cannot restore the cell.
pub const BUCKET_FORMAT: u16 = 2;

/// The format a lease without the field reads: the releases before the
/// field existed all read format 1.
const FIELDLESS_FORMAT: u16 = 1;

/// Whether every live lease reads `format`, so a node can write it.
///
/// A lease counts while it has not expired, so a crashed old node holds the
/// gate closed for one lease lifetime, which is the same bound its cells
/// wait before a peer takes them over. No live lease at all is `false`: a
/// node that has not seen the fleet does not know who is in it, and the
/// older format is always safe to write.
pub fn fleet_reads(peers: &[CapacityPeer], now_ms: u64, format: u16) -> bool {
    let mut live = peers
        .iter()
        .filter(|peer| peer.expires_ms > now_ms)
        .peekable();
    live.peek().is_some()
        && live.all(|peer| peer.bucket_format.unwrap_or(FIELDLESS_FORMAT) >= format)
}
