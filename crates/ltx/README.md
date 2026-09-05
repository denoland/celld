# celld-ltx

`celld-ltx` is celld's in-process SQLite replication library. It captures
committed WAL data as L0 LTX segments and reports the captured position. The
crate can write segments to a filesystem or an object store. It can restore a
database, compact levels, create a snapshot, and read segments from bundle
objects. It can also open a database by paging: a fault-in SQLite VFS reads
each page from the segments on first use, through a page map built from the
segments' own page indexes, and a paged epoch continues the chain it paged
from.

celld uses this library inside a larger durability protocol. The output gate,
epoch fencing, replicated node log, and takeover recovery enforce the write
acknowledgement contract.

## Provenance and attribution

Seeded on 2026-08-03 from a read-only snapshot of rustyriver
(https://github.com/mikenomitch/rustyriver), a from-scratch Rust
reimplementation of Litestream v0.5 and the LTX file format. celld owns and
evolves this snapshot as first-class celld source; it does not track an
upstream branch.

Attribution for the vendored and ported work:

- **rustyriver** — Copyright 2026 The rustyriver authors, licensed under the
  Apache License, Version 2.0.
- **Litestream** (https://github.com/benbjohnson/litestream) — Copyright (c)
  Ben Johnson and the Litestream authors, licensed under the Apache License,
  Version 2.0. The original replication behavior comes from tag v0.5.11. The
  current block format follows tag v0.5.16 because it includes LTX v0.5.2.
- **LTX file format and reference implementation**
  (https://github.com/superfly/ltx), tag v0.5.2 — Copyright (c) Superfly, Inc.,
  licensed under the Apache License, Version 2.0.
- **pierrec/lz4 block compressor**
  (https://github.com/pierrec/lz4), tag v4.1.23 — Copyright (c) 2015 Pierre
  Curto, licensed under the BSD 3-Clause License. The Rust port preserves the
  exact compressed bytes that the LTX v0.5.2 writer produces.

The port is not complete, deliberately. `leaser.rs` — Litestream's
object-storage lease (`leaser.go`, `heartbeat.go`, `s3/leaser.go`) — was
ported and then removed on 2026-08-06, unused. celld fences cell ownership
with a conditional-write record carrying an epoch, and fences the data path by
stamping that epoch into the LTX prefix; a lease file under the replica prefix
would be a second, competing layer. Upstream's own leaser is unwired for the
same reason. Recover it from git if a future design needs it.

The full Apache License, Version 2.0 text is in [LICENSE](LICENSE). The full
BSD 3-Clause License text is in
[LICENSE.pierrec-lz4](LICENSE.pierrec-lz4).

## File compatibility

Celld and Litestream v0.5.16 can read the new block files and the older frame
files. Litestream v0.5.11 can read only the older frame files. This is a reader
compatibility boundary because LTX keeps file version 3 for both layouts.

The ordinary celld L0 writer still emits the older frame representation. The
v0.5.2 encoder and compactor emit exact block files. Celld contains a
node-wide scheduler that publishes additive L1 files, and the scheduler is on
by default. Every takeover target must have the dual decoder before the first
L1 publication, so a mixed fleet must set `CELLD_LTX_COMPACTION=0` until all
nodes can read block files. The same reader-first requirement applies to a
later L0 writer switch.
