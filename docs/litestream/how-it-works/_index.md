---
title : "How it works"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "general"
weight: 130
---

Litestream is a streaming replication tool for SQLite databases. It runs as a
separate background process and continuously copies write-ahead log pages from
disk to one or more replicas. This asynchronous replication provides disaster
recovery similar to what is available with database servers like Postgres or
MySQL.


## Understanding the WAL

SQLite has a journaling mode called "WAL" (write-ahead log) which writes
database page changes to a separate `-wal` file first before later copying
those pages back into the main database file. This lets SQLite provide safe,
atomic transactions because it can simply delete WAL pages if a transaction gets
rolled back. If pages were written directly to the database file then there
would be no way to get back the original page data on rollback.

The WAL also allows read transactions to have their own snapshot view of the
database at the time the transaction started because there can be multiple
instances of the same database page spread across the database file & WAL.

However, the WAL continually grows so eventually pages have to be moved back to
the database file so the WAL can be restarted. This process is called
_checkpointing_ and can only be done when no transactions are active. That is
the crux of what lets Litestream replicate SQLite.


## The shadow WAL

Litestream works by effectively taking over the checkpointing process. It starts
a long-running read transaction to prevent any other process from checkpointing
and restarting the WAL file. Instead, it continually copies over new WAL pages
to a staging area called the _shadow WAL_ and manually calls out to SQLite to
perform checkpoints as necessary.

The shadow WAL is a directory next to your SQLite database where WAL files are
effectively recreated as a sequence. The first shadow WAL file starts with
`00000000.wal` and when a checkpoint occurs then it starts copying to
`00000001.wal`.

These WAL files contain the original WAL frames & checksums to ensure
consistency. To restore a database, we can simply start from a snapshot of the
database at some point in time and replay each WAL afterward to get it to the
current state.


## Snapshots & generations

In order to accurately restore a database, a snapshot and all subsequent WAL
frames must be available. Any break in the WAL frames would result in a
corrupted restored database file. This collection of snapshots & contiguous
WAL files is called a _generation_ in Litestream.

When Litestream first starts replicating a database, it creates a new
generation. This is simply a 16-character random hex string. A snapshot is
created by copying the current state of the database and then all WAL files
created after are named as 8-character incrementing hex values starting with
zero.

If Litestream detects that there is a break in the WAL frames then it will
automatically start a new generation with a new random hex string and a
snapshot. This ensures we always have a contiguous set of files to replay even
if Litestream is stopped and misses WAL frames being written.

This approach also has the benefit that two servers that accidentally share
the same replica destination will not overwrite each other's data. However,
note that it is not recommended to replicate two databases to the same exact
replica path.


## Retention

The time to restore a database from backup is directly related to the number and
size of WAL files since the snapshot. To avoid having the WAL files grow without
bound, Litestream performs new snapshots of the data periodically and removes old
WAL files.

This process is broken up into two steps. First, a snapshot interval is set to
re-snapshot the database on a regular basis. This allows you to keep copies of
your database at multiple points in time. 

The second step is retention enforcement. This periodically runs and removes any
snapshots older than the retention time as well as remove any WAL files older
than the oldest snapshot. By default, this the retention time is 24 hours.
Litestream will always ensure there is at least one snapshot retained.

This two-step process allows for more use cases such as snapshotting every day
but retaining snapshots for a week.

