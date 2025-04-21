---
title : "Tips & Caveats"
date: 2021-03-10T00:00:00Z
layout: docs
menu:
  docs:
    parent: "general"
weight: 120
---

Running an application that uses SQLite and Litestream can require some small
tweaks to optimize performance and usability. There are also some important
caveats to know.


## Busy timeout

SQLite is built to run as a multi-process embedded database and Litestream acts
as just another process. However, Litestream requires periodic but short write
locks on the database when checkpointing occurs. SQLite will return an error by
default if your application tries to obtain a write lock at the same time.

To prevent this, your application will need to set the [`busy_timeout`
][busy_timeout] pragma when it initializes the database connection. This pragma
will wait up to a given number of milliseconds before failing a query if it is
blocked on a write. It is recommended to set this to 5 seconds:

```
PRAGMA busy_timeout = 5000;
```

[busy_timeout]: https://www.sqlite.org/pragma.html#pragma_busy_timeout


## WAL journal mode

Litestream only works with the SQLite [WAL journaling mode][wal]. This is the
recommended mode for most applications as it generally performs better and
allows for concurrent read access. Litestream will automatically set the
database mode to `WAL` if it has not already been enabled by the application.

[wal]: https://sqlite.org/wal.html


## Deleting SQLite databases

If you're deleting and recreating a SQLite database from scratch, there are 3
files which must be deleted:

- Database file
- Shared memory file (`-shm`)
- WAL file (`-wal`)

If you delete your database file but not your WAL file then SQLite will try to
apply those old WAL pages to your new database. Litestream also tracks changes
via the WAL so it can cause replication issues if the WAL file is leftover.

Additionally, Litestream currently does not track database deletions. If you
remove your database and recreate it, you should delete the `-litestream`
directory next to your database file and restart Litestream.



## Synchronous PRAGMA

SQLite must call `fsync()` to flush data to disk to ensure transactions are
durable. While in WAL journaling mode, fsync calls can be relaxed in exchange
for durability without risking data corruption.

To do this, you can change the [`synchronous`][synchronous] mode to `NORMAL`
(it typically [defaults][synchronous_default] to `FULL`):

```
PRAGMA synchronous = NORMAL;
```

This mode will ensure that the `fsync()` calls are only called when the WAL
becomes full and has to checkpoint to the main database file. This is safe as
the WAL file is append only.

[synchronous]: https://www.sqlite.org/pragma.html#pragma_synchronous
[synchronous_default]: https://github.com/sqlite/sqlite/blob/86fbbbf96440fddc6e691213495b563f2584fce1/src/sqliteInt.h#L1268-L1286


## Data loss window

Litestream performs _asynchronous replication_ which means that changes are
replicated out-of-band from the transaction that wrote the changes. This is how
many replication tools work including [PostgreSQL's log-shipping
replication][pg]. Asynchronous replication is typically much faster than
synchronous replication but it trades off durability of recent writes.

By default, Litestream will replicate new changes to an S3 replica every
second. During this time where data has not yet been replicated, a catastrophic
crash on your server will result in the loss of data in that time window.

For more typical shutdown scenarios, when Litestream receives a signal to close,
it will attempt to synchronize all outstanding WAL changes to the S3 replica before terminating.

Synchronous replication is on the Litestream roadmap but has not yet been
implemented.

## Increase snapshots frequency to improve restore performance

By default, the `snapshot-interval` on a replica is unset so a new snapshot is
taken when the previous snapshot is removed because of retention. For example,
if your retention policy is the default setting of `24h` then a new snapshot
will be taken once per day.

However, if you're writing data often then WAL files will build up over that
time period and increase your restore time. If you have frequent writes then it
is recommended to either decrease your `retention` period or to set the
`snapshot-interval` to something lower such as `1h`.

For example, if your `retention` period is one day and your `snapshot-interval`
is one hour then you will see a rolling set of 24 snapshots for your replica.


## Disable autocheckpoints for high write load servers

By default, SQLite allows any process to perform a checkpoint. A checkpoint is
when pages that are written to the WAL are copied back to the main database
file. Litestream works by controlling this checkpointing process and
replicating the pages before they get copied back into the main database.
Litestream prevents other processes from checkpointing by maintaining a read
lock on the database in between its checkpoint requests.

However, under high load with many small write transactions (e.g. tens of
thousands per second), the application's SQLite instance can perform a
checkpoint in between Litestream-initiated checkpoints and cause Litestream
to miss a WAL file. When Litestream notices this it will force a new
generation and take a full snapshot to ensure consistency.

To prevent this, it is recommended to run your application with
autocheckpointing disabled. To do this, run the following PRAGMA when you
open your SQLite connection:

```
PRAGMA wal_autocheckpoint = 0;
```

## Multiple applications replicating into location can corrupt

Multiple applications replicating into the same bucket & path can cause situations
where you will be unable to restore. It is _your_ responsibility to ensure you
do not have multiple applications replicating concurrently. In the off-chance
that it does happen, and you're unable to restore, you may see an error along
the lines of:

```
cannot find max wal index for restore: missing initial wal segment: generation=f6d6d1e96d38dafb index=00000093 offset=4152
```

In this case, manually copy your most recent snapshot in
`generations/<id>/snapshots/<snapshot>.lz4` and decompress with `lz4`.

It's a good idea to perform an integrity check on the database using `sqlite3`:

```
$ sqlite3 /path/to/db
sqlite> PRAGMA integrity_check;
ok
```

You now use this snapshot file as your application's database file and continue
replicating it again with Litestream.

[pg]: https://www.postgresql.org/docs/9.3/warm-standby.html
[s3-replica]: https://litestream.io/reference/config/#s3-replica


