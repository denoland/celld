---
title : "Command: restore"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "reference"
weight: 540
---

The `restore` command recovers a database from a replica. By default, it
attempts to restore the latest data. However, it can also restore databases
to a specific point-in-time if it is covered by the WAL time range.

Restoration will only run if the output database file does not exist so you
cannot accidentally overwrite your running database.


## Usage

### With a database path

This command will restore a database specified in the Litestream configuration
file to its original location.

```
litestream restore [arguments] DB_PATH
```


### With a replica URL

This command will restore from a replica without using the Litestream
configuration file. It is useful if you don't have a configuration file such
as on a development machine.

```
litestream restore [arguments] REPLICA_URL
```


## Arguments

```
-o PATH
    Output path of the restored database.
    Defaults to original DB path.

-if-db-not-exists
    Returns exit code of 0 if the database already exists.

-if-replica-exists
    Returns exit code of 0 if no backups found.

-parallelism NUM
    Determines the number of WAL files downloaded in parallel.
    Defaults to 8

-replica NAME
    Restore from a specific replica.
    Defaults to replica with latest data.

-generation NAME
    Restore from a specific generation.
    Defaults to generation with latest data.

-index NUM
    Restore up to a specific WAL index (inclusive).
    Defaults to use the highest available index.

-timestamp TIMESTAMP
    Restore to a specific point-in-time.
    Defaults to use the latest available backup.

-config PATH
    Specifies the configuration file.
    Defaults to /etc/litestream.yml

-no-expand-env
    Disables environment variable expansion in configuration file.
```


## Examples

### Database restore

Restore the latest replica for the `/var/lib/db` database to its original
location:

```
$ litestream restore /var/lib/db
```

### Replica URL restore

Restore from an S3 replica URL to `/tmp/my.db`:

```
$ litestream restore -o /tmp/my.db s3://mybkt.litestream.io/my.db
```

### Restore to new path

Restores the `/var/lib/db` database to `/tmp/db` instead of its original location:

```
$ litestream restore -o /tmp/db /var/lib/db
```

### Point-in-time restore


Restore the `/var/lib/db` database to a specific point-in-time:

```
$ litestream restore -timestamp 2020-01-01T00:00:00Z /var/lib/db
```

### Filter by replica name

This example will only restore from the `s3` replica:

```
$ litestream restore -replica s3 /var/lib/db
```

