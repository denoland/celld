---
title : "Command: wal"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "reference"
weight: 570
---

The `wal` command lists WAL files available for a database or replica. This
command is not typically used in normal usage and is mainly used for debugging.


## Usage

### List by database

This command lists all WAL files across all replicas for a database specified
in the Litestream configuration file:

```
litestream wal [arguments] DB_PATH
```


### List by replica URL

This command lists all WAL files for a single replica URL. This approach is
useful when you do not have a configuration file.

```
litestream wal [arguments] REPLICA_URL
```


## Arguments

```
-config PATH
    Specifies the configuration file.
    Defaults to /etc/litestream.yml

-no-expand-env
    Disables environment variable expansion in configuration file.

-replica NAME
    Optional, filters by replica.
    Only applies when listing database snapshots.

-generation NAME
    Optional, filter by a specific generation.
```


## Example

### Database WAL files

List all WAL files across all replicas for the `/var/lib/db` database:

```
$ litestream wal /var/lib/db
```

### Filter by replica name

Lists all WAL files for the `/var/lib/db` database but filters by the `s3` replica:

```
$ litestream wal -replica s3 /var/lib/db
```

### Replica URL WAL files

Lists all WAL files for a single replica URL:

```
$ litestream wal s3://mybkt.litestream.io/db
```

