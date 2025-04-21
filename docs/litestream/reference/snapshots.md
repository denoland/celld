---
title : "Command: snapshots"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "reference"
weight: 550
---

The `snapshots` command lists snapshots available for a database or replica.


## Usage

### List by database

This command lists all snapshots across all replicas for a database specified
in the Litestream configuration file:

```
litestream snapshots [arguments] DB_PATH
```


### List by replica URL

This command lists all snapshots for a single replica URL. This approach is
useful when you do not have a configuration file.

```
litestream snapshots [arguments] REPLICA_URL
```


## Arguments

```
-config PATH
    Specifies the configuration file.
    Defaults to /etc/litestream.yml

-replica NAME
    Optional, filters by replica.
    Only applies when listing database snapshots.

-no-expand-env
    Disables environment variable expansion in configuration file.
```


## Example

### Database snapshots

List all snapshots across all replicas for the `/var/lib/db` database:

```
$ litestream snapshots /var/lib/db
```

### Filter by replica name

Lists all snapshots for the `/var/lib/db` database but filters by the `s3` replica:

```
$ litestream snapshots -replica s3 /var/lib/db
```

### Replica URL snapshots

Lists all snapshots for a single replica URL:

```
$ litestream snapshots s3://mybkt.litestream.io/db
```

