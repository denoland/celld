---
title : "Command: generations"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "reference"
weight: 520
---

The `generations` command lists all generations for a database or replica. It
lists stats about their lag behind the primary database and the time range
they cover.


## Usage

### List by database

This command format finds all generations across all replicas for a database
that is specified in the configuration file. The `-replica` flag can optionally
be used to filter by replica name.

```
litestream generations [arguments] DB_PATH
```


### List by replica URL

This command format finds all generations for a single replica by using the
replica URL. This approach is useful when you do not have a configuration file.

```
litestream generations [arguments] REPLICA_URL
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
    Only applies when listing database generations.
```


## Example

This example shows a set of generations for the `/var/lib/db` database across
two replicas (`file` & `s3`). The current generation `fb47ba294ac8dd70` has
minimal lag across both replicas and there is an older generation
`3fe4669c8066974b` that has not been removed yet.

```
$ litestream generations /var/lib/db
name  generation        lag    start                 end
file  fb47ba294ac8dd70  0.2s   2020-01-01T00:00:00Z  2021-01-01T00:00:00Z
s3    fb47ba294ac8dd70  1.3s   2020-01-01T00:00:00Z  2021-01-01T00:00:00Z
file  3fe4669c8066974b  10m3s  2020-01-01T00:00:00Z  2021-01-01T00:00:00Z
```
