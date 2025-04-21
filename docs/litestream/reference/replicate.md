---
title : "Command: replicate"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "reference"
weight: 530
---

The `replicate` command starts a server to monitor & continuously replicate
SQLite databases. You can specify your database & replicas in a configuration
file or you can replicate a single database file by specifying its path and its
replicas in the command line arguments.


## Usage

### Using a configuration file

This command format will read all options from the configuration file. This
is the recommended approach for production systems.

```
litestream replicate [arguments]
```


### Replicate a single file

This command format sets up replication for a single database file `DB_PATH`
and replicates it to one or more replica URLs. This is useful for testing
Litestream but is not recommended for production use.

```
litestream replicate [arguments] DB_PATH REPLICA_URL [REPLICA_URL...]
```


## Arguments

```
-config PATH
    Specifies the configuration file.
    Defaults to /etc/litestream.yml

-exec CMD
    Executes a subcommand. Litestream will exit when the child
    process exits. Useful for simple process management.

-no-expand-env
    Disables environment variable expansion in configuration file.
```
