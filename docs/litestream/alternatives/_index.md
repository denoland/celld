---
title: "Alternatives"
date: 2022-04-17T00:00:00Z
layout: docs
menu:
  docs:
    parent: "general"
weight: 140
---

## Overview

Litestream aims to provide a balance between durability and operational
complexity by batching changes into one-second windows and asynchronously
backing those changes up to external storage. This improves write performance
at the expense of having a small window of data loss in the event of a
catastrophic failure.

However, this tradeoff may not make sense for all applications. This page lists
alternative approaches and when they might be appropriate.


## Periodic SQLite backups

Sometimes Litestream can be overkill for projects with a small database that do
not have high durability requirements. In these cases, it may be more
appropriate to simply back up your database daily or hourly. This approach can
also be used in conjunction with Litestream as a fallback. 

Please see our [guide to running a cron-based backup strategy](/alternatives/cron).


## LiteFS

[LiteFS](https://github.com/superfly/litefs) is a distributed file system that
automatically replicates SQLite databases across a cluster of machines. It is
meant for environments where high availability and low global latency are
important whereas Litestream is primarily meant for disaster recovery.

The LiteFS project is developed by the same creators of Litestream and they
share many similarities internally.


## Raft-based consensus

You can achieve higher durability guarantees by running a cluster of nodes that
perform [Raft-based consensus](https://raft.github.io/) for every write. This
approach trades higher durability for lower write throughput as each write needs
to be committed to a quorum of nodes in the cluster.

Two projects exist that implement Raft-based consensus over SQLite:

- [rqlite](https://rqlite.io/)
- [dqlite](https://dqlite.io/)


## Virtual File System (VFS)-based approaches

Litestream works as a separate process so that it can separate operational
concerns from application concerns. However, there are replication solutions
that allow you to compile replication into your application using the SQLite
virtual file system:

- [Verneuil](https://github.com/backtrace-labs/verneuil)
- [LiteReplica](http://litereplica.io/)


## Other databases

Client/server databases such as [Postgres](https://www.postgresql.org/) and
[MySQL](https://www.mysql.com/) provide additional replication options such
as synchronous replication. These can be also be good choices over SQLite
although these typically have more operational complexity.


