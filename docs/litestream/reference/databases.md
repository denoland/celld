---
title : "Command: databases"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "reference"
weight: 510
---

The `databases` command lists all databases in the configuration file as well
as list their associated replicas.

## Usage

```
litestream databases [arguments]
```

## Arguments

```
-config PATH
    Specifies the configuration file.
    Defaults to /etc/litestream.yml

-no-expand-env
    Disables environment variable expansion in configuration file.
```


## Example

This example shows a single database `/var/lib/db` defined in the configuration
file with two replicas named `"file"` and `"s3"`:

```
$ litestream databases
path         replicas
/var/lib/db  file,s3
```

