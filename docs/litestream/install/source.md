---
title : "Build from Source"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "install"
weight: 130
---

## Installation

Most users should follow the steps above to install prebuilt binaries but you
can also install from source if a binary is not provided for your operating
system or architecture. First, [install the Go toolchain](https://golang.org/dl/).

Next, download the [source code](https://github.com/benbjohnson/litestream) and
run the following command:

```sh
go install ./cmd/litestream
```

This will install `litestream` into your `$GOPATH/bin` directory.

