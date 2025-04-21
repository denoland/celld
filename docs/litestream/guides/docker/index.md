---
title : "Running in a Docker container"
date: 2021-03-08T00:00:00Z
layout: docs
menu:
  docs:
    parent: "infrastructure-guides"
weight: 300
---

This guide will show you how to run Litestream within a Docker container either
as a sidecar or within the same container as your application. You will need a
[Docker][docker] installed on your machine for this guide.

{{< alert icon="❗️" text="The host and container must use the same OS. For example, running SQLite on macOS and running Litestream on Linux will not work as they use different locking mechanisms." >}}


[docker]: https://www.docker.com/


## Overview

Docker is a common tool for deploying applications and Litestream can be easily
integrated into your workflow. Docker typically recommends running one
application per container and Litestream can be run as a sidecar to another
container. However, some deployment models do not support this so we'll show
you how to run your application & Litestream in the same container as well.


## Running as a sidecar

Litestream provides an [official image][image] via Docker Hub. You can use it
with a configuration file or with a replica URL.


### Using a configuration file

Typically, it's recommended to run Litestream using a configuration file as it
provides more configuration options. First, create your configuration file:

```yml
access-key-id:     YOUR_ACCESS_KEY_ID
secret-access-key: YOUR_SECRET_ACCESS_KEY

dbs:
  - path: /data/db
    replicas:
      - url: s3://BUCKET/db
```

Note that the database `path` is using the `/data` path in your Docker container.
Also, you can specify access key & secret key via environment variables instead.

Next, you'll need to attach both your data directory and your configuration
file via a volume:

```sh
docker run \
  -v /local/path/to/data:/data \
  -v /local/path/to/litestream.yml:/etc/litestream.yml \
  litestream/litestream replicate
```

You can also use named volumes instead of absolute paths. See Docker's [Use
volumes][volumes] documentation for more information about which one to use.

Now that Litestream is running, you can start your application and mount the
same data volume.


### Using a replica URL

For basic replication of a single database, you can set your S3 credentials via
environment variables, mount a volume to read from, and specify the path and
replica as arguments:

```sh
docker run \
  --env LITESTREAM_ACCESS_KEY_ID \
  --env LITESTREAM_SECRET_ACCESS_KEY \
  -v /local/path/to/data:/data \
  litestream/litestream replicate /data/db s3://BUCKET/db
```

This command will use the `LITESTREAM_ACCESS_KEY_ID` and `LITESTREAM_SECRET_ACCESS_KEY`
environment variables in your current session and pass those into your Docker
container. You can also set the values explicitly using the `-e` flag.

The command then mounts a volume from your localpath to the `/data` directory
inside the container.

Finally, the `replicate` command will replicate data from the `db` database file
in your `/data` volume to an S3 bucket. You'll need to replace `BUCKET` with the
name of your bucket.

[image]: https://hub.docker.com/r/litestream/litestream



## Running in the same container

If you are deploying to a service like [Fly.io][fly] that only uses a single
container, you can bundle both your application and Litestream together using
Litestream's built-in process supervision. You can specify your application's
process and flags by passing them to the `-exec` flag:

```sh
litestream replicate -exec "myapp -myflag myarg"
```

Or you can pass them in via the config file:

```sh
exec: myapp -myflag myarg
dbs:
  - path: /path/to/db
```

Litestream will monitor your application's process and automatically shutdown
when it closes. You can find an example application in the
[litestream-docker-example][] repository.

If you need to monitor multiple application processes, you can also use [s6][]
as a process supervisor. s6 provides a simple init system for managing multiple
processes. It is wrapped by the [s6-overlay] project to provide this service to
Docker containers. You can find a small example application in the
[litestream-s6-example][] repository. 

[fly]: https://fly.io/
[litestream-docker-example]: https://github.com/benbjohnson/litestream-docker-example
[s6]: http://skarnet.org/software/s6
[s6-overlay]: https://github.com/just-containers/s6-overlay
[litestream-s6-example]: https://github.com/benbjohnson/litestream-s6-example
[volumes]: https://docs.docker.com/storage/volumes/
