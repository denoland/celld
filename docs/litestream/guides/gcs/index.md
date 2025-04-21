---
title : "Replicating to Google Cloud Storage"
date: 2021-06-02T00:00:00Z
layout: docs
menu:
  docs:
    parent: "replica-guides"
weight: 440
---

This guide will show you how to use Litestream to replicate to a [Google Cloud
Storage][] bucket. You will need a [Google Cloud][] account and to complete
this guide.

[Google Cloud Storage]: https://cloud.google.com/storage
[Google Cloud]: https://cloud.google.com/


## Setup

### Create a bucket

In the [GCP Console][], use the top search bar to navigate to _"Cloud Storage"_.
You may need to enable storage and billing if you have not previously set it up.

Next, click the _"Create Bucket"_ button. Enter a globally unique bucket name
and click the _"Create"_ button.

[GCP Console]: https://console.cloud.google.com/


### Create a service account

On a Compute Engine VM or Cloud Run service, Litestream will automatically pick
up the credentials associated with the instance from the instance's metadata
server.

If you run Litestream outside of Google Cloud, you'll need to set up a service 
account to authenticate into GCP and access your bucket.
From the top search bar, navigate to _"Service Accounts"_. Enter a name
for your service account and click the _"Create"_ button.

Next, you'll need to grant the _Storage Admin_ role to your service accounts
so they can read & write data. Then click the _"Done"_ button.

After your account has been created, click on it and go to the _"Keys"_
subsection. Click the _"Add Key"_ drop-down and select _"Create new key"_.
Choose the _"JSON"_ option.

This will download a JSON file to your local computer. Move it to a safe
location and set an environment variable pointing to its path:

```sh
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/xxxxxxx.json
```

## Usage

### Command line usage

You can specify your replica as a replica URL on the command line. For example,
you can replicate a database to your bucket. _Replace the placeholders with your
bucket & path._

```sh
litestream replicate /path/to/db gcs://BUCKET/PATH
```

You can later restore your database from Google Cloud Storage to a local `my.db`
path with the following command.

```sh
litestream restore -o my.db gcs://BUCKET/PATH
```

### Configuration file usage

Litestream is typically run as a background service which uses a [configuration
file][]. You can configure a replica for your database using the `url` format.

```yaml
dbs:
  - path: /path/to/local/db
    replicas:
      - url: gcs://BUCKET/PATH
```

Or you can expand your configuration into multiple fields:

```yaml
dbs:
  - path: /path/to/local/db
    replicas:
      - type:   gcs
        bucket: BUCKET
        path:   PATH
```

[configuration file]: /reference/config
