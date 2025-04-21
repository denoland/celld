---
title : "Running as a Systemd service"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "infrastructure-guides"
weight: 320
---

This guide will get you running Litestream as a systemd service on Debian-based
operating systems such as Ubuntu. Running as a background service means that
Litestream will always run in the background and restart automatically if the
server restarts. It assumes you are comfortable with using a command line.


## Prerequisites

This guide assumes you have read the [_Getting Started_](/getting-started)
tutorial already. Please read that to understand the basic operation of Litestream.


### Install Litestream & SQLite

Before continuing, [please install Litestream](/install/debian).

You will also need [SQLite](https://sqlite.org/) installed for this guide. It
comes packaged with some operating systems such as macOS but you may need to
install it separately.


### Creating an S3 bucket

If you don't already have an Amazon AWS account, you can go
[https://aws.amazon.com/](https://aws.amazon.com/) and click "Create Account".
Once you have an account, you'll need to [create an AWS IAM user][iam] with
_programmatic access_ and with `AmazonS3FullAccess` permissions. After creating
the user, you should have an **access key id** and a **secret access key**. We
will use those in one of the steps below.

You'll also need to create a bucket in AWS S3. You'll need to create a unique
name for your bucket. In this guide, we'll name our bucket
`"mybkt.litestream.io"` but replace that with your bucket name in the examples
below.

[iam]: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html


## Configuration file

When running as a systemd service, we'll configure Litestream using a
[configuration file](/reference/config) instead of command line flags so we
don't need to edit the service definition. The default path for the Litestream
configuration is `/etc/litestream.yml`

Litestream monitors one or more _databases_ and each of those databases
replicates to one or more _replicas_. First, we'll create a basic configuration
file. Make sure to replace your AWS credentials with your own, the bucket name
with your bucket name, and update `MYUSER` to your local Linux username.

```
sudo cat > /etc/litestream.yml <<EOF
access-key-id:     AKIAxxxxxxxxxxxxxxxx
secret-access-key: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/xxxxxxxxx

dbs:
  - path: /home/MYUSER/friends.db
    replicas:
      - url: s3://mybkt.litestream.io/friends.db
        sync-interval: 1s
EOF
```

This configuration specifies that we want Litestream to monitor our
`friends.db` database in our home directory and continuously replicate it to
our `mybkt.litestream.io` S3 bucket.

After changing our configuration, we'll need to restart the Litestream service
for it to register the change:

```
sudo systemctl restart litestream
```

## Writing to our database

Now that Litestream is running in the background, let's create our `friends.db`
database:

```sh
sqlite3 friends.db
```

We'll create a simple table to store our friends' names:

```sql
CREATE TABLE friends (name TEXT);
```

Then we'll insert some rows:

```
INSERT INTO friends (name) VALUES ('Cory');
INSERT INTO friends (name) VALUES ('Kelly');
```

Then type `.quit` or hit `CTRL-D` to exit the `sqlite3` session.


## Simulating a disaster

Next, we'll simulate a catastrophic server failure by stopping Litestream:

```sh
sudo systemctl stop litestream
```

And then we'll delete our database:

```sh
rm -rf friends.db friends.db-shm friends.db-wal friends.db-litestream
```

This is the state our server would be in if it had crashed and we had rebuilt it
but lost our data.


## Restoring our database

Our Litestream service and configuration file are still in place so we can
easily restore our database. Simply run the `restore` command with our database
path:

```sh
litestream restore friends.db
```

Litesteam will find the database in the configuration file and restore the most
recent copy it has from its S3 replica.

You can verify that your data is all there by connecting with SQLite:

```
sqlite3 friends.db
```

And querying for your data:

```
SELECT * FROM friends;
Cory
Kelly
```


## Further reading

You now have a production-ready replication setup using SQLite and Litestream.
Please see the [Reference](/reference) section for more configuration options.
