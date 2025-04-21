---
title : "Getting Started"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "general"
weight: 110
---

This tutorial will get you up and running with Litestream locally and
replicating an SQLite database to an S3-compatible store called
[MinIO](https://min.io/). This works the same as Amazon S3 but it's easier to
get started.

By the end, you'll understand the `replicate` and `restore` commands and be able
to continuously backup your database. It assumes you're comfortable on the
command line and have [Docker](https://www.docker.com/) installed.

{{< alert icon="⏱" text="You should expect this tutorial to take about 10 minutes." >}}


## Prerequisites

### Install Litestream & SQLite

Before continuing, [please install Litestream on your local machine](/install).

You will also need [SQLite](https://sqlite.org/) installed for this tutorial. It
comes packaged with some operating systems such as macOS but you may need to
install it separately.


### Setting up MinIO

We'll use a Docker instance of [MinIO](https://min.io/) for this example. This
gets us up and running quickly but it will only persist the data for as long as
the Docker container is running. That's good enough for this tutorial but you'll
want to use persistent storage in a production environment.

First, start your MinIO instance:

```sh
docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
```

Then open a web browser to <a href="http://localhost:9001/" target="_blank">http://localhost:9001/</a>
and enter the default credentials:

```
Username: minioadmin
Password: minioadmin
```

Next, navigate to "Buckets", click the "Create Bucket" button in the top right corner and then click the
_"Save"_ icon. Name your bucket, `"mybkt"`.


## Setting up your database

Now that our MinIO bucket is created, we can replicate data to it. Litestream
can work with any SQLite database so we'll use the `sqlite3` command line tool
to show how it works.

**In a terminal window**, create a new database file:

```
sqlite3 fruits.db
```

This will open the SQLite command prompt and now we can execute SQL commands.
We'll start by creating a new table:

```
CREATE TABLE fruits (name TEXT, color TEXT);
```

And we can add some data to our table:

```
INSERT INTO fruits (name, color) VALUES ('apple', 'red');
INSERT INTO fruits (name, color) VALUES ('banana', 'yellow');
```

## Replicating your database

**In a separate terminal window**, we'll run Litestream to replicate our new
database. Make sure both terminal windows are using the same working directory.

First, we'll set our MinIO credentials to our environment variables:

```sh
export LITESTREAM_ACCESS_KEY_ID=minioadmin
export LITESTREAM_SECRET_ACCESS_KEY=minioadmin
```

Next, run Litesteam's `replicate` command to start replication:

```
litestream replicate fruits.db s3://mybkt.localhost:9000/fruits.db
```

You should see Litestream print some initialization commands and then wait
indefinitely. Normally, Litestream is run as a background service so it
continuously watches your database for new changes so the command does not exit.

If you open the [MinIO Console](http://localhost:9000/minio/mybkt/),
you will see there is a `fruits.db` directory in your bucket.


## Restoring your database

**In a third terminal window**, we'll restore our database to a new file. First,
make sure your environment variables are set correctly:

```sh
export LITESTREAM_ACCESS_KEY_ID=minioadmin
export LITESTREAM_SECRET_ACCESS_KEY=minioadmin
```

Then run:

```sh
litestream restore -o fruits2.db s3://mybkt.localhost:9000/fruits.db
```

This will pull down the backup from MinIO and write it to the `fruits2.db` file.
You can verify the database matches by executing a query on our file:

```
sqlite3 fruits2.db 'SELECT * FROM fruits'
```

The data should show:

```
apple|red
banana|yellow
```


## Continuous replication

Litestream continuously monitors your database and backs it up to S3. We can
see this by writing some more data to our original `fruits.db` database.

**In our first terminal window**, write a new row to our table:

```
INSERT INTO fruits (name, color) VALUES ('grape', 'purple');
```

Then in your **third terminal window**, restore your database from our S3 backup
to a new `fruits3.db` file:

```
litestream restore -o fruits3.db s3://mybkt.localhost:9000/fruits.db
```

We can execute a query on this file:

```
sqlite3 fruits3.db 'SELECT * FROM fruits'
```

We should now see our new row:

```
apple|red
banana|yellow
grape|purple
```


## Further reading

Litestream was built to run as a background service that you don't need to worry
about—it just replicates your database all the time. To run Litestream as a
background service, please refer to the [How-To Guides section](/guides) to
run on your particular platform.
