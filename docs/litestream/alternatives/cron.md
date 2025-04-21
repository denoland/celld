---
title: "Cron-based backup"
date: 2022-04-17T00:00:00Z
layout: docs
---

## Overview

Sometimes Litestream can be overkill for projects with a small database that do
not have high durability requirements. In these cases, it may be more
appropriate to simply back up your database daily or hourly. This approach can
also be used in conjunction with Litestream as a fallback. You can never have
too many backups!

{{< alert icon="❗️" text="Do not use `cp` to back up SQLite databases. It is not transactionally safe." >}}

### Backup & compress

The `sqlite3` command line interface comes with a built-in `.backup` command
which will safely backup your database to another file location. You can run
it directly from the command line:

```sh
sqlite3 /path/to/db '.backup /path/to/backup'
```

The `VACUUM INTO` command provides an alternative to this which can work better
if your database is receiving a high volume of writes, as it captures a snapshot
from a single transaction. You can use that like this:

```sh
sqlite3 /path/to/db "VACUUM INTO '/path/to/backup'"
```

Both of these examples output your database to the file `/path/to/backup`. B-tree
databases like SQLite compress well so it's recommended to compress your database:

```sh
gzip /path/to/backup
```

This will compress your database to a new file called `/path/to/backup.gz`. You
can then upload this file to external storage.


### External storage using S3

If you are using S3, you can use the [AWS CLI](https://aws.amazon.com/cli/) to
copy the file to an S3 bucket:

```sh
aws s3 cp /path/to/backup.gz s3://mybucket/backup.gz
```

### Rolling backups

You can also add the day or hour to your filename to create a rolling backup.
For example, adding the hour to your filename gives you a back up every hour
for the last day. Newer backups for the same hour will overwrite the previous
one:

```sh
# 1-day, rolling hourly backup
aws s3 cp /path/to/backup.gz s3://mybucket/backup-`date +%H`.gz

# 1-month, rolling daily backup
aws s3 cp /path/to/backup.gz s3://mybucket/backup-`date +%d`.gz

# 1-month, rolling hourly backup
aws s3 cp /path/to/backup.gz s3://mybucket/backup-`date +%d%H`.gz
```

### Monitoring backups

It's recommended to call a "dead man" service after performing your backup. This
ensures that you will be notified if your backups stop working for any reason.
[Dead Man's Snitch](https://deadmanssnitch.com/) is one such service although
others also exist.


### Restoring & testing backups

A SQLite backup is simply the database file. To restore the database, download
it and decompress (if necessary) and move it into your application's database
path. Be sure to stop your application before copying the database into place.

While it is rare for backups to go wrong with SQLite, it's always good to
regularly test your backups. To do this, simply download the back up and run
an integrity check against it:

```sh
sqlite3 /path/to/backup 'PRAGMA integrity_check'
```


### Configuring cron

[`cron`](https://man7.org/linux/man-pages/man5/crontab.5.html) is a built-in
Unix tool for periodically running commands. First, we'll create a script to
run our backup commands. This is an example that combines the commands from
above.

```sh
#!/bin/bash -x

# Ensure script stops when commands fail.
set -e

# Backup & compress our database to the temp directory.
sqlite3 /path/to/db "VACUUM INTO '/path/to/backup'"
gzip /tmp/db

# Upload backup to S3 using a rolling daily naming scheme.
aws s3 cp /tmp/db.gz s3://mybucket/db-`date +%d`.gz

# Notify dead man that back up completed successfully.
curl -d s=$? https://nosnch.in/xxxxxxxxxx &> /dev/null
```

Then you can configure `crontab` to run this daily at midnight:

```sh
# Edit your cron jobs
crontab -e

# Add this to the end of the crontab
0 0 * * * /path/to/my_backup_script.sh
```

