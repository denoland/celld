---
title: "Why I Built Litestream"
description: "Despite an exponential increase in computing power, our applications require more machines than ever because of architectural decisions made 25 years ago. You can eliminate much of your complexity and cost by using SQLite & Litestream for your production applications."
date: 2021-02-10T00:00:00Z
draft: false
weight: 50
contributors: ["Ben Johnson"]
---

_tl;dr—Despite an exponential increase in computing power, our applications
require more machines than ever because of architectural decisions made 25 years
ago. You can eliminate much of your complexity and cost by using SQLite &
Litestream for your production applications._


## When I was your age...

I can tell I'm getting old because I talk about the "good old days" of computing.
Back when knowing a single programming language and SQL was good enough to work
at most jobs. Back when you could build all your web pages with basic HTML skills.

But the good old days weren't actually that good.

They kinda sucked.

In 1995, the [LAMP stack][lamp] emerged. It was a combination of Linux, Apache,
MySQL, & PHP. It's a simple stack that all lived on one box. Unfortunately, that
box was stocked with a CPU that was measured in megahertz and memory that was
measured in megabytes. It was dog slow.

But [Moore's Law][moore] promised a better future as computers became
exponentially faster. So why do we need more computers than ever before?

[lamp]: https://tedium.co/2019/10/01/lamp-stack-php-mysql-apache-history/
[moore]: https://en.wikipedia.org/wiki/Moore%27s_law


### Where it went wrong

Since early web languages like PHP and Ruby were slow, you didn't want them
running on your database server so you moved them to their own servers in what
is called an [n-tier architecture][n-tier]. Now you can keep adding more
stateless Ruby servers to this _presentation layer_ and scale your application.

Database servers like Oracle, PostgreSQL, and MySQL tended to be complicated to
operate so this architecture helped to isolate them. Scaling and managing
a fleet of database servers is a nighmare so organizations scale up their
database machine vertically as much as possible before they scaled horizontally
to multiple servers.

This n-tier architecture sounds simple at first but it has hidden complexity. On
a single machine, our server could add an in-memory cache to speed up requests
but now data is shared across multiple machines so we must add a [memcached][]
or [Redis][] server to share cached data. Database servers are easily overloaded
by numerous connections so we have to add intermediate services like
[PgBouncer][] to pool connections. If we have events in our system that must be
communicated to all our nodes then we need a cluster of [Kafka][] machines.

Now we have a fleet of machines to manage. These systems have then caused an
explosion in infrastructure automation with tooling like [Kubernetes][]. All of
these layers slow us down from our job of writing software systems that solve
real problems.

What started as a simple two-tier software system has grown into a behemoth with
a dozen layers of complexity.

Complexity begets complexity.

[n-tier]: https://en.wikipedia.org/wiki/Multitier_architecture
[memcached]: https://memcached.org/
[redis]: https://redis.io/
[pgbouncer]: https://www.pgbouncer.org/
[kafka]: https://kafka.apache.org/
[kubernetes]: https://kubernetes.io/


## Building Litestream

My background is in writing databases. Seven years ago, I wrote a pure Go,
embedded key/value store called [BoltDB][] that has seen success in other
open-source applications that use it such as [etcd][].

For a while, I used Bolt when writing applications and it was refreshing because
there were no depedencies to set up and performance was _blazingly fast_.
However, it lacked features like schema migration, a query language, or a
[REPL][] so it made application development difficult. But instead of going back
to database servers like Postgres, I turned to [SQLite][sqlite].

[boltdb]: http://github.com/boltdb/bolt
[etcd]: https://etcd.io/
[repl]: https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop
[sqlite]: https://sqlite.org/


### Moving to SQLite

_"But nobody writes production applications with SQLite, right?"_

No, there's a growing movement of people that see the value of single process
applications. Expensify has run tests of [4 million queries per second on a
single node][expensify] using SQLite. [David Crawshaw][crawshaw] has a
[conference talk][gonorthwest] and [blog post][single-process] on building
single process applications on SQLite. Django co-creator [Simon
Willison][willison] built a data exploration and publishing tool called
[Datasette][] that's built on SQLite.

SQLite is known for being bulletproof and having an [absurdly in-depth testing
suite](sqlite-testing). Its motto is _"Small. Fast. Reliable. Choose any three."_

So why is SQLite considered a "toy" database in the application development
world and not a production database?

[expensify]: https://blog.expensify.com/2018/01/08/scaling-sqlite-to-4m-qps-on-a-single-server/
[crawshaw]: https://twitter.com/davidcrawshaw
[gonorthwest]: https://www.youtube.com/watch?v=RqubKSF3wig
[single-process]: https://crawshaw.io/blog/one-process-programming-notes
[willison]: https://simonwillison.net/
[datasette]: https://datasette.io/
[sqlite-testing]: https://www.sqlite.org/testing.html



### That one big issue

The biggest problem with using SQLite in production is disaster recovery. If
your server dies, so does your data. That's... not good.

Other database servers have replication so they can stream database changes to
another server in case one goes down. The best you can hope for with standard
SQLite is to run a nightly backup. Solutions like [rqlite][] are great but
it requires a 3-node cluster.

Why can't SQLite have a replication tool that's as easy to use as SQLite?

[rqlite]: https://github.com/rqlite/rqlite


### The problem Litestream solves

I built Litestream to bring back sanity to application development. Litestream
is a tool that runs in a separate process and continuously replicates a SQLite
database to Amazon S3. You can get up and running with a few
lines of configuration. Then you can set-it-and-forget-it and get back to
writing code.

You might think this sounds expensive to continuously write to cloud storage
that provides [99.99% uptime][s3] and [99.999999999% durability][s3] but
it's astoundingly cheap. Typical costs are only about $1 per month. Litestream
is free and open-source too so there's never a license to pay for.

[s3]: https://aws.amazon.com/s3/storage-classes/


### But I need 100% uptime...

The software industry has chased uptime as a goal in and of itself over the last
several decades. Solutions such as Kubernetes tout the benefits of zero-downtime
deployments but ignore that their inherent complexity causes availability
issues. [There's even a web site dedicated to public postmortems related to
Kubernetes.][k8s.af]

Most cloud providers provide multiple layers of redundancy in their systems to
protect against individual node and network failures. This doesn't provide a
100% guarantee but will provide you with very high uptime servers. Anecdotally,
I've run several VPS servers over the years which all have well over 99.9%
uptime and have suffered no catastrophic failures.

[k8s.af]: https://k8s.af/


### Sounds good, but how do you scale this?

Developers always want to know how to scale but that depends on their particular
application. Typically, you want to scale vertically first by simply increasing
amount of CPUs cores and RAM on your machine first.

Servers these days have a _ton_ of power. I recently wrote a Go web application
with a SQLite database that would serve an HTTP request with multiple database
queries in under _50µs_ on a $5/month VPS. That translates to thousands of
requests per second per core. SQLite scales reads well with the number of cores
on a machine. Amazon AWS has machines that can then scale up to 96 CPU cores and
hundreds of gigabytes of RAM.

If you exceed the capacity of a single node, sharding your data can allow you to
scale horizontally to multiple nodes. This works particularly well for SaaS
applications where each customer is isolated from one another. Because SQLite
and Litestream simplify deployment, managing a cluster of several isolated nodes
is easy to maintain.


## The Future of Litestream

Litestream is helping to simplify application development but that's only the
start of it. There are exciting features coming including replication to
[read-only replicas][replicas]. This will give you the ability to run local
copies of your database at the edge to deliver requests instantly.

If you're interested in trying out Litestream, the [Getting
Started](/getting-started) can get you up and running in less than 10 minutes.

I'm interested to hear from others that want to simplify and improve application
development. If you have ideas or thoughts about the future of Litestream,
please get in touch on the [GitHub Discussions board][discussions] and drop me a
line.

_Update: This post sparked a lot of discussion on Hacker News that you can [read here][hn]._

—Ben Johnson ([@benbjohnson](https://twitter.com/benbjohnson))

[replicas]: https://github.com/benbjohnson/litestream/issues/8
[discussions]: https://github.com/benbjohnson/litestream/discussions
[hn]: https://news.ycombinator.com/item?id=26103776
