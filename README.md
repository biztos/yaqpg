# YAQPg - Yet Another Queue for PostgreSQL

YAQPg is a _simple_ PostgreSQL locking (and skipping) queue for local use.
It is pronounced "yack-pee-gee" -- or _jákpídzí_ or อยากผีจี if you prefer.

It is intended for small, _simple_ workloads.  If you want to build a system
with a queue that might one day expand to use
[RabbitMQ](https://www.rabbitmq.com)
or [Amazon SQS](https://aws.amazon.com/sqs/), but right now you just want to
run it on a single host, YAQPg might be of help.  If you want to run a bigger
queue on your PostgreSQL database, and you know what you're doing, you should
consider using the original [pgq](https://github.com/pgq/pgq).
If you want to try different, possibly more mature implementation of a
PostgreSQL queue in Go, there are
[many](https://pkg.go.dev/search?q=pgq&m=package)
[packages](https://pkg.go.dev/search?q=postgres+queue&m=package) available.

The idea came (to me) from these two authors:

[David Christensen](https://www.crunchydata.com/blog/message-queuing-using-native-postgresql)

[Andrew Stuart](https://news.ycombinator.com/item?id=20020501)

This implementation is biased towards what we consider the "normal" use-case
at [TONSAI LLC](https://tonsai.dev).  Your definition of "normal" may vary!

## WARNING! ALPHA SOFTWARE!

This package is new (as of June 2023) and has not been tested much.  Like all
software, it probably contains bugs, and like all _new_ software it probably
contains a lot of them. 🪲🪲🪲

## Example Usage

```go

package main

import (
    "context"
    "log"
    "time"

    "github.com/biztos/pgslq"
)

func main() {

    queue := pgslq.MustStartNamedQueue("example")
    if err := queue.Add("ex1", []byte("any payload")); err != nil {
        log.Fatal(err)
    }
    if err := queue.AddDelayed("ex2", []byte("later payload"), time.Second); err != nil {
        log.Fatal(err)
    }
    proc := pgslq.FunctionProcessor{
        func(ctx context.Context, item *pgslq.Item) error {
            log.Println("processing", item.Ident, string(item.Payload))
            return nil
        }}
    queue.LogCounts()       // shows 1 ready, 1 pending
    time.Sleep(time.Second) // wait out the pending item
    if err := queue.Process(2, proc); err != nil {
        log.Fatal(err)
    }
    queue.LogCounts() // shows queue is empty

}

```

## How It Works

YAQPg inserts items into a queue table in your PostgreSQL database.  Within a
queue table, there can be many named queues.

Each item consists of an `Ident`ifier and a binary `Payload`.  An item also
has a `ReadyAt` timestamp, indicating the earliest time at which it can be
processed; and an `Attempts` count to allow giving up.

When you `Process` an item in the queue, it asynchronously executes the
`ProcessFunc` function with the `Item` and a timeout context; if that
function returns an `error` then the processing was assumed to have failed
and the item is released back into the queue, subject to the `MaxAttempts`
value of the `Queue`.

If the function returns `nil` or `MaxAttempts` is exceeded, the item is
dropped from the queue, i.e. deleted from the database.

## Limitations

### Duplicates Allowed

Preventing duplicates is a tricky problem, because:

- Duplicates of what exactly? Identifier? Payload?
- Always? Optionally?
- For how long?
- At what performance cost?
- And what should you do when you get a dupe anyway?

Instead of giving opinionated answers to these questions, this package simply
allows duplicates.  Queue processing should be idempotent(ish), and if it
can't be then build that logic into your processor!

### Binary Payload

The payload is just a `[]byte` -- that is, a `BYTEA` in the database.  This is
convenient or inconvenient, depending on your use case.

The rationale is pretty simple: you should not be examining the payload on the
database anyway, so opacity is not an issue; you would have to marshal behind
the scenes anyway if we have `jsonb` on the database side and some object on
the Go side; and pure bytes leaves the door open to sending actual binary
payloads and/or compressing the payloads.

It _might_ change to `interface{}` (aka `any`) in the future, but probably
not.

## Recommendations (not enforced)

### Use autovacuum.

Not using
[autovacuum](https://www.postgresql.org/docs/current/routine-vacuuming.html)
will very likely cause pain!

### Use a dedicated database.

Because the insert/update/delete activity on a job queue can become very
intense, and require a lot of vacuuming, you should usually keep the queue(s)
in a separate database from your main database (if you have one).

That way, you can examine the database storage and watch for problems.  This
being experimental software, there might be problems!

You *may* also wish to use a dedicated table per queue, or even a dedicated
database.  But out of the box, `yaqpg` will handle multiple named queues in a
single table.

You shouldn't need a dedicated server unless you are running high volumes, in
which case you probably shouldn't be using this package. (Or maybe yes?)

### Don't put too much in the Payload.

That's not what a queue is for!  And your database may not like it.

There are no hard limits here, so `$TOO_MUCH` is Up To You.  But you have been
warned.

## Schema

This package includes a file `schema.sql` which will create the default table
named `yaqpg_queue` in your database.  You can also get this SQL as a string
from the `Schema` function.

You can make your own queue table, but it needs to have _all_ the columns in
the schema SQL or it will not work with this package.
