# pq - A pure Go postgres driver for Go's database/sql package

[![Build Status](https://travis-ci.org/lib/pq.png?branch=master)](https://travis-ci.org/lib/pq)

## Install

	go get github.com/lib/pq

## Docs

<http://godoc.org/github.com/lib/pq>

## Use

	package main

	import (
		_ "github.com/lib/pq"
		"database/sql"
	)

	func main() {
		db, err := sql.Open("postgres", "user=pqgotest dbname=pqgotest sslmode=verify-full")
		// ...
	}

**Connection String Parameters**

These are a subset of the libpq connection parameters.  In addition, a
number of the [environment
variables](http://www.postgresql.org/docs/9.1/static/libpq-envars.html)
supported by libpq are also supported.  Just like libpq, these have
lower precedence than explicitly provided connection parameters.

See http://www.postgresql.org/docs/9.1/static/libpq-connect.html.

* `dbname` - The name of the database to connect to
* `user` - The user to sign in as
* `password` - The user's password
* `host` - The host to connect to. Values that start with `/` are for unix domain sockets. (default is `localhost`)
* `port` - The port to bind to. (default is `5432`)
* `sslmode` - Whether or not to use SSL (default is `require`, this is not the default for libpq)
	Valid values are:
	* `disable` - No SSL
	* `require` - Always SSL (skip verification)
	* `verify-full` - Always SSL (require verification)

Use single quotes for values that contain whitespace:

    "user=pqgotest password='with spaces'"

See http://golang.org/pkg/database/sql to learn how to use with `pq` through the `database/sql` package.

## Listening for notifications

PostgreSQL supports a simple publish/subscribe model over the database
connections.  See http://www.postgresql.org/docs/current/static/sql-notify.html
for more information about the general mechanism.

To start listening for notifications, you have to open a new dedicated
connection to the database using NewListener().  Its signature is:

    func NewListener(name string,
                     minReconnectInterval time.Duration,
                     maxReconnectInterval time.Duration) *Listener

The first argument should be set to a connection string to be used to
establish the database connection (see above).  minReconnectInterval controls
the duration to wait before trying to re-establish the database connection
after connection loss.  After each consecutive failure this interval is
doubled, until maxReconnectInterval is reached.  Successfully completing the
connection establishment procedure resets the interval back to
minReconnectInterval.

The return value is a pointer to an instance of a Listener struct, which
exports three functions:

* Listen(relname string) - Sends a LISTEN query to the server to start
  listening for notifications on the channel specified in relname.  Calls to
  this function will block until an acknowledgement has been received from the
  server.  Note that this may include time taken to re-establish the connection
  if there is no active connection to the server.  Calling Listen on a channel
  which has already been opened will return the error ErrChannelAlreadyOpen.
* Unlisten(relname string) - Sends an UNLISTEN query to the server.  If there
  is no active connection, this function will return immediately.  If the
  channel specified by relname is not open, this function will return
  ErrChannelNotOpen.
* Close() - Closes the connection.

The relname in both Listen and Unlisten is case sensitive, and can contain any
characters legal in an identifier (see
http://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
for more information).  Note that the identifier will be truncated to 63 bytes
by the PostgreSQL server.

A call to Listen() will only fail in three cases:
* The channel is already open.  The returned error will be
  ErrChannelAlreadyOpen.
* The query was executed on the remote server, but PostgreSQL returned an error
  message.  The returned error will be a pq.Error containing the information
  the server supplied.
* Close() has been called on the Listener.

In all other cases Listener will make sure to deliver the command to the
server, re-establishing the connection if necessary.

After a successful call to Listen, notifications can be received from the
Listener.Notify channel.  The returned Notification structure looks as follows:

    type Notification struct {
        BePid   int
        RelName string
        Extra   string
    }

BePid is the Process ID (PID) of the notifying PostgreSQL server backend.
RelName is the name of the channel.  Extra is a special payload string which
can be emitted along with the notification.  If a payload is not supplied,
Extra will be set to the empty string.

In addition to listening for notifications, any users of Listener should also
handle `Events` from the Listener.  These events are emitted through the
Listener.Event channel to offer information about the state of the underlying
database connection.   The information contained in an event are:

    type ListenerEvent struct {
        Event ListenerEventType
        Error error
    }

Error provides an error associated with the event (if any), and Event can be
one of the following:

* ListenerEventConnected - Emitted only when the database connection has been
initially established.  Error will always be nil.  This event can be safely
ignored.
* ListenerEventDisconnect - Emitted after a database connection has been lost,
either because of an error or because Close() has been called.  Error will be
set to the reason the database connection was lost.  This event can be safely
ignored.
* ListenerEventReconnected - Emitted after a database connection has been
established after losing it.  Error will always be nil.  It is *NOT* safe to
ignore this event, see below.
* ListenerEventConnectionAttemptFailed - Emitted after a connection to the
database was attempted, but failed.  Error will be set to an error describing
why the connection attempt did not succeed.  This event can be safely ignored.

In the case of a connection failure, Listener will temporarily not be receiving
any notifications from the server.  As Listener can't possibly know which
notifications would have been sent by the server had the connection been alive,
any users of Listener should treat ListenerEventReconnected as if it was a
broadcast message notifying all channels the Listener is listening on.  Failure
to do so might result in your program being unresponsive for longer periods of
time.

There is also a lower level ListenerConn interface available which gives you
more control over the underlying database connection, but its use is
discouraged, and the interface is undocumented.

## Tests

`go test` is used for testing.  A running PostgreSQL server is
required, with the ability to log in.  The default database to connect
to test with is "pqgotest," but it can be overridden using environment
variables.

Example:

	PGHOST=/var/run/postgresql go test github.com/lib/pq

Optionally, a benchmark suite can be run as part of the tests:

	PGHOST=/var/run/postgresql go test -bench .

## Features

* SSL
* Handles bad connections for `database/sql`
* Scan `time.Time` correctly (i.e. `timestamp[tz]`, `time[tz]`, `date`)
* Scan binary blobs correctly (i.e. `bytea`)
* pq.ParseURL for converting urls to connection strings for sql.Open.
* Many libpq compatible environment variables
* Unix socket support
* Notifications: `LISTEN`/`NOTIFY`

## Future / Things you can help with

* `hstore` sugar (i.e. handling hstore in `rows.Scan`)

## Thank you (alphabetical)

Some of these contributors are from the original library `bmizerany/pq.go` whose
code still exists in here.

* Andy Balholm (andybalholm)
* Ben Berkert (benburkert)
* Bill Mill (llimllib)
* Bjørn Madsen (aeons)
* Blake Gentry (bgentry)
* Brad Fitzpatrick (bradfitz)
* Chris Walsh (cwds)
* Daniel Farina (fdr)
* Everyone at The Go Team
* Evan Shaw (edsrzf)
* Ewan Chou (coocood)
* Federico Romero (federomero)
* Gary Burd (garyburd)
* Heroku (heroku)
* Jason McVetta (jmcvetta)
* Joakim Sernbrant (serbaut)
* John Gallagher (jgallagher)
* Kamil Kisiel (kisielk)
* Kelly Dunn (kellydunn)
* Keith Rarick (kr)
* Maciek Sakrejda (deafbybeheading)
* Marc Brinkmann (mbr)
* Matt Robenolt (mattrobenolt)
* Martin Olsen (martinolsen)
* Mike Lewis (mikelikespie)
* Nicolas Patry (Narsil)
* Ryan Smith (ryandotsmith)
* Samuel Stauffer (samuel)
* notedit (notedit)
