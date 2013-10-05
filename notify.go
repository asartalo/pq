// Package pq is a pure Go Postgres driver for the database/sql package.
// This module contains support for Postgres LISTEN/NOTIFY.
package pq

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Notification struct {
	BePid   int
	RelName string
	Extra   string
}

func recvNotification(r *readBuf) Notification {
	bePid := r.int32()
	relname := r.string()
	extra := r.string()

	return Notification{bePid, relname, extra}
}

type message struct {
	typ byte
	buf *readBuf
}

const (
	connStateIdle int32 = iota
	connStateExpectResponse
	connStateExpectReadyForQuery
)

type ListenerConn struct {
	cn *conn
	err error

	notificationChan chan<- Notification

	// guards only sending/closing of senderToken
	lock sync.Mutex
	connState int32
	// see acquireToken()
	senderToken chan bool

	replyChan chan message
}

func NewListenerConn(name string, notificationChan chan<- Notification) (*ListenerConn, error) {
	cn, err := Open(name)
	if err != nil {
		return nil, err
	}

	l := &ListenerConn{
		cn: cn.(*conn),
		notificationChan: notificationChan,
		lock: sync.Mutex{},
		connState: connStateIdle,
		senderToken: make(chan bool, 1),
		replyChan: make(chan message, 2),
	}
	// summon the token
	l.releaseToken()

	go l.listenerConnMain()

	return l, nil
}

// acquireToken() attempts to grab the token a goroutine needs to be holding to
// be allowed to send anything on the connection.  It waits until the token is
// in the caller's possession, or returns false if the connection has been
// closed and no further sends should be attempted.
func (l *ListenerConn) acquireToken() bool {
	token, ok := <-l.senderToken
	if !ok {
		return false
	}

	// sanity check
	if !token {
		panic("false token")
	}
	return token
}

// Release the token acquire by acquireToken.
func (l *ListenerConn) releaseToken() {
	// If we lost the connection, the goroutine running listenerConnMain will
	// have closed the channel.  As attempting to send on a closed channel
	// panics in Go, we will not try and do that; instead, listenerConnMain
	// sets the err pointer to let us know that the channel has been closed and
	// this connection is going away.  Obviously, we need be holding the mutex
	// while checking the err pointer, but that should not be a problem.
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.err != nil {
		return
	}

	select {
		case l.senderToken <- true:

		// sanity check
		default:
			panic("senderToken channel full")
	}
}

func (l *ListenerConn) setState(newState int32) bool {
	var expectedState int32

	switch newState {
	case connStateExpectResponse:
		expectedState = connStateIdle
	case connStateExpectReadyForQuery:
		expectedState = connStateExpectResponse
	case connStateIdle:
		expectedState = connStateExpectReadyForQuery
	default:
		panic(fmt.Sprintf("unexpected listenerConnState %d", newState))
	}

	return atomic.CompareAndSwapInt32(&l.connState, expectedState, newState)
}

// Main logic is here: receive messages from the postgres backend, forward
// notifications and query replies and keep the internal state in sync with the
// protocol state.  Returns when the connection has been lost, is about to go
// away or should be discarded because we couldn't agree on the state with the
// server backend.
func (l *ListenerConn) listenerConnLoop() error {
	for {
		t, r, err := l.cn.recvMessage()
		if err != nil {
			return err
		}

		switch t {
		case 'A':
			// notification
			n := recvNotification(r)
			l.notificationChan <- n

		case 'E':
			// We might receive an ErrorResponse even when not in a query; it
			// is expected that the server will close the connection after
			// that, but we should make sure that the error we display is the
			// one from the stray ErrorResponse, not io.ErrUnexpectedEOF.
			if !l.setState(connStateExpectReadyForQuery) {
				return parseError(r)
			}
			l.replyChan <- message{t, r}

		case 'C':
			if !l.setState(connStateExpectReadyForQuery) {
				// protocol out of sync
				return fmt.Errorf("unexpected CommandComplete")
			}
			l.replyChan <- message{t, r}

		case 'Z':
			if !l.setState(connStateIdle) {
				// protocol out of sync
				return fmt.Errorf("unexpected ReadyForQuery")
			}
			l.replyChan <- message{t, r}

		case 'T', 'N', 'S', 'D':
			// ignore
		default:
			return Error(fmt.Errorf("unexpected messge %q from server", t))
		}
	}
}

// This is the main routine for the goroutine receiving on the database
// connection.  Most of the main logic is in listenerConnLoop.
func (l *ListenerConn) listenerConnMain() {
	err := l.listenerConnLoop()

	// Make sure nobody tries to start any new queries.  We have to also set
	// the err pointer while holding the lock or acquireToken() would panic;
	// see comments near the beginning of that function.
	//
	// Noteworthy is that we only set err if it hasn't already been set.  That
	// is because the connection could be closed by either this goroutine or
	// one sending on the connection -- whoever closes the connection is
	// assumed to have the more meaningful error message (as the other one will
	// probably get net.errConnClosed), so that goroutine sets the error we
	// expose, and the other one's error is discarded.  If the connection is
	// lost while two goroutines are operating on the socket, it probably
	// doesn't matter which error we expose.
	l.lock.Lock()
	close(l.senderToken)
	if l.err == nil {
		l.err = err
	}
	l.cn.Close()
	l.lock.Unlock()

	// There might be a query in-flight; make sure nobody's waiting for a
	// response to it, since there's not going to be one.
	close(l.replyChan)

	// let the listener know we're done
	close(l.notificationChan)

	// this ListenerConn is done
}

func quoteRelname(relname string) string {
	// Relnames for channels are always quoted, and thus case sensitive.
	return fmt.Sprintf(`"%s"`, strings.Replace(relname, `"`, `""`, -1))
}

// Send a LISTEN query to the server.  See execSimpleQuery.
func (l *ListenerConn) Listen(relname string) (bool, error) {
	return l.ExecSimpleQuery("LISTEN " + quoteRelname(relname))
}

// Send an UNLISTEN query to the server.  See execSimpleQuery.
func (l *ListenerConn) Unlisten(relname string) (bool, error) {
	return l.ExecSimpleQuery("UNLISTEN " + quoteRelname(relname))
}

// Attempt to send a query on the connection.  Returns an error if sending the
// query failed, and the caller should initiate closure of this connection.
// The caller must be holding senderToken (see acquireToken and releaseToken).
func (l *ListenerConn) sendSimpleQuery(q string) (err error) {
	defer errRecover(&err)

	// must set connection state before sending the query
	if !l.setState(connStateExpectResponse) {
		panic("two queries running at the same time")
	}

	data := writeBuf([]byte("Q\x00\x00\x00\x00"))
	b := &data
	b.string(q)
	l.cn.send(b)

	return nil
}

// Execute a "simple query" (i.e. one with no bindable parameters) on the
// connection.  The first return parameter is true if the query was executed
// on the connection (if the query failed with an error message, "error" will
// be set to the error message we received from the server), or false if we did
// not manage to execute the server on the query, in which case the connection
// will be closed and all subsequently executed queries will return an error.
func (l *ListenerConn) ExecSimpleQuery(q string) (bool, error) {
	if !l.acquireToken() {
		return false, io.EOF
	}
	defer l.releaseToken()

	err := l.sendSimpleQuery(q)
	if err != nil {
		// We can't know what state the protocol is in, so we need to abandon
		// this connection.
		l.lock.Lock()
		// see listenerConnMain()
		if l.err == nil {
			l.err = err
		}
		l.cn.Close()
		l.lock.Unlock()
		return false, err
	}

	for {
		m, ok := <-l.replyChan
		if !ok {
			// We lost the connection to server, don't bother waiting for a
			// a response.
			return false, io.EOF
		}
		t, r := m.typ, m.buf
		switch t {
		case 'Z':
			// done
			return true, err
		case 'E':
			err = parseError(r)
		case 'C':
			// query succeeded, wait for ReadyForQuery
		default:
			return false, Error(fmt.Errorf("unknown response for simple query: %q", t))
		}
	}
}

func (l *ListenerConn) Close() error {
	return l.cn.Close()
}

// Err() returns the reason the connection was closed.  It is not safe to call
// this function until l.Notify has been closed.
func (l *ListenerConn) Err() error {
	return l.err
}

var errClosed = errors.New("pq: Listener has been closed")

var ErrChannelAlreadyOpen = errors.New("channel is already open")
var ErrChannelNotOpen = errors.New("channel is not open")

type ListenerEventType int
const (
	ListenerEventConnected ListenerEventType = iota
	ListenerEventDisconnected
	ListenerEventReconnected
	ListenerEventConnectionAttemptFailed
)

type ListenerEvent struct {
	Event ListenerEventType
	Error error
}

type Listener struct {
	name string

	lock sync.Mutex
	isClosed bool
	reconnectCond *sync.Cond
	cn *ListenerConn
	connNotificationChan <-chan Notification
	channels map[string] bool

	Notify chan Notification
	Event chan ListenerEvent
}

func NewListener(name string) *Listener {
	l := &Listener{
		name: name,
		lock: sync.Mutex{},
		isClosed: false,
		cn: nil,
		connNotificationChan: nil,
		channels: make(map[string] bool),

		Notify: make(chan Notification, 64),
		Event: make(chan ListenerEvent, 8),
	}
	l.reconnectCond = sync.NewCond(&l.lock)

	go l.listenerMain()

	return l
}

// Start listening for notifications on channel relname.  Only returns an error
// if we're already listening on the channel, the query was executed on the
// remote server but failed, or Close() has been called.
//
// Waits for the connection to be re-establisehed if called while the connection is down.
func (l *Listener) Listen(relname string) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.isClosed {
		return errClosed
	}

	// The server allows you to issue a LISTEN on a channel which is already
	// open, but it seems useful to be able to detect this case to spot for
	// mistakes in application logic.  If the application genuinely does't
	// care, it can check the exported error and ignore it.
	_, exists := l.channels[relname]
	if exists {
		return ErrChannelAlreadyOpen
	}

	if l.cn != nil {
		// If gotResponse is true but error is set, the query was executed on
		// the remote server, but resulted in an error.  This should be
		// relatively rare, so it's fine if we just pass the error to our
		// caller.  However, if gotResponse is false, we could not complete the
		// query on the remote server, and our underlying connection is about
		// to go away, and we only have to add relname to l.channels, and wait
		// for resync() to take care of the rest.
		gotResponse, err := l.cn.Listen(relname)
		if gotResponse && err != nil {
			return err
		}
	}

	l.channels[relname] = true
	for l.cn == nil {
		l.reconnectCond.Wait()
		// we let go of the mutex for a while
		if l.isClosed {
			return errClosed
		}
	}

	return nil
}

// Stop listening on channel relname.
func (l *Listener) Unlisten(relname string) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.isClosed {
		return errClosed
	}

	// Similarly to LISTEN, this is not an error in Postgres, but it seems
	// useful to distinguish from the normal conditions.
	_, exists := l.channels[relname]
	if !exists {
		return ErrChannelNotOpen
	}

	if l.cn != nil {
		// Similarly to Listen (see comment in that function), the caller
		// should only be bothered with an error if it came from the backend as
		// a response to our query.
		gotResponse, err := l.cn.Unlisten(relname)
		if gotResponse && err != nil {
			return err
		}
	}

	// Don't bother waiting for resync if there's no connection.
	delete(l.channels, relname)
	return nil
}

// Clean up after losing the server connection.  Returns l.cn.Err(), which
// should have the reason the connection was lost.
func (l *Listener) disconnectCleanup() error {
	l.lock.Lock()
	defer l.lock.Unlock()

	// sanity check; can't look at Err() until the channel has been closed
	select {
		case _, ok := <-l.connNotificationChan:
			if !ok {
				panic("connNotificationChan not closed")
			}
		default:
			panic("connNotificationChan not closed")
	}

	err := l.cn.Err()
	l.cn.Close()
	l.cn = nil
	return err
}

// Synchronize the list of channels we want to be listening on with the sserver
// after the connection has been established.
func (l *Listener) resync(cn *ListenerConn, notificationChan <-chan Notification) error {
	doneChan := make(chan error)
	go func() {
		for relname := range l.channels {
			// If we got a response, return that error to our caller as it's
			// going to be more descriptive than cn.Err().
			gotResponse, err := cn.Listen(relname)
			if gotResponse && err != nil {
				doneChan <- err
				return
			}

			// If we couldn't reach the server, wait for notificationChan to
			// close and then return the error message from the connection, as
			// per ListenerConn's interface.
			if err != nil {
				for _ = range notificationChan {}
				doneChan <- cn.Err()
				return
			}
		}
		doneChan <- nil
	}()

	for {
		// Ignore notifications while synchronization is going on to avoid
		// deadlocks.  We have to emit a Reconnect event anyway as we can't
		// possibly know which notifications (if any) we lost while the
		// connection was down, so there's no reason to try and process these
		// messages at all.
		select {
			case _, ok := <-notificationChan:
				if !ok {
					notificationChan = nil
				}

			case err := <-doneChan:
				return err
		}
	}
}

// caller should NOT be holding l.lock
func (l *Listener) closed() bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	return l.isClosed
}

func (l *Listener) connect() error {
	var notificationChan chan Notification
	var cn *ListenerConn
	var err error

	notificationChan = make(chan Notification, 32)
	cn, err = NewListenerConn(l.name, notificationChan)
	if err != nil {
		return err
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	err = l.resync(cn, notificationChan)
	if err != nil {
		cn.Close()
		return err
	}

	l.cn = cn
	l.connNotificationChan = notificationChan
	l.reconnectCond.Broadcast()

	return nil
}

func (l *Listener) Close() error {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.isClosed {
		return errClosed
	}

	if l.cn != nil {
		l.cn.Close()
	}
	l.isClosed = true

	return nil
}

func (l *Listener) emitEvent(event ListenerEventType, err error) {
	l.Event <- ListenerEvent{event, err}
}

func (l *Listener) listenerMain() {
	var nextReconnect time.Time

	for {
		for {
			err := l.connect()
			if err == nil {
				break
			}

			if l.closed() {
				return
			}
			l.emitEvent(ListenerEventConnectionAttemptFailed, err)
			time.Sleep(30 * time.Second)
		}
		if nextReconnect.IsZero() {
			l.emitEvent(ListenerEventConnected, nil)
		} else {
			l.emitEvent(ListenerEventReconnected, nil)
		}
		nextReconnect = time.Now().Add(30 * time.Second)

		for {
			notification, ok := <-l.connNotificationChan
			if !ok {
				// lost connection, loop again
				break
			}
			l.Notify <- notification
		}

		err := l.disconnectCleanup()
		if l.closed() {
			return
		}
		l.emitEvent(ListenerEventDisconnected, err)
		time.Sleep(nextReconnect.Sub(time.Now()))
	}
}

