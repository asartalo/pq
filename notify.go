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

var ErrBufferFull = errors.New("listener: buffer full")

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

const ListenerPidOpen = -100
const ListenerPidDisconnect = -101
const ListenerPidReconnect = -102

type Notification struct {
	BePid   int
	RelName string
	Extra   string
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

// acquireToken() attempts to grab the token a goroutine needs to be holding
// to be allowed to send anything on the connection.  Waits until the token is
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

func (l *ListenerConn) releaseToken() {
	// If we lost the connection, the goroutine running listenerConnMain will
	// have closed the channel.  As attempting to send on a closed channel
	// panics in Go, we will not try and do that; instead, listenerConnMain
	// sets the err pointer to let us know that the channel has been closed and
	// this connection is going away.  Obviously, we need be holding the mutex
	// while checking for the err pointer, but that should not be a problem.
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

func (l *ListenerConn) listenerConnMain() {
	err := l.listenerConnLoop()
	// Make sure nobody tries to start any new queries.  We have to
	// also send err pointer while holding the lock or acquireToken()
	// would panic; see comments near the beginning of that function.
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
	return fmt.Sprintf(`"%s"`, strings.Replace(relname, `"`, `""`, -1))
}

func (l *ListenerConn) TESTKillConnection() {
	l.cn.Close()
}
func (l *ListenerConn) TESTRunAnyQuery(q string) (bool, error) {
	if !l.acquireToken() {
		return false, io.EOF
	}
	defer l.releaseToken()

	return l.execSimpleQuery(q)
}


func (l *ListenerConn) Listen(relname string) (bool, error) {
	if !l.acquireToken() {
		return false, io.EOF
	}
	defer l.releaseToken()

	return l.execSimpleQuery("LISTEN " + quoteRelname(relname))
}

func (l *ListenerConn) Unlisten(relname string) (bool, error) {
	if !l.acquireToken() {
		return false, io.EOF
	}
	defer l.releaseToken()

	return l.execSimpleQuery("UNLISTEN " + quoteRelname(relname))
}

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

func (l *ListenerConn) execSimpleQuery(q string) (bool, error) {
	err := l.sendSimpleQuery(q)
	if err != nil {
		// We can't know what state the protocol is in, so we need to abandon
		// this connection.
		l.lock.Lock()
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
			// query succeeded, nothing to do
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
}

func (l *ListenerConn) Close() error {
	return l.cn.Close()
}

// Err() returns the reason the connection was closed.  You shouldn't call this
// function until l.notificationChan has been closed.
func (l *ListenerConn) Err() error {
	return l.err
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
}

func NewListener(name string) (*Listener, error) {
	l := &Listener{
		name: name,
		lock: sync.Mutex{},
		isClosed: false,
		cn: nil,
		connNotificationChan: nil,
		channels: make(map[string] bool),

		Notify: make(chan Notification, 64),
	}
	l.reconnectCond = sync.NewCond(&l.lock)

	go l.listenerMain()

	return l, nil
}

func (l *Listener) Listen(relname string) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	_, exists := l.channels[relname]
	if exists {
		return fmt.Errorf("chanenl aalreadyu")
	}

	if l.cn != nil {
		// XXX this deserves a comment
		gotResponse, err := l.cn.Listen(relname)
		if gotResponse && err != nil {
			return err
		}
		l.channels[relname] = true
		return nil
	} else {
		// add us to the list of channels and wait for a resync
		l.channels[relname] = true
		for l.cn == nil {
			l.reconnectCond.Wait()
		}
		return nil
	}
}

func (l *Listener) Unlisten(relname string) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	_, exists := l.channels[relname]
	if !exists {
		return fmt.Errorf("channel no")
	}

	if l.cn != nil {
		gotResponse, err := l.cn.Unlisten(relname)
		if gotResponse && err != nil {
			return err
		}
	}
	delete(l.channels, relname)
	return nil
}

func (l *Listener) disconnectCleanup() {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.cn.Close()
	l.cn = nil
}

func (l *Listener) resync(cn *ListenerConn, notificationChan <-chan Notification) error {
	doneChan := make(chan error)
	go func() {
		for relname := range l.channels {
			_, err := cn.Listen(relname)
			if err != nil {
				doneChan <- err
				return
			}
		}
		close(doneChan)
	}()

	for {
		// Ignore notifications while the synchronization is going on to avoid
		// deadlocks; we'll send a Reconnect anyway
		select {
			case _, ok := <-notificationChan:
				if !ok {
					notificationChan = nil
				}

			case err, ok := <-doneChan:
				if ok {
					return nil
				}
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

func (l *Listener) connect() bool {
	var notificationChan chan Notification
	var cn *ListenerConn
	var err error

	for {
		notificationChan = make(chan Notification, 32)
		for {
			if l.closed() {
				return false
			}

			cn, err = NewListenerConn(l.name, notificationChan)
			if err == nil {
				break
			}

			// XXX
			//time.Sleep(5 * time.Second)
			time.Sleep(1 * time.Millisecond)
		}

		l.lock.Lock()
		if l.resync(cn, notificationChan) == nil {
			break
		}

		// fail :-(
		l.lock.Unlock()
		time.Sleep(1 * time.Millisecond)
	}

	l.cn = cn
	l.connNotificationChan = notificationChan
	//TODO l.broadcast(ListenerPidReconnect)
	l.reconnectCond.Broadcast()
	l.lock.Unlock()

	l.Notify <- Notification{BePid: ListenerPidReconnect}

	return true
}

func (l *Listener) Close() {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.cn != nil {
		l.cn.Close()
	}
	l.isClosed = true
}

func (l *Listener) listenerMain() {
	for {
		if !l.connect() {
			break
		}

		for {
			notification, ok := <-l.connNotificationChan
			if !ok {
				// lost connection, loop again
				break
			}
			l.Notify <- notification
		}

		if l.closed() {
			break
		}

		l.disconnectCleanup()
	}
}


