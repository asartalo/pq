// Package pq is a pure Go Postgres driver for the database/sql package.
// This module contains support for Postgres LISTEN/NOTIFY.
package pq

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
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

type ListenerConn struct {
	cn *conn
	err error

	notificationChan chan<- Notification

	// guards only sending/closing of senderToken
	lock sync.Mutex
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
		senderToken: make(chan bool, 1),
		replyChan: make(chan message, 1),
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
	// sets the channel pointer to nil to let us know that the channel has been
	// closed.  Obviously, we need be holding the mutex while checking for the
	// channel's nil-ness, but that should not pose a problem as there's only
	// one token anyway.
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.senderToken == nil {
		return
	}

	select {
		case l.senderToken <- true:

		// sanity check
		default:
			panic("senderToken channel full")
	}
}

func (l *ListenerConn) recv() (typ byte, buf *readBuf, err error) {
	typ, buf, err = l.cn.recvMessage()
	if err != nil {
		return
	}

	return
}

func (l *ListenerConn) listenerConnMain() {
	for {
		t, r, err := l.recv()
		if err != nil {
			// Make sure nobody tries to start any new queries.  We can't
			// simply close senderToken or acquireToken() would panic; see
			// comments near the beginning of that function.
			l.lock.Lock()
			close(l.senderToken)
			l.senderToken = nil
			l.lock.Unlock()

			// There might be a query in-flight; make sure nobody's waiting for a
			// response to it, since there's not going to be one.
			close(l.replyChan)

			// let the listener know we're done
			l.err = err
			close(l.notificationChan)

			// this ListenerConn is done
			return
		}

		switch t {
		case 'A':
			// notification
			n := recvNotification(r)
			l.notificationChan <- n
		case 'Z', 'E':
			// reply to a query
			select {
				case l.replyChan <- message{t, r}:

				// sanity check
				default:
					panic("replyChan channel full")
			}
		case 'C', 'T', 'N', 'S', 'D':
			// ignore
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
}

func (l *ListenerConn) Listen(relname string) error {
	if !l.acquireToken() {
		return io.EOF
	}
	defer l.releaseToken()

	return l.execSimpleQuery("LISTEN " + quoteRelname(relname))
}

func quoteRelname(relname string) string {
	return fmt.Sprintf(`"%s"`, strings.Replace(relname, `"`, `""`, -1))
}

func (l *ListenerConn) execSimpleQuery(q string) (err error) {
	defer errRecover(&err)

	b := l.cn.writeBuf('Q')
	b.string(q)
	l.cn.send(b)

	for {
		m, ok := <-l.replyChan
		if !ok {
			// We lost the connection to server, don't bother waiting for a
			// a response.
			return io.EOF
		}
		t, r := m.typ, m.buf
		switch t {
		case 'Z':
			// done
			return nil
		case 'E':
			return parseError(r)
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
	panic("not reached")
}

func (l *ListenerConn) Unlisten(relname string) error {
	if !l.acquireToken() {
		return io.EOF
	}
	defer l.releaseToken()

	return l.execSimpleQuery("UNLISTEN " + quoteRelname(relname))
}

func (l *ListenerConn) Close() error {
	return l.cn.Close()
}

// Err() returns the reason the connection was closed.  You shouldn't call this
// function until l.notificationChan has been closed.
func (l *ListenerConn) Err() error {
	return l.err
}

type listenRequest struct {
	relname string
	unlisten bool
}

type listenClient struct {
	ch chan<- Notification
	relnames map[string] bool
}

type Listener struct {
	name string
	closed bool
	cn *ListenerConn
	notificationChan <-chan Notification

	listenRequestChan chan listenRequest
	listenRequestWorkerSignaler chan bool
	listenRequestWorkerWG sync.WaitGroup

	lock sync.Mutex
	listenClients map[chan<- Notification] *listenClient
	channels map[string] map[chan<- Notification] *listenClient
}

func NewListener(name string) (*Listener, error) {
	l := &Listener{
		name: name,
		closed: false,
		cn: nil,
		notificationChan: nil,
		listenRequestChan: nil,
		listenRequestWorkerSignaler: nil,
		lock: sync.Mutex{},
		listenClients: make(map[chan<- Notification] *listenClient),
		channels: make(map[string] map[chan<- Notification] *listenClient),
	}

	go l.listenerMain()

	return l, nil
}

func (l *Listener) requestListen(relname string, unlisten bool) error {
	request := listenRequest{
		relname: relname,
		unlisten: unlisten,
	}
	select {
		case l.listenRequestChan <-request:
			return nil
		default:
			return ErrBufferFull
	}
}

func (l *Listener) Listen(relname string, c chan<- Notification) (bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	lnc, ok := l.listenClients[c]
	if !ok {
		lnc = &listenClient{
			ch: c,
			relnames: make(map[string] bool, 1),
		}
		l.listenClients[c] = lnc
	}
	if lnc.ch == nil {
		lnc.ch = c
	}
	lnc.relnames[relname] = true

	data, ok := l.channels[relname]
	if ok {
		// the connection is already listening on this channel, we're done
		data[c] = lnc
		if l.cn == nil {
			// .. or would be, if there was a connection.  In this case we
			// return false, and the caller will have to wait for the
			// connection to be re-established.
			return false, nil
		}
		return true, nil
	}

	data = make(map[chan<- Notification] *listenClient, 1)
	data[c] = lnc
	l.channels[relname] = data

	if l.cn == nil {
		return false, nil
	}

	err := l.requestListen(relname, false)
	if err != nil {
		return false, err
	}

	return false, nil
}

func (l *Listener) Unlisten(relname string, c chan<- Notification) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	data, ok := l.channels[relname]
	if !ok {
		// already gone, nothing to do
		return nil
	}

	delete(data, c)
	if len(data) == 0 {
		// no empty maps in channels
		delete(l.channels, relname)

		// Request the channel to be UNLISTENed.  ErrBufferFull is not a
		// problem here; we'll just do it later.
		err := l.requestListen(relname, true)
		if err != nil && err != ErrBufferFull {
			return err
		}
	}
	return nil
}

func (l *Listener) disconnectCleanup() {
	l.killListenRequestWorker()
	l.listenRequestWorkerWG.Wait()

	l.lock.Lock()
	close(l.listenRequestChan)
	l.listenRequestChan = nil
	close(l.listenRequestWorkerSignaler)
	l.listenRequestWorkerSignaler = nil
	l.lock.Unlock()
}

// caller must be holding l.lock
func (l *Listener) broadcast(pid int) {
	n := Notification{
		BePid: pid,
	}
	for _, lnc := range l.listenClients {
		if lnc.ch == nil {
			continue
		}

		select {
			case lnc.ch <- n:
			default:
				close(lnc.ch)
				lnc.ch = nil
		}
	}
}

func (l *Listener) resync(cn *ListenerConn, notificationChan <-chan Notification) error {
	doneChan := make(chan error)
	go func() {
		for relname := range l.channels {
			err := cn.Listen(relname)
			if err != nil {
				doneChan <- err
				return
			}
		}
		close(doneChan)
	}()

	for {
		// Ignore notifications to avoid deadlocks; we'll broadcast a Reconnect
		// anyway.
		select {
			case _ = <-notificationChan:

			case err, ok := <-doneChan:
				if ok {
					return nil
				}
				return err
		}
	}
}

func (l *Listener) connect() {
	var notificationChan chan Notification
	var cn *ListenerConn
	var err error

	for {
		notificationChan = make(chan Notification, 32)
		for {
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

	l.startListenRequestWorker()
	l.cn = cn
	l.notificationChan = notificationChan
	l.broadcast(ListenerPidReconnect)
	l.lock.Unlock()
}

// caller must be holding l.lock
func (l *Listener) startListenRequestWorker() {
	l.listenRequestWorkerWG = sync.WaitGroup{}
	l.listenRequestWorkerWG.Add(1)
	l.listenRequestChan = make(chan listenRequest, 32)
	l.listenRequestWorkerSignaler = make(chan bool, 1)

	go l.listenRequestWorkerMain()
}

func (l *Listener) killListenRequestWorker() {
	select {
		case l.listenRequestWorkerSignaler <- true:

		// sanity check
		default:
			panic("listenRequestWorkerSignaler channel full")
	}
}

func (l *Listener) listenRequestWorkerMain() {
	for {
		select {
			case request := <-l.listenRequestChan:
				var err error
				if !request.unlisten {
					err = l.cn.Listen(request.relname)
				} else {
					err = l.cn.Unlisten(request.relname)
				}
				//TODO
				if err != nil {
					panic(err)
				}

				n := Notification{
					BePid: ListenerPidOpen,
					RelName: request.relname,
				}
				l.dispatch(n)

			case shouldDie := <-l.listenRequestWorkerSignaler:
				if !shouldDie {
					panic("shouldDie == false")
				}
				goto die
		}
	}
die:
	l.listenRequestWorkerWG.Done()
}

func (l *Listener) Close() {
	l.closed = true
}

func (l *Listener) dispatch(n Notification) {
	l.lock.Lock()
	defer l.lock.Unlock()

	m, ok := l.channels[n.RelName]
	if !ok {
		// no listeners, try and UNLISTEN
		err := l.requestListen(n.RelName, true)
		if err != nil && err != ErrBufferFull {
			// shouldn't happen
			panic(err)
		}
		return
	}

	// sanity check; no empty maps should appear in channels
	if len(m) == 0 {
		panic("len(m) == 0")
	}

	for _, lnc := range m {
		if lnc.ch == nil {
			continue
		}

		select {
			case lnc.ch <- n:

			default:
				// Glock the channel
				close(lnc.ch)
				lnc.ch = nil
		}
	}
}

func (l *Listener) listenerMain() {
	for {
		l.connect()

		for {
			notification, ok := <-l.notificationChan
			if !ok {
				// lost connection, loop again
				break
			}
			l.dispatch(notification)
		}

		if l.closed {
			return
		}

		l.disconnectCleanup()
	}
}


