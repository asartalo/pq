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

func (l *ListenerConn) listenerConnMain() {
	for {
		t, r, err := l.cn.recvMessage()
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

type listenChannel struct {
	relname string
	open bool
	clients map[chan<- Notification] *listenClient
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
	channels map[string] *listenChannel
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
		channels: make(map[string] *listenChannel),
	}

	go l.listenerMain()

	return l, nil
}

func (l *Listener) requestListen(relname string, unlisten bool) bool {
	request := listenRequest{
		relname: relname,
		unlisten: unlisten,
	}
	select {
		case l.listenRequestChan <-request:
			return true
		default:
			return false
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

	channel, ok := l.channels[relname]
	if ok {
		// The channel exists, so we're either listening on it or about to.
		// If the channel is open and the connection is presumed to still be
		// alive, it's safe for the caller to assume it's listening on the
		// channel.  In any other case, we have to tell the client it's not
		// listening yet.
		channel.clients[c] = lnc
		if l.cn != nil && channel.open {
			return true, nil
		}
		return false, nil
	}

	// Try and listen on the channel.  If that fails, there's nothing more we
	// can (or should) do; the caller will have to retry later.  However, if
	// there's no connection, there's no need to try that; just add the channel
	// and the resync logic will take care the rest.
	if l.cn != nil && !l.requestListen(relname, false) {
		return false, ErrBufferFull
	}

	channel = &listenChannel{
		relname: relname,
		open: false,
		clients: make(map[chan<- Notification] *listenClient, 1),
	}
	channel.clients[c] = lnc
	l.channels[relname] = channel

	return false, nil
}

func (l *Listener) Unlisten(relname string, c chan<- Notification) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	channel, ok := l.channels[relname]
	if !ok {
		// already gone, nothing to do
		return nil
	}

	delete(channel.clients, c)
	if len(channel.clients) == 0 {
		if l.requestListen(relname, true) {
			delete(l.channels, relname)
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
				l.removeListenClient(lnc)
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
		// Ignore notifications while the synchronization is going on to avoid
		// deadlocks; we'll broadcast a Reconnect for every channel anyway.
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

func (l *Listener) connect() bool {
	var notificationChan chan Notification
	var cn *ListenerConn
	var err error

	for {
		notificationChan = make(chan Notification, 32)
		for {
			l.lock.Lock()
			if l.closed {
				return false
			}
			l.lock.Unlock()

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

	return true
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
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.cn != nil {
		l.cn.Close()
	}
	l.closed = true
}

func (l *Listener) removeListenClient(lnc *listenClient) {
	for relname := range(lnc.relnames) {
		// sanity check
		_, ok := l.channels[relname].clients[lnc.ch]
		if !ok {
			panic("missing entry")
		}
		delete(l.channels[relname].clients, lnc.ch)
	}
	lnc.relnames = nil
	delete(l.listenClients, lnc.ch)
	close(lnc.ch)
	lnc.ch = nil
}

func (l *Listener) dispatch(n Notification) {
	l.lock.Lock()
	defer l.lock.Unlock()

	channel, ok := l.channels[n.RelName]
	if !ok {
		// A NOTIFY was queued for a channel we've (or are about to) issue an
		// UNLISTEN for; no problem
		return
	}

	if len(channel.clients) == 0 {
		// no listeners, try and UNLISTEN
		if l.requestListen(n.RelName, true) {
			delete(l.channels, n.RelName)
		}
		return
	}

	// If the channel was recently opened, mark it as such.  We could do this
	// for "real" notifications, too, but let's keep the semantics strict right
	// now.
	if n.BePid == ListenerPidOpen {
		channel.open = true
	}

	for _, lnc := range channel.clients {
		select {
			case lnc.ch <- n:

			default:
				l.removeListenClient(lnc)
		}
	}
}

// XXX remove?
func (l *Listener) ListenChannels() int {
	l.lock.Lock()
	defer l.lock.Unlock()

	return len(l.channels)
}

func (l *Listener) listenerMain() {
	for {
		if !l.connect() {
			break
		}

		for {
			notification, ok := <-l.notificationChan
			if !ok {
				// lost connection, loop again
				break
			}
			l.dispatch(notification)
		}

		if l.closed {
			break
		}

		l.disconnectCleanup()
	}
}


