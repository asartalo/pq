// Package pq is a pure Go Postgres driver for the database/sql package.
// This module contains support for Postgres LISTEN/NOTIFY.
package pq

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

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

type Notification struct {
	BePid   int
	RelName string
	Extra   string
}

type ListenerConn struct {
	cn        *conn
	closed    bool
	err error

	notificationChan chan<- Notification

	// guards only sending/closing of senderToken
	lock      *sync.Mutex
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
		closed: false,
		notificationChan: notificationChan,
		lock: new(sync.Mutex),
		senderToken: make(chan bool, 1),
		replyChan: make(chan message, 1),
	}
	// summon the token
	l.releaseToken()

	go l.listenerConnMain()

	return l, nil
}

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
	// have closed the channel, and we can not try and release the token; this
	// is necessary because send on a closed channel would panic.  But it's not
	// a huge problem since we need to serialize access to releaseToken()
	// anyway (as there's only one token).
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
			n := recvNotification(r)
			l.notificationChan <- n
		case 'Z', 'E':
			l.replyChan <- message{t, r}
		case 'C':
			// ignore
		case 'T', 'N', 'S', 'D':
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
	l.closed = true
	return l.cn.Close()
}
