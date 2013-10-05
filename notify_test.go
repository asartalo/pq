package pq

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

func expectNotification(t *testing.T, ch <-chan Notification, relname string, extra string) error {
	select {
		case n := <-ch:
			if n.RelName != relname || n.Extra != extra {
				return fmt.Errorf("unexpected notification %v", n)
			}
			return nil
		case <-time.After(500 * time.Millisecond):
			return fmt.Errorf("timeout")
	}
	panic("not reached")
}

func expectNoNotification(t *testing.T, ch <-chan Notification) error {
	select {
		case n := <-ch:
			return fmt.Errorf("unexpected notification %v", n)
		case <-time.After(500 * time.Millisecond):
			return nil
	}
	panic("not reached")
}

func expectEvent(t *testing.T, l *Listener, et ListenerEventType) error {
	select {
	case e := <-l.Event:
		if e.Event != et {
			return fmt.Errorf("unexpected event %v", e)
		}
		return nil
	case <-time.After(500 * time.Millisecond):
		return fmt.Errorf("timeout")
	}
	panic("not reached")
}

func expectNoEvent(t *testing.T, l *Listener) error {
	select {
	case e := <-l.Event:
		return fmt.Errorf("unexpected event %v", e)
	case <-time.After(500 * time.Millisecond):
		return nil
	}
	panic("not reached")
}

func newTestListenerConn(t *testing.T) (*ListenerConn, <-chan Notification) {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	notificationChan := make(chan Notification)
	l, err := NewListenerConn("", notificationChan)
	if err != nil {
		t.Fatal(err)
	}

	return l, notificationChan
}

func TestNewListenerConn(t *testing.T) {
	l, _ := newTestListenerConn(t)

	defer l.Close()
}

func TestConnListen(t *testing.T) {
	l, channel := newTestListenerConn(t)

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	ok, err := l.Listen("notify_test")
	if !ok || err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test")
	if err != nil {
		t.Fatal(err)
	}

	err = expectNotification(t, channel, "notify_test", "")
	if err != nil {
		t.Fatal(err)
	}
}

func TestConnUnlisten(t *testing.T) {
	l, channel := newTestListenerConn(t)

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	ok, err := l.Listen("notify_test")
	if !ok || err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test")

	err = expectNotification(t, channel, "notify_test", "")
	if err != nil {
		t.Fatal(err)
	}

	ok, err = l.Unlisten("notify_test")
	if !ok || err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test")
	if err != nil {
		t.Fatal(err)
	}

	err = expectNoNotification(t, channel)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNotifyExtra(t *testing.T) {
	l, channel := newTestListenerConn(t)

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	ok, err := l.Listen("notify_test")
	if !ok || err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test, 'something'")
	if err != nil {
		t.Fatal(err)
	}

	err = expectNotification(t, channel, "notify_test", "something")
	if err != nil {
		t.Fatal(err)
	}
}

// create a new test listener and also set the timeouts
func newTestListenerTimeout(t *testing.T, min time.Duration, max time.Duration) (*Listener) {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	l := NewListener("", min, max)
	err := expectEvent(t, l, ListenerEventConnected)
	if err != nil {
		t.Fatal(err)
	}
	return l
}


func newTestListener(t *testing.T) (*Listener) {
	return newTestListenerTimeout(t, time.Hour, time.Hour)
}


func TestListenerListen(t *testing.T) {
	l := newTestListener(t)
	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	err = expectNotification(t, l.Notify, "notify_listen_test", "")
	if err != nil {
		t.Fatal(err)
	}
}

func TestListenerUnlisten(t *testing.T) {
	l := newTestListener(t)
	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	err = l.Unlisten("notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	err = expectNotification(t, l.Notify, "notify_listen_test", "")
	if err != nil {
		t.Fatal(err)
	}

    _, err = db.Exec("NOTIFY notify_listen_test")
    if err != nil {
        t.Fatal(err)
    }

	err = expectNoNotification(t, l.Notify)
	if err != nil {
		t.Fatal(err)
	}
}

func TestListenerFailedQuery(t *testing.T) {
	l := newTestListener(t)
	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	err = expectNotification(t, l.Notify, "notify_listen_test", "")
	if err != nil {
		t.Fatal(err)
	}

	// shouldn't cause a disconnect
	ok, err := l.cn.ExecSimpleQuery("SELECT error")
	if !ok {
		t.Fatalf("could not send query to server: %s", err)
	}
	_, ok = err.(PGError)
	if !ok {
		t.Fatalf("unexpected error %s", err)
	}
	err = expectNoEvent(t, l)
	if err != nil {
		t.Fatal(err)
	}

	// should still work
    _, err = db.Exec("NOTIFY notify_listen_test")
    if err != nil {
        t.Fatal(err)
    }

	err = expectNotification(t, l.Notify, "notify_listen_test", "")
	if err != nil {
		t.Fatal(err)
	}
}

func TestListenerReconnect(t *testing.T) {
	l := newTestListenerTimeout(t, time.Millisecond, time.Hour)
	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_listen_test")
	if err != nil {
		t.Fatal(err)
	}

	err = expectNotification(t, l.Notify, "notify_listen_test", "")
	if err != nil {
		t.Fatal(err)
	}

	// kill the query and make sure it comes back up
	ok, err := l.cn.ExecSimpleQuery("SELECT pg_terminate_backend(pg_backend_pid())")
	if ok {
		t.Fatalf("could not kill the connection: %s", err)
	}
	if err != io.EOF {
		t.Fatalf("unexpected error %s", err)
	}
	err = expectEvent(t, l, ListenerEventDisconnected)
	if err != nil {
		t.Fatal(err)
	}
	err = expectEvent(t, l, ListenerEventReconnected)
	if err != nil {
		t.Fatal(err)
	}

	// should still work
    _, err = db.Exec("NOTIFY notify_listen_test")
    if err != nil {
        t.Fatal(err)
    }

	err = expectNotification(t, l.Notify, "notify_listen_test", "")
	if err != nil {
		t.Fatal(err)
	}
}
