package pq

import (
	"os"
	"testing"
	"time"
)

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

	err := l.Listen("notify_test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test")
	if err != nil {
		t.Fatal(err)
	}

	n := <-channel

	if n.RelName != "notify_test" {
		t.Errorf("Notification RelName invalid: %v", n.RelName)
	}
}

func TestConnUnlisten(t *testing.T) {
	l, channel := newTestListenerConn(t)

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_test")
	if err != nil {
		t.Fatal(err)
	}

	l.Unlisten("notify_test")

	_, err = db.Exec("NOTIFY notify_test")
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-channel:
		t.Fatal("Got notification after Unlisten")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestNotifyExtra(t *testing.T) {
	l, channel := newTestListenerConn(t)

	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	err := l.Listen("notify_test")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test, 'something'")
	if err != nil {
		t.Fatal(err)
	}

	n := <-channel
	if n.Extra != "something" {
		t.Errorf("Notification extra invalid: %v", n.Extra)
	}
}

func newTestListener(t *testing.T) (*Listener) {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	l, err := NewListener("")
	if err != nil {
		t.Fatal(err)
	}

	return l
}


func TestListenerListen(t *testing.T) {
	var n Notification

	l := newTestListener(t)
	defer l.Close()

	db := openTestConn(t)
	defer db.Close()

	channel := make(chan Notification, 2)
	time.Sleep(3 * time.Second)
	err := l.Listen("notify_test", channel)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("NOTIFY notify_test")
	if err != nil {
		t.Fatal(err)
	}

	select {
		case n = <-channel:

		case <-time.After(5 * time.Second):
			panic("timeout")
	}

	if n.RelName != "notify_test" {
		t.Errorf("Notification RelName invalid: %v", n.RelName)
	}
}

