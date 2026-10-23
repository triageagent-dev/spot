package triggers

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func tokenFile(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(s+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func wait(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); !ok(); {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStreamDeliversWhatChangedAndCoalesces(t *testing.T) {
	var auth, query atomic.Value
	send := make(chan string, 8)
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		query.Store(r.URL.Path + "?" + r.URL.RawQuery)
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"hello":true}`))
		for m := range send {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(m))
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(send) })

	c, err := New(srv.URL, "example", tokenFile(t, "secret-token"), quiet())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	wait(t, "hello", c.Connected)
	if p := c.Take(); !p.Hello || p.Issues {
		t.Fatalf("first pending %+v", p)
	}
	if got := auth.Load(); got != "Bearer secret-token" {
		t.Fatalf("authorization %v", got)
	}
	if got := query.Load(); got != Path+"?tenant=example" {
		t.Fatalf("request %v", got)
	}

	send <- `{"issues":true}`
	send <- `{"rooms":["team"]}`
	send <- `{"rooms":["team","agents"]}`
	wait(t, "frames", func() bool {
		p := c.Take()
		if !p.Issues {
			// Take clears, so put back what a partial read took.
			c.add(frame{Rooms: p.Rooms})
			return false
		}
		return len(p.Rooms) == 2
	})
	if !c.Take().Empty() {
		t.Fatal("Take must clear")
	}
}

func TestStreamReconnectsAndIsNotConnectedWhileDown(t *testing.T) {
	var conns atomic.Int32
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := conns.Add(1)
		if n == 1 {
			http.Error(w, "no", http.StatusForbidden)
			return
		}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"hello":true}`))
		if n == 2 {
			conn.Close() // drops once
			return
		}
		defer conn.Close()
		time.Sleep(time.Second)
	}))
	t.Cleanup(srv.Close)
	c, err := New(strings.Replace(srv.URL, "http", "http", 1), "example", tokenFile(t, "t"), quiet())
	if err != nil {
		t.Fatal(err)
	}
	if c.Connected() {
		t.Fatal("connected before it dialled")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	wait(t, "a third connection", func() bool { return conns.Load() >= 3 })
	wait(t, "connected again", c.Connected)
}

func TestNewRefusesAnythingButHTTP(t *testing.T) {
	for _, u := range []string{"", "ftp://x", "core.svc:8080"} {
		if _, err := New(u, "t", "f", quiet()); err == nil {
			t.Errorf("%q accepted", u)
		}
	}
}
