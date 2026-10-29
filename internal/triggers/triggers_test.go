package triggers

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

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
		defer func() { _ = conn.Close() }()
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
			_ = conn.Close() // drops once
			return
		}
		defer func() { _ = conn.Close() }()
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

func TestStreamCarriesTheCoresFactsDownAndTheSpotsReportsUp(t *testing.T) {
	var mu sync.Mutex
	var got []string
	conns := make(chan *websocket.Conn, 2)
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conns <- conn
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"hello":true,"name":"example-spot","budget":{"spent":10,"cap":100,"remaining":90,"blocked":false}}`))
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				return
			}
			mu.Lock()
			got = append(got, string(b))
			mu.Unlock()
		}
	}))
	t.Cleanup(srv.Close)

	c, err := New(srv.URL, "example", tokenFile(t, "tok"), quiet())
	if err != nil {
		t.Fatal(err)
	}
	// A turn reported while the stream is down is not lost: it goes first on connect.
	c.ReportTurn(Turn{Model: "m", Input: 900, Cached: 600, Output: 100, DurationMs: 1500})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	wait(t, "hello", c.Connected)
	p := c.Take()
	if p.Name != "example-spot" || !p.Budget || p.Empty() {
		t.Fatalf("pending %+v", p)
	}
	if b, ok := c.Budget(); !ok || b.Spent != 10 || b.Cap != 100 || b.Blocked {
		t.Fatalf("budget %+v %v", b, ok)
	}
	// Facts alone are not a reason to read the core.
	if (Pending{Name: "x", Budget: true}).Wake() {
		t.Fatal("a name and a budget must not wake the supervisor")
	}
	c.ReportStats(Stats{State: "idle", Turns: 3, Wakes: map[string]int64{"chat": 2}})
	c.ReportWake(Wake{TS: 1000, Reason: "tick", Lane: "long", Outcome: "quiet", Next: 2000, NextSource: "quiet"})
	wait(t, "all reports", func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(got[0], `"turn":{"model":"m","input":900,"cached":600,"output":100,"durationMs":1500}`) ||
		!strings.Contains(got[1], `"stats":{"state":"idle","turns":3,"wakes":{"chat":2}}`) ||
		!strings.Contains(got[2], `"wake":{"ts":1000,"reason":"tick","lane":"long","outcome":"quiet","next":2000,"nextSource":"quiet"}`) {
		t.Fatalf("frames %v", got)
	}
}

// controlServer is a core that sends the frames after hello and keeps what the
// spot sends back.
func controlServer(t *testing.T, frames ...string) (url string, got func() []string) {
	t.Helper()
	var mu sync.Mutex
	var recv []string
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"hello":true}`))
		for _, f := range frames {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(f))
		}
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				return
			}
			mu.Lock()
			recv = append(recv, string(b))
			mu.Unlock()
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), recv...) }
}

func results(t *testing.T, raw []string) map[string]ControlResult {
	t.Helper()
	byID := map[string]ControlResult{}
	for _, g := range raw {
		var f struct {
			R ControlResult `json:"controlResult"`
		}
		if err := json.Unmarshal([]byte(g), &f); err != nil {
			t.Fatalf("%s: %v", g, err)
		}
		byID[f.R.ID] = f.R
	}
	return byID
}

// The core asks for an operation on a task; the spot answers once with the same
// id, shortens what it says, and refuses what it cannot do.
func TestEveryControlIsAnsweredOnceWithItsID(t *testing.T) {
	url, got := controlServer(t,
		`{"control":{"id":"1","op":"list"}}`,
		`{"control":{"id":"2","op":"stop","task":"w-3"}}`,
		`{"control":{"id":"3","op":"stop","task":"`+strings.Repeat("x", 300)+`"}}`,
		`{"control":{"op":"list"}}`,
	)
	c, err := New(url, "example", tokenFile(t, "tok"), quiet())
	if err != nil {
		t.Fatal(err)
	}
	c.SetControl(func(_ context.Context, ctl Control) ControlResult {
		if ctl.Op == "list" {
			return ControlResult{OK: true, Tasks: []TaskLine{{Key: "w-3", Title: strings.Repeat("t", 200), Lane: "task", State: "running", StartedMs: 5}}}
		}
		return ControlResult{OK: false, Msg: "refused: " + ctl.Task}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	wait(t, "three answers", func() bool { return len(got()) >= 3 })
	time.Sleep(200 * time.Millisecond) // a fourth must not come: the control without an id has nobody to answer to
	if len(got()) != 3 {
		t.Fatalf("answers: %v", got())
	}
	byID := results(t, got())
	if r := byID["1"]; !r.OK || len(r.Tasks) != 1 || len(r.Tasks[0].Title) != maxTitle {
		t.Errorf("list: %+v", r)
	}
	if r := byID["2"]; r.OK || r.Msg != "refused: w-3" {
		t.Errorf("stop: %+v", r)
	}
	if r := byID["3"]; r.OK || !strings.Contains(r.Msg, "too long") {
		t.Errorf("a key of 300 bytes: %+v", r)
	}
}

// A spot that is not answering controls yet says so at once; the core does not
// have to wait for its own timeout.
func TestAControlIsRefusedWithoutAHandler(t *testing.T) {
	url, got := controlServer(t, `{"control":{"id":"7","op":"list"}}`)
	c, err := New(url, "example", tokenFile(t, "tok"), quiet())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	wait(t, "the refusal", func() bool { return len(got()) == 1 })
	if r := results(t, got())["7"]; r.OK || !strings.Contains(r.Msg, "does not answer controls") {
		t.Fatalf("%+v", r)
	}
}

// The core asks for a wake with a frame of its own: a promise outranks a tick, and
// the words it carries are bounded.
func TestAWakeTheCoreAsksForIsPendingAndAPromiseOutranksATick(t *testing.T) {
	c, err := New("http://core.invalid", "example", tokenFile(t, "tok"), quiet())
	if err != nil {
		t.Fatal(err)
	}
	c.add(frame{Wake: &Order{Reason: "tick", Why: "first"}})
	c.add(frame{Wake: &Order{Reason: "promise", Why: strings.Repeat("é", 300)}})
	c.add(frame{Wake: &Order{Reason: "tick", Why: "later"}})
	p := c.Take()
	if p.Empty() || p.Wake() {
		t.Fatalf("an order is news for the loop, not a reason to read the core: %+v", p)
	}
	if p.Order == nil || p.Order.Reason != "promise" || len(p.Order.Why) > maxOrderWhy || !utf8.ValidString(p.Order.Why) {
		t.Fatalf("order: %+v", p.Order)
	}
	if c.Take().Order != nil {
		t.Fatal("Take must clear the order")
	}
}

// After the core's hello the spot hands over the schedule it has, once per
// connection, so a core that restarted gets it back; wake reports carry the plan
// and the promise.
func TestTheScheduleIsSentOnceAfterTheHello(t *testing.T) {
	var conns atomic.Int32
	frames := make(chan map[string]json.RawMessage, 8)
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"hello":true}`))
		if conns.Add(1) == 1 {
			// The first connection ends once the spot has answered: the second hello
			// must be answered again.
			var m map[string]json.RawMessage
			if conn.ReadJSON(&m) == nil {
				frames <- m
			}
			return
		}
		for {
			var m map[string]json.RawMessage
			if conn.ReadJSON(&m) != nil {
				return
			}
			frames <- m
		}
	}))
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, "example", tokenFile(t, "tok"), quiet())
	if err != nil {
		t.Fatal(err)
	}
	c.SetSchedule(Schedule{Next: 1000, NextWhy: "look again", NextSource: "agent", Promise: &Promise{At: 900, Why: "the note", Human: true}, BackoffMs: 5, MaxWakeMs: 7200000})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	for i := 0; i < 2; i++ {
		select {
		case m := <-frames:
			var sc Schedule
			if err := json.Unmarshal(m["schedule"], &sc); err != nil || sc.Next != 1000 || sc.NextWhy != "look again" || sc.Promise == nil || sc.Promise.At != 900 || !sc.Promise.Human || sc.BackoffMs != 5 || sc.MaxWakeMs != 7200000 {
				t.Fatalf("connection %d: %v %+v", i+1, err, string(m["schedule"]))
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no schedule on connection %d", i+1)
		}
	}
	wait(t, "the second hello", c.Connected)
	c.ReportWake(Wake{TS: 1, Reason: "tick", Outcome: "turn", Next: 2000, NextWhy: "why", Promise: &Promise{At: 1500, Why: "p"}})
	select {
	case m := <-frames:
		var w Wake
		if err := json.Unmarshal(m["wake"], &w); err != nil || w.NextWhy != "why" || w.Promise == nil || w.Promise.At != 1500 {
			t.Fatalf("wake: %v %s", err, m["wake"])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no wake report")
	}
}
