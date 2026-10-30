package corecli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type fakeCore struct {
	mu        sync.Mutex
	auth      []string
	upserts   []map[string]any
	issuesOut string
	withRoom  bool
	cell      string // the content of the memory cell "cell-1"; empty = no such cell
	roomArgs  []map[string]any
}

func (f *fakeCore) start(t *testing.T) *httptest.Server {
	t.Helper()
	s := server.NewMCPServer("fake-core", "1")
	s.AddTool(mcp.NewTool("list_open_issues", mcp.WithString("tenant"), mcp.WithNumber("limit")),
		func(_ context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if r.GetArguments()["tenant"] != "example" {
				return mcp.NewToolResultError("wrong tenant"), nil
			}
			return mcp.NewToolResultText(f.issuesOut), nil
		})
	s.AddTool(mcp.NewTool("upsert_memory_item", mcp.WithString("id"), mcp.WithString("kind"), mcp.WithString("scope"),
		mcp.WithString("content"), mcp.WithString("title"), mcp.WithString("tags"), mcp.WithString("stale_after"), mcp.WithString("tenant")),
		func(_ context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.upserts = append(f.upserts, r.GetArguments())
			return mcp.NewToolResultText(`{"id":"x","status":"active"}`), nil
		})
	s.AddTool(mcp.NewTool("get_memory_cell", mcp.WithString("id")),
		func(_ context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if r.GetArguments()["id"] != "cell-1" || f.cell == "" {
				return mcp.NewToolResultError("memory cell not found"), nil
			}
			b, _ := json.Marshal(map[string]any{"id": "cell-1", "kind": "spot_system_identity", "content": f.cell})
			return mcp.NewToolResultText(string(b)), nil
		})
	if f.withRoom {
		s.AddTool(mcp.NewTool("read_workspace_room", mcp.WithString("tenant"), mcp.WithString("room"), mcp.WithNumber("since_ts")),
			func(_ context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				f.mu.Lock()
				f.roomArgs = append(f.roomArgs, r.GetArguments())
				f.mu.Unlock()
				return mcp.NewToolResultText(`{"messages":[{"id":"m1","sender":"alice","text":"hi","ts":1792672900123}]}`), nil
			})
	}
	h := server.NewStreamableHTTPServer(s)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auth = append(f.auth, r.Header.Get("Authorization"))
		f.mu.Unlock()
		h.ServeHTTP(w, r)
	}))
}

func tokenFile(t *testing.T, v string) string {
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(v+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestListOpenIssuesAndHeartbeatOverMCP(t *testing.T) {
	f := &fakeCore{issuesOut: `{"count":1,"issues":[{"id":"i1","severity":"critical","status":"new","summary":"boom","reopenCount":2,"comments":3}]}`}
	srv := f.start(t)
	defer srv.Close()
	tf := tokenFile(t, "tok-1")
	c := New(srv.URL, tf, "example")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	issues, err := c.ListOpenIssues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].ID != "i1" || issues[0].ReopenCount != 2 || issues[0].Comments != 3 {
		t.Fatalf("issues: %+v", issues)
	}

	if err := c.Heartbeat(ctx, "system2-heartbeat-example", "hb", "alive", 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	up := f.upserts[0]
	f.mu.Unlock()
	if up["id"] != "system2-heartbeat-example" || up["kind"] != "note" || up["stale_after"] != "30m0s" || up["tenant"] != "example" {
		t.Fatalf("upsert args: %v", up)
	}

	// The token is read from the file on each request: rotate it.
	if err := os.WriteFile(tf, []byte("tok-2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListOpenIssues(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.auth[0] != "Bearer tok-1" || f.auth[len(f.auth)-1] != "Bearer tok-2" {
		t.Fatalf("authorization headers: %v", f.auth)
	}
}

func TestRoomToolAbsentIsTolerated(t *testing.T) {
	f := &fakeCore{}
	srv := f.start(t)
	defer srv.Close()
	c := New(srv.URL, tokenFile(t, "t"), "example")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, _, err := c.RoomMessages(ctx, "team", ""); err != ErrNoRoomTool {
		t.Fatalf("want ErrNoRoomTool, got %v", err)
	}
}

func TestRoomToolPresent(t *testing.T) {
	f := &fakeCore{withRoom: true}
	srv := f.start(t)
	defer srv.Close()
	c := New(srv.URL, tokenFile(t, "t"), "example")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msgs, next, err := c.RoomMessages(ctx, "team", "")
	if err != nil || len(msgs) != 1 || msgs[0].Sender != "alice" || next != "1792672900123" {
		t.Fatalf("msgs=%+v next=%q err=%v", msgs, next, err)
	}
	// The cursor goes back to the core as the number since_ts, not as "since".
	if _, _, err := c.RoomMessages(ctx, "team", next); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, has := f.roomArgs[0]["since_ts"]; has {
		t.Fatalf("first read must not send a cursor: %v", f.roomArgs[0])
	}
	if got, ok := f.roomArgs[1]["since_ts"].(float64); !ok || int64(got) != 1792672900123 {
		t.Fatalf("second read args: %v", f.roomArgs[1])
	}
	if _, has := f.roomArgs[1]["since"]; has {
		t.Fatalf("the old since argument must be gone: %v", f.roomArgs[1])
	}
}

func TestParseRoomShapes(t *testing.T) {
	m, next, err := ParseRoom(`[{"event_id":"e2","from":"bob","body":"x","created_at":"t"}]`, "e1")
	if err != nil || len(m) != 1 || m[0].Sender != "bob" || m[0].Text != "x" || next != "e2" {
		t.Fatalf("%+v %q %v", m, next, err)
	}
	m, next, _ = ParseRoom(`{"messages":[],"next":"c9"}`, "c1")
	if len(m) != 0 || next != "c9" {
		t.Fatalf("%+v %q", m, next)
	}
	if _, _, err := ParseRoom(`oops`, ""); err == nil {
		t.Fatal("garbage must error")
	}
}

func TestUnreachableCoreErrors(t *testing.T) {
	c := New("http://127.0.0.1:1/mcp", tokenFile(t, "t"), "example")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.ListOpenIssues(ctx); err == nil {
		t.Fatal("expected an error")
	}
}

// The shape the core's read_workspace_room really returns (ts is a number in
// milliseconds, the cursor is the newest ts, and it is sent back as since_ts).
func TestParseRoomCoreShape(t *testing.T) {
	reply := `{"room":"team","messages":[` +
		`{"id":"$a","sender":"@u.alice:srv","name":"alice","body":"hey","ts":1792672900123},` +
		`{"id":"$b","sender":"@u.spot.t:srv","name":"t-spot","body":"hi","ts":1792672901123,"spot":"t-spot","hop":0},` +
		`{"id":"$c","sender":"@tria:srv","name":"Tria","body":"ok","ts":1792672902123,"tria":true}]}`
	m, next, err := ParseRoom(reply, "")
	if err != nil || len(m) != 3 {
		t.Fatalf("%+v %v", m, err)
	}
	if next != "1792672902123" || m[0].TS != 1792672900123 || m[0].Name != "alice" || m[1].Spot != "t-spot" || !m[2].Tria {
		t.Fatalf("next=%q msgs=%+v", next, m)
	}
	// An empty read keeps the cursor.
	if _, next, _ := ParseRoom(`{"room":"team","messages":[]}`, "1792672902123"); next != "1792672902123" {
		t.Fatalf("empty read moved the cursor to %q", next)
	}
}

func TestParseRoomCarriesTheCoresAddressee(t *testing.T) {
	m, _, err := ParseRoom(`{"messages":[`+
		`{"id":"$a","sender":"@u.alice:s","body":"@other hi","ts":1,"mentions":["other"],"forYou":false},`+
		`{"id":"$b","sender":"@u.alice:s","body":"hi all","ts":2,"mentions":[],"forYou":true},`+
		`{"id":"$c","sender":"@u.alice:s","body":"old core","ts":3}]}`, "")
	if err != nil || len(m) != 3 {
		t.Fatalf("%+v %v", m, err)
	}
	if len(m[0].Mentions) != 1 || m[0].Mentions[0] != "other" || m[0].ForYou == nil || *m[0].ForYou {
		t.Fatalf("addressed to another spot: %+v", m[0])
	}
	if len(m[1].Mentions) != 0 || m[1].ForYou == nil || !*m[1].ForYou {
		t.Fatalf("for nobody, this spot answers: %+v", m[1])
	}
	if m[2].ForYou != nil || m[2].Mentions != nil {
		t.Fatalf("a core without the fields must leave them unset: %+v", m[2])
	}
}

func TestParseRoomShowsAnAttachment(t *testing.T) {
	m, _, err := ParseRoom(`{"messages":[{"id":"$i","sender":"@u.alice:s","body":"shot.png","ts":5,"file":{"name":"shot.png","mime":"image/png","size":9}},{"id":"$t","sender":"@u.alice:s","body":"hi","ts":6}]}`, "")
	if err != nil || len(m) != 2 || m[0].File != "image/png" || m[0].FileName != "shot.png" || m[1].File != "" {
		t.Fatalf("%+v %v", m, err)
	}
}

func TestMemoryCellContent(t *testing.T) {
	f := &fakeCore{cell: "You are {{NAME}}."}
	srv := f.start(t)
	defer srv.Close()
	c := New(srv.URL, tokenFile(t, "tok-1"), "example")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	got, err := c.MemoryCellContent(ctx, "cell-1")
	if err != nil || got != "You are {{NAME}}." {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := c.MemoryCellContent(ctx, "nope"); err == nil {
		t.Error("an unknown cell must be an error")
	}
	f.mu.Lock()
	f.cell = "  \n"
	f.mu.Unlock()
	if _, err := c.MemoryCellContent(ctx, "cell-1"); err == nil {
		t.Error("an empty cell must be an error")
	}
}

func TestSelectNotesPutsRulesFirstAndFindsAHoldRule(t *testing.T) {
	items := []Note{
		{ID: "n1", Title: "Status update", Content: "all healthy", Tags: []string{"spot-note", "spot:s"}, Updated: "2026-10-27T14:00:00Z"},
		{ID: "n2", Title: "Status later", Content: "still healthy", Tags: []string{"spot-note", "spot:s"}, Updated: "2026-10-27T15:00:00Z"},
		{ID: "r1", Title: "Operational Rule: No autonomous checks without human request", Content: `Instruction from a person: "next wakeup dont check any services or statuses before I aks".`, Tags: []string{"spot-note", "spot:s"}, Updated: "2026-10-27T15:03:00Z"},
		{ID: "x", Title: "Another spot's note", Content: "not mine", Tags: []string{"spot-note", "spot:other"}},
		{ID: "n1", Title: "Status update", Content: "dup", Tags: []string{"spot-note", "spot:s"}},
	}
	lines, hold := SelectNotes(items, "s")
	if !hold {
		t.Error("a rule saying not to check before being asked is a hold rule")
	}
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "RULE: ") || !strings.Contains(lines[1], "still healthy") || !strings.Contains(lines[2], "all healthy") {
		t.Fatalf("lines: %q", lines)
	}
	_, hold = SelectNotes([]Note{{ID: "r", Title: "Rule", Content: "always say hello", Tags: []string{"spot-rule"}}}, "s")
	if hold {
		t.Error("a rule that holds nothing back is not a hold rule")
	}
	_, hold = SelectNotes([]Note{{ID: "r", Title: "Pause", Content: "stand by", Tags: []string{"spot-rule", "spot-hold"}}}, "s")
	if !hold {
		t.Error("the spot-hold tag is a hold rule")
	}
}

func TestARuleTheSpotRewroteKeepsOnlyTheCoresStampAndStillHolds(t *testing.T) {
	items := []Note{{
		ID: "r1", Title: "Operational Rule: Zero infra checks or reactions without human request",
		Content: "Instruction from a person: 'zero check on infra, just wait for my command'. On wake (startup, tick, edge, or human), do not perform rounds, check service/cluster health, or react to infra events unless explicitly asked by a human.",
		Tags:    []string{"author/system:serviceaccount:ns:example-spot", "tenant/t1"}, Updated: "2",
	}}
	lines, hold := SelectNotes(items, "example-spot")
	if !hold || len(lines) != 1 || !strings.HasPrefix(lines[0], "RULE:") {
		t.Fatalf("hold=%v lines=%v", hold, lines)
	}
}

// A core that refuses the token is asked once; nothing more is sent until the
// token file holds another one.
func TestARefusedTokenIsNotSentAgainUntilTheFileChanges(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	}))
	defer srv.Close()
	tok := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tok, []byte("expired"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := New(srv.URL+"/mcp", tok, "example")
	defer c.Close()
	var logs bytes.Buffer
	c.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))

	_, err := c.ListOpenIssues(context.Background())
	if err == nil {
		t.Fatal("a refused token gave issues")
	}
	first := hits.Load()
	if first == 0 {
		t.Fatal("the core was never asked")
	}
	for i := 0; i < 200; i++ {
		_, err = c.ListOpenIssues(context.Background())
	}
	if !errors.Is(err, ErrCredentialRejected) {
		t.Fatalf("err = %v, want ErrCredentialRejected", err)
	}
	if hits.Load() != first {
		t.Fatalf("%d requests after the refusal, want none", hits.Load()-first)
	}
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 {
		t.Fatalf("%d warnings for 201 failed calls, want 1:\n%s", n, logs.String())
	}

	// A new token is tried at once.
	if err := os.WriteFile(tok, []byte("renewed"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _ = c.ListOpenIssues(context.Background())
	if hits.Load() == first {
		t.Fatal("a changed token file was not tried")
	}
}

// A core that cannot be reached is tried less and less often, up to a cap, and
// the failures are said once a minute.
func TestAnUnreachableCoreIsRetriedWithABackoff(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "down", http.StatusBadGateway)
	}))
	defer srv.Close()
	tok := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tok, []byte("t"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := New(srv.URL+"/mcp", tok, "example")
	defer c.Close()
	now := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return now }
	var logs bytes.Buffer
	c.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))

	ask := func() { _, _ = c.ListOpenIssues(context.Background()) }
	ask()
	one := hits.Load()
	for i := 0; i < 50; i++ {
		ask() // the same instant: gated
	}
	if hits.Load() != one {
		t.Fatalf("%d requests inside the backoff", hits.Load()-one)
	}
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		now = now.Add(want)
		before := hits.Load()
		ask()
		if hits.Load() == before {
			t.Fatalf("no attempt after %s", want)
		}
	}
	// The wait is capped.
	for i := 0; i < 30; i++ {
		now = now.Add(retryMax)
		ask()
	}
	if got := c.retryAt.Sub(now); got != retryMax {
		t.Fatalf("the backoff is %s, want the cap %s", got, retryMax)
	}
	if n := strings.Count(logs.String(), "level=WARN"); n < 1 || n > 33 {
		t.Fatalf("%d warnings:\n%s", n, logs.String())
	}
}
