package corecli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
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

func TestParseRoomShowsAnAttachment(t *testing.T) {
	m, _, err := ParseRoom(`{"messages":[{"id":"$i","sender":"@u.alice:s","body":"shot.png","ts":5,"file":{"name":"shot.png","mime":"image/png","size":9}},{"id":"$t","sender":"@u.alice:s","body":"hi","ts":6}]}`, "")
	if err != nil || len(m) != 2 || m[0].File != "image/png" || m[0].FileName != "shot.png" || m[1].File != "" {
		t.Fatalf("%+v %v", m, err)
	}
}
