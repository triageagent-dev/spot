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
		s.AddTool(mcp.NewTool("read_workspace_room", mcp.WithString("tenant"), mcp.WithString("room"), mcp.WithString("since")),
			func(_ context.Context, r mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return mcp.NewToolResultText(`{"messages":[{"id":"m1","sender":"alice","text":"hi"}]}`), nil
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
	if err != nil || len(msgs) != 1 || msgs[0].Sender != "alice" || next != "m1" {
		t.Fatalf("msgs=%+v next=%q err=%v", msgs, next, err)
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
