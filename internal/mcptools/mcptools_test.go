package mcptools

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/mark3labs/mcp-go/server/servertest"
)

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func names(ts []Tool) []string {
	var out []string
	for _, t := range ts {
		out = append(out, t.Name)
	}
	return out
}

func TestToolsAreListedAndSorted(t *testing.T) {
	s := server.NewMCPServer("example", "1")
	for _, n := range []string{"c", "a", "b"} {
		s.AddTool(mcp.NewTool(n, mcp.WithDescription("tool "+n)), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("ok"), nil
		})
	}
	ts := servertest.NewTestStreamableHTTPServer(s)
	defer ts.Close()

	c, err := Connect(ctx(t), ts.URL, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.Tools(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(names(got), want) {
		t.Fatalf("tools = %v, want %v", names(got), want)
	}
	if got[0].Description != "tool a" {
		t.Errorf("description: %q", got[0].Description)
	}
}

func TestConnectUnreachable(t *testing.T) {
	if _, err := Connect(ctx(t), "http://127.0.0.1:1/mcp", "", time.Second); err == nil {
		t.Fatal("an unreachable server must be an error")
	}
}

// A server whose tool schemas the typed client cannot parse (a "type" that is
// a list) still lists, with the schema as the server wrote it, and still calls.
func TestSchemasTheTypedClientCannotParseSurviveUnchanged(t *testing.T) {
	const odd = `{"type":"object","properties":{"x":{"type":["string","null"]}}}`
	s := server.NewMCPServer("example", "1")
	s.AddTool(mcp.NewToolWithRawSchema("a", "d", json.RawMessage(odd)),
		func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("x=" + req.GetString("x", "none")), nil
		})
	s.AddTool(mcp.NewToolWithRawSchema("c", "d", json.RawMessage(`{"type":["object"]}`)),
		func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("ok"), nil
		})
	ts := servertest.NewTestStreamableHTTPServer(s)
	defer ts.Close()
	c, err := Connect(ctx(t), ts.URL, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.Tools(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names(got), []string{"a", "c"}) {
		t.Fatalf("tools = %v", names(got))
	}
	var want, have any
	_ = json.Unmarshal([]byte(odd), &want)
	_ = json.Unmarshal(got[0].InputSchema, &have)
	if !reflect.DeepEqual(want, have) {
		t.Errorf("schema = %s", got[0].InputSchema)
	}
	r, err := c.Call(ctx(t), "a", json.RawMessage(`{"x":"hi"}`))
	if err != nil || r.IsError || r.Text != "x=hi" {
		t.Fatalf("call: %+v %v", r, err)
	}
}

func TestCallResults(t *testing.T) {
	s := server.NewMCPServer("example", "1")
	h := func(f func() *mcp.CallToolResult) server.ToolHandlerFunc {
		return func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) { return f(), nil }
	}
	s.AddTool(mcp.NewTool("two"), h(func() *mcp.CallToolResult {
		return &mcp.CallToolResult{Content: []mcp.Content{mcp.NewTextContent("one"), mcp.NewTextContent("two")}}
	}))
	s.AddTool(mcp.NewTool("err"), h(func() *mcp.CallToolResult { return mcp.NewToolResultError("nope") }))
	s.AddTool(mcp.NewTool("img"), h(func() *mcp.CallToolResult { return mcp.NewToolResultImage("cap", "aGVsbG8=", "image/png") }))
	s.AddTool(mcp.NewTool("structured"), h(func() *mcp.CallToolResult {
		return &mcp.CallToolResult{StructuredContent: map[string]any{"n": 1}}
	}))
	ts := servertest.NewTestStreamableHTTPServer(s)
	defer ts.Close()
	c, err := Connect(ctx(t), ts.URL, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if r, err := c.Call(ctx(t), "two", nil); err != nil || r.Text != "one\ntwo" || r.IsError {
		t.Errorf("two: %+v %v", r, err)
	}
	if r, err := c.Call(ctx(t), "err", nil); err != nil || r.Text != "nope" || !r.IsError {
		t.Errorf("err: %+v %v", r, err)
	}
	if r, err := c.Call(ctx(t), "img", nil); err != nil || !strings.Contains(r.Text, "cap") || !strings.Contains(r.Text, "[image content omitted: image/png") {
		t.Errorf("img: %+v %v", r, err)
	}
	if r, err := c.Call(ctx(t), "structured", nil); err != nil || r.Text != `{"n":1}` {
		t.Errorf("structured: %+v %v", r, err)
	}
	// A call the server rejects at the protocol level is an error, not a result.
	if _, err := c.Call(ctx(t), "nonexistent", nil); err == nil {
		t.Error("an unknown tool is a protocol error")
	}
}

func TestTokenFileIsReadOnEveryRequest(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	s := server.NewMCPServer("example", "1")
	s.AddTool(mcp.NewTool("a"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("ok"), nil
	})
	ts := servertest.NewTestStreamableHTTPServer(s, server.WithHTTPContextFunc(func(c context.Context, r *http.Request) context.Context {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		return c
	}))
	defer ts.Close()
	tok := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tok, []byte("one\n"), 0o600)
	c, err := Connect(ctx(t), ts.URL, tok, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = os.WriteFile(tok, []byte("two\n"), 0o600)
	if _, err := c.Call(ctx(t), "a", nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 2 || seen[0] != "Bearer one" || seen[len(seen)-1] != "Bearer two" {
		t.Fatalf("authorization headers: %v", seen)
	}
}

func TestNoTokenFileSendsNoAuthorization(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	s := server.NewMCPServer("example", "1")
	ts := servertest.NewTestStreamableHTTPServer(s, server.WithHTTPContextFunc(func(c context.Context, r *http.Request) context.Context {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		return c
	}))
	defer ts.Close()
	c, err := Connect(ctx(t), ts.URL, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mu.Lock()
	defer mu.Unlock()
	for _, h := range seen {
		if h != "" {
			t.Errorf("a connector gets no credential: %q", h)
		}
	}
}
