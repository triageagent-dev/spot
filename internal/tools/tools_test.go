package tools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/triageagent-dev/spot/internal/llm"
)

const listSchema = `{"type":"object","properties":{"limit":{"type":["integer","null"]}},"required":[],"additionalProperties":false}`

// fakeServer serves list_open_issues (echoes its arguments), get_pods (an
// error result), slow_tool (waits), big_tool (a long text), image_tool (an
// image) and resolve_issue (a tool the spot must never get).
func fakeServer(t *testing.T) (url string, auth func() string) {
	t.Helper()
	s := server.NewMCPServer("example", "1")
	h := func(f func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error)) server.ToolHandlerFunc {
		return f
	}
	s.AddTool(mcp.NewToolWithRawSchema("list_open_issues", "Open issues.", json.RawMessage(listSchema)), h(func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		b, _ := json.Marshal(req.GetArguments())
		return mcp.NewToolResultText("args=" + string(b)), nil
	}))
	s.AddTool(mcp.NewTool("get_pods", mcp.WithDescription("Pods.")), h(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultError("forbidden: pods"), nil
	}))
	s.AddTool(mcp.NewTool("slow_tool"), h(func(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		return mcp.NewToolResultText("late"), nil
	}))
	s.AddTool(mcp.NewTool("big_tool"), h(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText(strings.Repeat("é", 100)), nil // 200 bytes
	}))
	s.AddTool(mcp.NewTool("image_tool"), h(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultImage("a chart", "aGVsbG8=", "image/png"), nil
	}))
	s.AddTool(mcp.NewTool("resolve_issue"), h(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("resolved"), nil
	}))
	var mu sync.Mutex
	var seen string
	ts := server.NewTestStreamableHTTPServer(s, server.WithHTTPContextFunc(func(ctx context.Context, r *http.Request) context.Context {
		mu.Lock()
		seen = r.Header.Get("Authorization")
		mu.Unlock()
		return ctx
	}))
	t.Cleanup(ts.Close)
	return ts.URL, func() string { mu.Lock(); defer mu.Unlock(); return seen }
}

func newRegistry(t *testing.T, url string, mut func(*Options)) *Registry {
	t.Helper()
	tok := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tok, []byte("core-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	o := Options{
		Core:        Server{Name: CoreServer, URL: url, TokenFile: tok, Allow: []string{"list_open_issues", "get_pods", "slow_tool", "big_tool", "image_tool", "resolve_issue"}, Deny: CoreToolsDenied},
		CallTimeout: 2 * time.Second, ResultMax: 100,
	}
	if mut != nil {
		mut(&o)
	}
	r := New(o)
	t.Cleanup(r.End)
	return r
}

func names(defs []llm.ToolDef) []string {
	var out []string
	for _, d := range defs {
		out = append(out, d.Name)
	}
	return out
}

func TestListPrefixesFiltersAndSorts(t *testing.T) {
	url, auth := fakeServer(t)
	r := newRegistry(t, url, nil)
	r.Begin()
	defs, err := r.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := "core__big_tool,core__get_pods,core__image_tool,core__list_open_issues,core__slow_tool,finish,schedule_next"
	if got := strings.Join(names(defs), ","); got != want {
		t.Fatalf("tools:\n got %s\nwant %s", got, want)
	}
	for _, d := range defs {
		if d.Name == "core__list_open_issues" {
			var a, b any
			_ = json.Unmarshal(d.InputSchema, &a)
			_ = json.Unmarshal([]byte(listSchema), &b)
			ab, _ := json.Marshal(a)
			bb, _ := json.Marshal(b)
			if string(ab) != string(bb) || d.Description != "Open issues." {
				t.Errorf("schema or description changed: %s %q", d.InputSchema, d.Description)
			}
		}
		if d.Name == "core__resolve_issue" {
			t.Error("a denied tool must not be offered even when it is in the allow list")
		}
	}
	if auth() != "Bearer core-token" {
		t.Errorf("authorization: %q", auth())
	}
}

func TestOnlyAllowedToolsAndAllMode(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) { o.Core.Allow = []string{"get_pods"} })
	r.Begin()
	defs, err := r.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(names(defs), ","); got != "core__get_pods,finish,schedule_next" {
		t.Fatalf("allow list: %s", got)
	}
	// A tool that was not offered cannot be called by name.
	if c, isErr := r.Call(context.Background(), "core__big_tool", nil); !isErr || !strings.Contains(c, "not in your tool list") {
		t.Errorf("call of an unlisted tool: %q %v", c, isErr)
	}

	r = newRegistry(t, url, func(o *Options) { o.Core.AllowAll, o.Core.Allow, o.Core.Deny = true, nil, nil })
	r.Begin()
	defs, _ = r.List(context.Background())
	if !strings.Contains(strings.Join(names(defs), ","), "core__resolve_issue") {
		t.Errorf("all mode offers every tool: %v", names(defs))
	}
	r = newRegistry(t, url, func(o *Options) { o.Core.AllowAll, o.Core.Allow = true, nil })
	r.Begin()
	defs, _ = r.List(context.Background())
	if strings.Contains(strings.Join(names(defs), ","), "core__resolve_issue") {
		t.Errorf("a denied tool stays denied in all mode: %v", names(defs))
	}
}

func TestCallTextErrorTruncationTimeoutAndPlaceholders(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) { o.CallTimeout = 300 * time.Millisecond })
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	c, isErr := r.Call(ctx, "core__list_open_issues", json.RawMessage(`{"limit":15}`))
	if isErr || c != `args={"limit":15}` {
		t.Errorf("text: %q %v", c, isErr)
	}
	if c, isErr = r.Call(ctx, "core__list_open_issues", nil); isErr || c != "args={}" {
		t.Errorf("no input: %q %v", c, isErr)
	}
	if c, isErr = r.Call(ctx, "core__get_pods", json.RawMessage(`{}`)); !isErr || c != "forbidden: pods" {
		t.Errorf("tool error: %q %v", c, isErr)
	}
	c, isErr = r.Call(ctx, "core__big_tool", json.RawMessage(`{}`))
	if isErr || !strings.HasSuffix(c, "[result truncated: 100 bytes dropped]") || !strings.HasPrefix(c, strings.Repeat("é", 50)) {
		t.Errorf("truncation: %q %v", c, isErr)
	}
	if c, isErr = r.Call(ctx, "core__image_tool", json.RawMessage(`{}`)); isErr || !strings.Contains(c, "[image content omitted: image/png") {
		t.Errorf("image: %q %v", c, isErr)
	}
	start := time.Now()
	if c, isErr = r.Call(ctx, "core__slow_tool", json.RawMessage(`{}`)); !isErr || !strings.Contains(c, "timed out") || time.Since(start) > 3*time.Second {
		t.Errorf("timeout: %q %v after %s", c, isErr, time.Since(start))
	}
	if c, isErr = r.Call(ctx, "nowhere__x", nil); !isErr {
		t.Errorf("unknown server: %q", c)
	}
	// A cancelled turn is an error result, not a panic or a Go error.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if c, isErr = r.Call(cctx, "core__list_open_issues", nil); !isErr {
		t.Errorf("cancelled: %q", c)
	}
}

func TestCutKeepsCharactersWhole(t *testing.T) {
	r := New(Options{ResultMax: 5, Core: Server{Name: "core"}})
	got := r.cut("aé" + "é" + "x") // a=1 byte, é=2 bytes each: cutting at 5 would split nothing; at 4 keeps "aéé"? check boundaries
	if !strings.HasPrefix(got, "aéé") || !strings.Contains(got, "truncated") {
		t.Errorf("cut: %q", got)
	}
	r = New(Options{ResultMax: 2, Core: Server{Name: "core"}})
	if got = r.cut("aéé"); !strings.HasPrefix(got, "a\n") {
		t.Errorf("a cut inside a character backs up: %q", got)
	}
	if got = r.cut("ab"); got != "ab" {
		t.Errorf("short results are untouched: %q", got)
	}
}

func TestCoreDownIsAnErrorAndAConnectorDownIsSkipped(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, "http://127.0.0.1:1/mcp", nil)
	r.Begin()
	if _, err := r.List(context.Background()); !errors.Is(err, ErrCoreDown) {
		t.Fatalf("core down: %v", err)
	}

	r = newRegistry(t, url, func(o *Options) {
		o.Others = []Server{
			{Name: "example-kube", URL: url, Allow: []string{"get_pods"}},
			{Name: "example-down", URL: "http://127.0.0.1:1/mcp", Allow: []string{"get_pods"}},
		}
	})
	r.Begin()
	defs, err := r.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(names(defs), ",")
	if !strings.Contains(got, "example-kube__get_pods") || strings.Contains(got, "example-down") {
		t.Errorf("tools: %s", got)
	}
	if d := r.Down(); len(d) != 1 || d[0] != "example-down" {
		t.Errorf("down: %v", d)
	}
	// A connector's tools are called through its own server and carry no token.
	if c, isErr := r.Call(context.Background(), "example-kube__get_pods", nil); !isErr || c != "forbidden: pods" {
		t.Errorf("connector call: %q %v", c, isErr)
	}
}

func TestScheduleNextAndFinish(t *testing.T) {
	r := New(Options{Core: Server{Name: "core"}})
	r.Begin()
	ctx := context.Background()
	if c, isErr := r.Call(ctx, ScheduleNext, json.RawMessage(`{"after":"soon","why":"x"}`)); !isErr || r.Schedule() != nil {
		t.Errorf("a bad duration is an error the model can fix: %q %v", c, isErr)
	}
	if _, isErr := r.Call(ctx, ScheduleNext, json.RawMessage(`{"after":"-5m","why":"x"}`)); !isErr {
		t.Error("a negative duration is an error")
	}
	if _, isErr := r.Call(ctx, ScheduleNext, json.RawMessage(`[1]`)); !isErr {
		t.Error("input that is not an object is an error")
	}
	if _, isErr := r.Call(ctx, ScheduleNext, json.RawMessage(`{"after":"15m","why":"check the rollout"}`)); isErr {
		t.Fatal("a good call")
	}
	if _, isErr := r.Call(ctx, ScheduleNext, json.RawMessage(`{"after":"20m","why":"later"}`)); isErr {
		t.Fatal("a second call replaces the first")
	}
	if n := r.Schedule(); n == nil || n.After != "20m" || n.Why != "later" {
		t.Errorf("schedule: %+v", n)
	}
	if _, ok := r.Finished(); ok {
		t.Error("not finished yet")
	}
	if _, isErr := r.Call(ctx, Finish, json.RawMessage(`{"summary":"  looked, all quiet "}`)); isErr {
		t.Fatal("finish")
	}
	if s, ok := r.Finished(); !ok || s != "looked, all quiet" {
		t.Errorf("finished: %q %v", s, ok)
	}
	if _, isErr := r.Call(ctx, Finish, json.RawMessage(`nope`)); !isErr {
		t.Error("finish with bad input is an error")
	}

	// End keeps what was recorded for the supervisor; Begin forgets it.
	r.End()
	if r.Schedule() == nil {
		t.Error("End must keep the schedule for the supervisor to read")
	}
	r.Begin()
	if r.Schedule() != nil {
		t.Error("Begin starts a clean turn")
	}
	if _, ok := r.Finished(); ok {
		t.Error("Begin forgets finish")
	}
}

func TestBuiltinsAreAlwaysListed(t *testing.T) {
	defs := builtinDefs()
	if len(defs) != 2 {
		t.Fatalf("%v", names(defs))
	}
	for _, d := range defs {
		var m map[string]any
		if err := json.Unmarshal(d.InputSchema, &m); err != nil || m["type"] != "object" {
			t.Errorf("%s schema: %s", d.Name, d.InputSchema)
		}
	}
}

func TestDefaultListsDoNotOverlap(t *testing.T) {
	allowed := map[string]bool{}
	for _, n := range CoreTools {
		allowed[n] = true
	}
	for _, n := range CoreToolsDenied {
		if allowed[n] {
			t.Errorf("%s is both allowed and denied", n)
		}
	}
}
