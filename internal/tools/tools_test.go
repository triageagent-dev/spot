package tools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
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
	s.AddTool(mcp.NewToolWithRawSchema("upsert_memory_item", "Write an item.", json.RawMessage(listSchema)), h(func(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		b, _ := json.Marshal(req.GetArguments())
		return mcp.NewToolResultText("args=" + string(b)), nil
	}))
	for _, n := range []string{"get_ego_score", "get_ego_tools", "get_ego_errors"} {
		n := n
		s.AddTool(mcp.NewTool(n), h(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(n), nil
		}))
	}
	for _, n := range []string{"get_memory_context", "say_in_workspace"} {
		n := n
		s.AddTool(mcp.NewTool(n), h(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(n), nil
		}))
	}
	s.AddTool(mcp.NewTool("search_memory"), h(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("notes"), nil
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
	if isErr || !strings.HasSuffix(c, "[result truncated: 100 bytes dropped; ask for less next time: a name, a namespace, a limit]") || !strings.HasPrefix(c, strings.Repeat("é", 50)) {
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
	defs := builtinDefs(false)
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

func TestMemoryWritesCarryTheSpotTag(t *testing.T) {
	url, _ := fakeServer(t)
	withName := func(name string) *Registry {
		r := newRegistry(t, url, func(o *Options) {
			o.Core.Allow = []string{"upsert_memory_item", "list_open_issues"}
			o.SpotName = name
			o.ResultMax = 1000
		})
		r.Begin()
		if _, err := r.List(context.Background()); err != nil {
			t.Fatal(err)
		}
		return r
	}
	for _, c := range []struct{ name, in, want string }{
		{"no tags", `{"content":"c"}`, `"tags":"spot:example"`},
		{"other tags", `{"tags":"spot-note,service/x"}`, `"tags":"spot-note,service/x,spot:example"`},
		{"trailing comma", `{"tags":"a,"}`, `"tags":"a,spot:example"`},
		{"already there", `{"tags":"spot:example,a"}`, `"tags":"spot:example,a"`},
		{"null tags", `{"tags":null}`, `"tags":"spot:example"`},
		{"a number stays a number", `{"tags":"a","limit":15}`, `"limit":15`},
		{"not a string is left to the core", `{"tags":["a"]}`, `"tags":["a"]`},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, isErr := withName("example").Call(context.Background(), "core__upsert_memory_item", json.RawMessage(c.in))
			if isErr || !strings.Contains(got, c.want) {
				t.Errorf("got %q (error %v), want it to contain %s", got, isErr, c.want)
			}
		})
	}
	if got, _ := withName("").Call(context.Background(), "core__upsert_memory_item", json.RawMessage(`{"content":"c"}`)); strings.Contains(got, "spot:") {
		t.Errorf("no spot name, no tag: %q", got)
	}
	if got, _ := withName("example").Call(context.Background(), "core__list_open_issues", json.RawMessage(`{"limit":1}`)); strings.Contains(got, "spot:") {
		t.Errorf("only memory writes are tagged: %q", got)
	}
}

func TestModelAddedPrefixIsStripped(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, nil)
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, name := range []string{"default_api:core__list_open_issues", "functions.core__list_open_issues", "core__list_open_issues"} {
		if c, isErr := r.Call(ctx, name, json.RawMessage(`{"limit":3}`)); isErr || c != `args={"limit":3}` {
			t.Errorf("%s: %q %v", name, c, isErr)
		}
	}
	if c, isErr := r.Call(ctx, "default_api:finish", json.RawMessage(`{"summary":"done"}`)); isErr {
		t.Errorf("a prefixed built-in: %q", c)
	} else if sum, ok := r.Finished(); !ok || sum != "done" {
		t.Errorf("finish through a prefix: %q %v", sum, ok)
	}
	for _, name := range []string{"default_api:core__nope", "default_api:", "x:y:core__nope", "other.core__resolve_issue"} {
		if c, isErr := r.Call(ctx, name, nil); !isErr || !strings.Contains(c, "not in your tool list") {
			t.Errorf("%s must stay unknown: %q %v", name, c, isErr)
		}
	}
}

// A model that takes the spot's own tools for a server's ("core__schedule_next")
// still schedules and finishes: the call is the built-in.
func TestServerPrefixOnABuiltinIsTheBuiltin(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, nil)
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if c, isErr := r.Call(ctx, "core__schedule_next", json.RawMessage(`{"after":"1h","why":"look again"}`)); isErr {
		t.Fatalf("core__schedule_next: %q", c)
	}
	if n := r.Schedule(); n == nil || n.After != "1h" {
		t.Fatalf("schedule: %+v", n)
	}
	if c, isErr := r.Call(ctx, "core__finish", json.RawMessage(`{"summary":"done"}`)); isErr {
		t.Fatalf("core__finish: %q", c)
	} else if sum, ok := r.Finished(); !ok || sum != "done" {
		t.Fatalf("finish: %q %v", sum, ok)
	}
	if c, isErr := r.Call(ctx, "core__nope", nil); !isErr || !strings.Contains(c, "not in your tool list") {
		t.Errorf("an unknown name stays unknown: %q %v", c, isErr)
	}
}

func TestResultCutIsSmallAndTellsTheModelToAskForLess(t *testing.T) {
	r := New(Options{})
	big := strings.Repeat("x", DefaultResultMax+500)
	out := r.cut(big)
	if !strings.Contains(out, "500 bytes dropped") || !strings.Contains(out, "ask for less") {
		t.Fatalf("cut note: %.40s...%s", out, out[len(out)-120:])
	}
	if DefaultResultMax != 12288 {
		t.Errorf("default result cap = %d", DefaultResultMax)
	}
}

func TestEgoToolsHaveTheSessionReaders(t *testing.T) {
	have := map[string]bool{}
	for _, n := range EgoTools {
		have[n] = true
	}
	for _, n := range []string{"summarize_session", "explain_session", "get_session_spans", "list_threads", "get_ego_score", "get_ego_tools", "get_ego_errors"} {
		if !have[n] {
			t.Errorf("EgoTools lacks %s", n)
		}
	}
	for _, n := range []string{"register_correction", "label_session", "set_role_binding", "delete_role_binding", "run_findings"} {
		if have[n] {
			t.Errorf("EgoTools must not hold the writer %s", n)
		}
	}
}

func TestEgoScoreIsOfferedAndSurvivesAModelPrefix(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.Others = []Server{{Name: EgoServer, URL: url, Allow: EgoTools}}
	})
	r.Begin()
	defs, err := r.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"get_ego_score", "get_ego_tools", "get_ego_errors"} {
		if !strings.Contains(strings.Join(names(defs), ","), "ego__"+n) {
			t.Fatalf("ego__%s is not offered: %v", n, names(defs))
		}
		for _, name := range []string{"ego__" + n, "default_api:ego__" + n} {
			if c, isErr := r.Call(context.Background(), name, nil); isErr || c != n {
				t.Errorf("%s: %q %v", name, c, isErr)
			}
		}
	}
}

func TestNothingActsBeforeTheNotesAreSearched(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.RecallFirst = true
		o.Core.Allow = []string{"search_memory", "list_open_issues", "get_pods"}
		o.Others = []Server{{Name: EgoServer, URL: url, Allow: EgoTools}}
	})
	ctx := context.Background()
	r.Begin()
	if _, err := r.List(ctx); err != nil {
		t.Fatal(err)
	}
	if c, isErr := r.Call(ctx, "core__list_open_issues", nil); !isErr || !strings.Contains(c, "search your notes first") {
		t.Errorf("a core tool before the search: %q %v", c, isErr)
	}
	if c, isErr := r.Call(ctx, "ego__get_ego_score", nil); isErr || c != "get_ego_score" {
		t.Errorf("ego reads are never held back: %q %v", c, isErr)
	}
	if c, isErr := r.Call(ctx, "default_api:core__search_memory", nil); isErr || c != "notes" {
		t.Errorf("the search itself: %q %v", c, isErr)
	}
	if c, isErr := r.Call(ctx, "core__list_open_issues", json.RawMessage(`{"limit":1}`)); isErr || !strings.Contains(c, "limit") {
		t.Errorf("after the search: %q %v", c, isErr)
	}
	// A new turn starts unsearched again.
	r.Begin()
	if _, err := r.List(ctx); err != nil {
		t.Fatal(err)
	}
	if _, isErr := r.Call(ctx, "core__list_open_issues", nil); !isErr {
		t.Error("the search of one turn must not carry into the next")
	}
}

func TestRecallFirstIsOffWithoutTheSearchTool(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.RecallFirst = true
		o.Core.Allow = []string{"list_open_issues"}
	})
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c, isErr := r.Call(context.Background(), "core__list_open_issues", nil); isErr {
		t.Errorf("no search_memory offered, nothing to wait for: %q", c)
	}
}

func TestFailedSearchDoesNotCount(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.RecallFirst = true
		o.Core.Allow = []string{"search_memory", "list_open_issues"}
	})
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	// An unknown tool is not a search.
	r.Call(context.Background(), "core__get_pods", nil)
	if _, isErr := r.Call(context.Background(), "core__list_open_issues", nil); !isErr {
		t.Error("held until a search succeeds")
	}
}

func TestNothingIsPostedBeforeTheMemoryIsReadAndANoteWritten(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.PostGate = true
		o.Core.Allow = []string{"say_in_workspace", "get_memory_context", "upsert_memory_item"}
	})
	ctx := context.Background()
	r.Begin()
	if _, err := r.List(ctx); err != nil {
		t.Fatal(err)
	}
	if c, isErr := r.Call(ctx, "core__say_in_workspace", nil); !isErr || !strings.Contains(c, "read what the core holds") {
		t.Errorf("a post before the read: %q %v", c, isErr)
	}
	if c, isErr := r.Call(ctx, "core__get_memory_context", nil); isErr || c != "get_memory_context" {
		t.Errorf("the read: %q %v", c, isErr)
	}
	if c, isErr := r.Call(ctx, "core__say_in_workspace", nil); !isErr || !strings.Contains(c, "write your note first") {
		t.Errorf("a post before the note: %q %v", c, isErr)
	}
	if _, isErr := r.Call(ctx, "core__upsert_memory_item", json.RawMessage(`{"limit":1}`)); isErr {
		t.Error("the note")
	}
	if c, isErr := r.Call(ctx, "core__say_in_workspace", nil); isErr || c != "say_in_workspace" {
		t.Errorf("a post after both: %q %v", c, isErr)
	}
	// A new turn starts unread and unwritten again.
	r.Begin()
	if _, err := r.List(ctx); err != nil {
		t.Fatal(err)
	}
	if _, isErr := r.Call(ctx, "core__say_in_workspace", nil); !isErr {
		t.Error("the read and the note of one turn must not carry into the next")
	}
}

func TestASearchOfTheNotesIsTheReadThePostGateWants(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.PostGate = true
		o.Core.Allow = []string{"say_in_workspace", "search_memory", "get_memory_context", "upsert_memory_item"}
	})
	ctx := context.Background()
	r.Begin()
	if _, err := r.List(ctx); err != nil {
		t.Fatal(err)
	}
	if _, isErr := r.Call(ctx, "core__search_memory", nil); isErr {
		t.Fatal("the search")
	}
	if _, isErr := r.Call(ctx, "core__upsert_memory_item", json.RawMessage(`{"limit":1}`)); isErr {
		t.Fatal("the note")
	}
	if c, isErr := r.Call(ctx, "core__say_in_workspace", nil); isErr || c != "say_in_workspace" {
		t.Errorf("a post after a search and a note, with no get_memory_context: %q %v", c, isErr)
	}
}

func TestPostGateSkipsAStepTheCoreCannotSatisfy(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.PostGate = true
		o.Core.Allow = []string{"say_in_workspace"}
	})
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c, isErr := r.Call(context.Background(), "core__say_in_workspace", nil); isErr {
		t.Errorf("no read or note tool offered, nothing to wait for: %q", c)
	}
}

func TestPostGateIsOffByDefault(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.Core.Allow = []string{"say_in_workspace", "get_memory_context", "upsert_memory_item"}
	})
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c, isErr := r.Call(context.Background(), "core__say_in_workspace", nil); isErr {
		t.Errorf("gate off: %q", c)
	}
}

func TestResultsAreFencedWithARandomId(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.FenceResults = true
		o.Core.Allow = []string{"list_open_issues", "get_pods"}
	})
	ctx := context.Background()
	r.Begin()
	if _, err := r.List(ctx); err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(?s)^<data id=([A-Za-z0-9]+) from=core__list_open_issues>\n(.*)\n</data id=([A-Za-z0-9]+)>$`)
	var ids []string
	for range 2 {
		c, isErr := r.Call(ctx, "core__list_open_issues", nil)
		m := re.FindStringSubmatch(c)
		if isErr || m == nil || m[1] != m[3] || m[2] != "args={}" {
			t.Fatalf("a result is fenced with one id: %q %v", c, isErr)
		}
		ids = append(ids, m[1])
	}
	if ids[0] == ids[1] {
		t.Errorf("the id is the same on two calls: %s", ids[0])
	}
	// An error the tool returns is data too.
	c, isErr := r.Call(ctx, "core__get_pods", json.RawMessage(`{}`))
	if !isErr || !strings.Contains(c, "forbidden: pods") || !strings.HasPrefix(c, "<data id=") {
		t.Errorf("a tool error is fenced: %q %v", c, isErr)
	}
	// The supervisor's own words are not.
	if c, _ := r.Call(ctx, "core__nowhere", nil); strings.Contains(c, "<data") {
		t.Errorf("a refusal of the supervisor is not data: %q", c)
	}
	if c, _ := r.Call(ctx, Finish, json.RawMessage(`{"summary":"done"}`)); strings.Contains(c, "<data") {
		t.Errorf("a built-in is not data: %q", c)
	}
}

func TestResultsAreNotFencedByDefault(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) { o.Core.Allow = []string{"list_open_issues"} })
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c, _ := r.Call(context.Background(), "core__list_open_issues", nil); c != "args={}" {
		t.Errorf("without the option the result is as it came: %q", c)
	}
}

func TestRoomReadsAskOnlyForWhatIsNew(t *testing.T) {
	since := func(room string) int64 {
		if room == "agents" {
			return 0
		}
		return 1700
	}
	got := string(withRoomSince(json.RawMessage(`{"tenant":"t"}`), since))
	if !strings.Contains(got, `"since_ts":1700`) || !strings.Contains(got, `"limit":10`) {
		t.Errorf("a read of the default room is not narrowed: %s", got)
	}
	if got := string(withRoomSince(json.RawMessage(`{"tenant":"t","since_ts":5,"limit":50}`), since)); strings.Contains(got, "1700") || !strings.Contains(got, `"limit":50`) {
		t.Errorf("the model's own since_ts and limit stand: %s", got)
	}
	if got := string(withRoomSince(json.RawMessage(`{"room":"agents"}`), since)); strings.Contains(got, "since_ts") {
		t.Errorf("a room with no watermark is left alone: %s", got)
	}
	if got := string(withRoomSince(json.RawMessage(`not json`), since)); got != "not json" {
		t.Errorf("unreadable arguments change: %s", got)
	}
}

func TestNotesOnlyOffersOneToolAndRefusesTheRest(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.NotesOnly = true
		o.Core.Allow = append(o.Core.Allow, "search_memory", "say_in_workspace")
	})
	defs, err := r.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || defs[0].Name != "core__search_memory" {
		var names []string
		for _, d := range defs {
			names = append(names, d.Name)
		}
		t.Fatalf("the model was offered %v, want only core__search_memory", names)
	}
	for _, n := range []string{"core__list_open_issues", "core__say_in_workspace", "schedule_next", "finish"} {
		if _, isErr := r.Call(context.Background(), n, json.RawMessage(`{}`)); !isErr {
			t.Errorf("%s ran in notes-only mode", n)
		}
	}
}

func TestALimitedTurnHasOnlyTheNotesTheRoomAndThePost(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.Core.Allow = []string{"say_in_workspace", "get_memory_context", "upsert_memory_item", "list_open_issues"}
	})
	ctx := context.Background()
	r.Limit(true)
	r.Begin()
	defs, err := r.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, d := range defs {
		got[d.Name] = true
	}
	for _, want := range []string{"core__say_in_workspace", "core__get_memory_context", "core__upsert_memory_item", ScheduleNext, Finish} {
		if !got[want] {
			t.Errorf("a limited turn lacks %s: %v", want, got)
		}
	}
	if got["core__list_open_issues"] {
		t.Error("a limited turn offers a tool that reads the tenant")
	}
	if _, isErr := r.Call(ctx, "core__list_open_issues", nil); !isErr {
		t.Error("a tool outside the limit ran")
	}
	r.Limit(false)
	r.Begin()
	defs, _ = r.List(ctx)
	found := false
	for _, d := range defs {
		found = found || d.Name == "core__list_open_issues"
	}
	if !found {
		t.Error("the next turn stayed limited")
	}
}

func TestBareToolNameIsTheOneServerOffering(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, nil)
	r.Begin()
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c, isErr := r.Call(context.Background(), "list_open_issues", json.RawMessage(`{}`)); isErr {
		t.Fatalf("bare name of an offered tool: %q", c)
	}
	// A tool the lists leave out is not offered, bare or prefixed.
	if c, isErr := r.Call(context.Background(), "resolve_issue", json.RawMessage(`{}`)); !isErr || !strings.Contains(c, "not in your tool list") {
		t.Errorf("a denied tool stays out by its bare name: %q %v", c, isErr)
	}
}

func TestStartTaskIsOfferedOnlyToATurnThatMayStartOne(t *testing.T) {
	url, _ := fakeServer(t)
	offered := func(mut func(*Options), limit bool) bool {
		r := newRegistry(t, url, mut)
		r.Limit(limit)
		defs, err := r.List(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range defs {
			if d.Name == StartTask {
				return true
			}
		}
		return false
	}
	if offered(nil, false) {
		t.Error("start_task offered without Options.Tasks")
	}
	if !offered(func(o *Options) { o.Tasks = true }, false) {
		t.Error("start_task not offered with Options.Tasks")
	}
	if offered(func(o *Options) { o.Tasks = true }, true) {
		t.Error("start_task offered under a hold rule")
	}
	if offered(func(o *Options) { o.Tasks = true; o.NotesOnly = true }, false) {
		t.Error("start_task offered in notes-only mode")
	}
}

func TestStartTaskRecordsTheTaskAndKeepsAtMostThreePerTurn(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) { o.Tasks = true })
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	call := func(name, in string) (string, bool) { return r.Call(context.Background(), name, json.RawMessage(in)) }
	if got, isErr := call("start_task", `{"title":"","goal":"x"}`); !isErr {
		t.Errorf("an empty title was taken: %s", got)
	}
	if got, isErr := call("start_task", `not json`); !isErr {
		t.Errorf("bad input was taken: %s", got)
	}
	for i, title := range []string{"a", "b", "c"} {
		if got, isErr := call("core__start_task", `{"title":"`+title+`","goal":"do `+title+`"}`); isErr {
			t.Fatalf("task %d: %s", i, got)
		}
	}
	if got, isErr := call("functions.start_task", `{"title":"d","goal":"do d"}`); !isErr || !strings.Contains(got, "3 tasks") {
		t.Errorf("a fourth task: %q %v", got, isErr)
	}
	got := r.Tasks()
	if len(got) != 3 || got[0] != (TaskRequest{Title: "a", Goal: "do a"}) {
		t.Fatalf("tasks: %+v", got)
	}
	r.Begin()
	if len(r.Tasks()) != 0 {
		t.Error("Begin forgets the tasks")
	}
}

func TestStartTaskIsUnknownWithoutTasks(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, nil)
	if _, err := r.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, isErr := r.Call(context.Background(), "start_task", json.RawMessage(`{"title":"a","goal":"b"}`)); !isErr || !strings.Contains(got, "unknown tool") {
		t.Errorf("%q %v", got, isErr)
	}
}

func TestATaskTurnCannotScheduleOrStartTasks(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) { o.Tasks = true })
	r.Task(true)
	r.Begin()
	defs, err := r.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	offered := map[string]bool{}
	for _, d := range defs {
		offered[d.Name] = true
	}
	if !offered[ScheduleNext] || !offered[StartTask] {
		t.Fatalf("the tool list of a task must be the same as any turn's (the cached prefix): %v", offered)
	}
	for _, c := range []struct{ name, in string }{{"schedule_next", `{"after":"5m"}`}, {"start_task", `{"title":"a","goal":"b"}`}} {
		if got, isErr := r.Call(context.Background(), c.name, json.RawMessage(c.in)); !isErr || !strings.Contains(got, "a task cannot") {
			t.Errorf("%s: %q %v", c.name, got, isErr)
		}
	}
	if r.Schedule() != nil || len(r.Tasks()) != 0 {
		t.Error("a refused call recorded something")
	}
	if got, isErr := r.Call(context.Background(), "finish", json.RawMessage(`{"summary":"done"}`)); isErr {
		t.Errorf("a task ends with finish: %q", got)
	}
}

func TestTheNotesTheSupervisorReadCountAsTheSearch(t *testing.T) {
	url, _ := fakeServer(t)
	r := newRegistry(t, url, func(o *Options) {
		o.RecallFirst = true
		o.Core.Allow = []string{"search_memory", "list_open_issues"}
	})
	ctx := context.Background()
	r.Begin()
	if _, err := r.List(ctx); err != nil {
		t.Fatal(err)
	}
	if _, isErr := r.Call(ctx, "core__list_open_issues", nil); !isErr {
		t.Fatal("held until the notes are searched")
	}
	r.NotesRead()
	if c, isErr := r.Call(ctx, "core__list_open_issues", nil); isErr {
		t.Errorf("the supervisor read the notes for this turn: %q", c)
	}
	r.Begin()
	if _, isErr := r.Call(ctx, "core__list_open_issues", nil); !isErr {
		t.Error("the next turn starts held again")
	}
}
