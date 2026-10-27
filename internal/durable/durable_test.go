package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/tools"
)

var srv *testsuite.DevServer

func TestMain(m *testing.M) {
	var err error
	srv, err = testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{LogLevel: "error"})
	if err != nil {
		panic(err)
	}
	heartbeatEvery = 150 * time.Millisecond
	code := m.Run()
	_ = srv.Stop()
	os.Exit(code)
}

// room is where the fake core's say_in_workspace writes.
type room struct {
	mu    sync.Mutex
	posts []string
	slow  map[string]time.Duration // a post that takes this long, by text
}

func (r *room) add(s string) { r.mu.Lock(); r.posts = append(r.posts, s); r.mu.Unlock() }
func (r *room) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.posts...)
}
func (r *room) count(s string) int {
	n := 0
	for _, p := range r.all() {
		if p == s {
			n++
		}
	}
	return n
}

// coreServer is a core MCP server with the tools the tests need.
func coreServer(t *testing.T, rm *room) string {
	t.Helper()
	s := server.NewMCPServer("example", "1")
	text := func(s string) *mcp.CallToolResult { return mcp.NewToolResultText(s) }
	for _, n := range []string{"search_memory", "get_memory_context", "get_tenant_situation", "upsert_memory_item"} {
		s.AddTool(mcp.NewTool(n), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) { return text(n + " ok"), nil })
	}
	s.AddTool(mcp.NewTool("slow_read"), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		select {
		case <-time.After(700 * time.Millisecond):
		case <-ctx.Done():
		}
		return text("read"), nil
	})
	s.AddTool(mcp.NewTool("say_in_workspace", mcp.WithString("text")), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		msg, _ := req.GetArguments()["text"].(string)
		rm.mu.Lock()
		d := rm.slow[msg]
		rm.mu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
		rm.add(msg)
		return text("posted"), nil
	})
	ts := server.NewTestStreamableHTTPServer(s)
	t.Cleanup(ts.Close)
	return ts.URL
}

// model is a scripted model: fn answers by the first message of the turn and by
// how many answers the turn has had.
type model struct {
	mu    sync.Mutex
	fn    func(first string, n int, last string) llm.Response
	calls []string // "<first line>#<n>"
}

func (m *model) Complete(_ context.Context, req llm.Request) (llm.Response, error) {
	first := req.Messages[0].Content[0].Text
	n, last := 0, ""
	for _, msg := range req.Messages {
		if msg.Role == llm.RoleAssistant {
			n++
		}
	}
	if l := req.Messages[len(req.Messages)-1]; l.Role == llm.RoleUser {
		for _, b := range l.Content {
			last += b.Content + b.Text
		}
	}
	m.mu.Lock()
	m.calls = append(m.calls, fmt.Sprintf("%s#%d", strings.SplitN(first, "\n", 2)[0], n))
	m.mu.Unlock()
	r := m.fn(first, n, last)
	r.Model = "fake-model"
	r.Usage = llm.Usage{Input: 100, Output: 10}
	return r, nil
}

func (m *model) count(prefix string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := 0
	for _, c := range m.calls {
		if strings.HasPrefix(c, prefix) {
			k++
		}
	}
	return k
}

func use(calls ...llm.Call) llm.Response { return llm.ToolUse(llm.Usage{}, calls...).Response }
func say(text string) llm.Call {
	b, _ := json.Marshal(map[string]string{"text": text})
	return llm.Call{Name: "core__say_in_workspace", Input: string(b)}
}
func askLater(after, why string) llm.Call {
	b, _ := json.Marshal(map[string]string{"after": after, "why": why})
	return llm.Call{Name: "schedule_next", Input: string(b)}
}
func finish(sum string) llm.Call {
	b, _ := json.Marshal(map[string]string{"summary": sum})
	return llm.Call{Name: "finish", Input: string(b)}
}

// host is the process side: the notes, the books, the announcement. It reads a
// hold rule from a flag the test sets.
type host struct {
	mu         sync.Mutex
	hold       bool
	paused     time.Time
	min, max   time.Duration
	prepared   []PrepareIn
	accounts   []AccountIn
	announces  []AnnounceIn
	failures   int
	typingCall []bool
}

func (h *host) Prepare(in PrepareIn) (PrepareOut, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.prepared = append(h.prepared, in)
	out := PrepareOut{
		Min: h.min, Max: h.max, Turn: 30 * time.Second, Tool: 10 * time.Second, Model: "fake-model", Typing: true,
		Caps: agent.Caps{MaxSteps: 12, MaxToolCalls: 10, MaxTurnTokens: 0},
	}
	if !h.paused.IsZero() {
		out.Paused, out.Until = true, h.paused
		return out, nil
	}
	exempt := in.Promise || in.PromisePending
	out.Hold = h.hold && in.Reason != ReasonHuman && (in.Reason != ReasonTick || !exempt)
	out.Limited = h.hold && in.Reason == ReasonHuman
	out.Prompt = "Wake reason: " + in.Reason + "\n" + strings.Join(in.Facts, "\n")
	if in.Why != "" {
		out.Prompt += "\nYour reason: " + in.Why
	}
	return out, nil
}

func (h *host) Account(in AccountIn) (AccountOut, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.accounts = append(h.accounts, in)
	failed := in.Out.Failed
	if in.Lane == LaneLong {
		if failed {
			h.failures++
		} else {
			h.failures = 0
		}
	}
	return AccountOut{Failed: failed, Failures: h.failures, Backoff: time.Duration(h.failures) * h.min}, nil
}

func (h *host) Announce(in AnnounceIn) error {
	h.mu.Lock()
	h.announces = append(h.announces, in)
	h.mu.Unlock()
	return nil
}

func (h *host) Typing(_ context.Context, _ string, on bool) error {
	h.mu.Lock()
	h.typingCall = append(h.typingCall, on)
	h.mu.Unlock()
	return nil
}

func (h *host) setHold(v bool) { h.mu.Lock(); h.hold = v; h.mu.Unlock() }
func (h *host) lanes(lane string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, a := range h.accounts {
		if a.Lane == lane {
			n++
		}
	}
	return n
}
func (h *host) reasons() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, a := range h.accounts {
		out = append(out, a.Lane+":"+a.Reason)
	}
	return out
}

type rig struct {
	t     *testing.T
	c     client.Client
	url   string
	room  *room
	host  *host
	model *model
	opts  Options
	eng   *Engine
	id    string
	seq   int64
}

func newRig(t *testing.T, mut func(*Options)) *rig {
	t.Helper()
	name := fmt.Sprintf("t%s-%d", strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name())), time.Now().UnixNano()%1_000_000)
	rm := &room{slow: map[string]time.Duration{}}
	r := &rig{t: t, room: rm, url: coreServer(t, rm), host: &host{min: time.Second, max: time.Hour}, model: &model{}, id: "spot-" + name}
	r.opts = Options{Name: name, TaskQueue: "tq-" + name, WorkflowID: r.id, StopTimeout: 100 * time.Millisecond}
	if mut != nil {
		mut(&r.opts)
	}
	var err error
	if r.opts.PayloadKeyFile != "" {
		dc, fc, derr := dataConverter(r.opts.PayloadKeyFile)
		if derr != nil {
			t.Fatal(derr)
		}
		r.c, err = client.Dial(client.Options{HostPort: srv.FrontendHostPort(), Namespace: "default", DataConverter: dc, FailureConverter: fc})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.c.Close)
	} else {
		r.c = srv.Client()
	}
	r.startWorker()
	t.Cleanup(r.stopWorker)
	return r
}

func (r *rig) startWorker() {
	r.t.Helper()
	tok := filepath.Join(r.t.TempDir(), "token")
	if err := os.WriteFile(tok, []byte("t\n"), 0o600); err != nil {
		r.t.Fatal(err)
	}
	newTools := func() *tools.Registry {
		return tools.New(tools.Options{
			Core: tools.Server{Name: tools.CoreServer, URL: r.url, TokenFile: tok,
				Allow: []string{"search_memory", "get_memory_context", "get_tenant_situation", "upsert_memory_item", "say_in_workspace", "slow_read"}},
			CallTimeout: 10 * time.Second,
		})
	}
	r.eng = New(r.c, r.opts, Deps{LLM: r.model, NewTools: newTools, System: []string{"system"}, Model: "fake-model", Host: r.host})
	if err := r.eng.Start(); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) stopWorker() {
	if r.eng != nil {
		r.eng.Close()
		r.eng = nil
	}
}

func (r *rig) wake(reason string, facts ...string) {
	r.t.Helper()
	r.seq++
	if err := r.engine().Wake(context.Background(), WakeSignal{Seq: r.seq, Reason: reason, Facts: facts}); err != nil {
		r.t.Fatal(err)
	}
}

// engine is a handle for signalling even while the worker is stopped.
func (r *rig) engine() *Engine { return &Engine{o: r.opts, c: r.c} }

func (r *rig) state() SpotState {
	r.t.Helper()
	st, err := r.engine().State(context.Background())
	if err != nil {
		r.t.Fatal(err)
	}
	return st
}

func (r *rig) waitPost(text string, within time.Duration) {
	r.t.Helper()
	r.waitFor(fmt.Sprintf("post %q", text), within, func() bool { return r.room.count(text) > 0 })
}

func (r *rig) waitFor(what string, within time.Duration, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	r.t.Fatalf("timed out waiting for %s\nposts: %v\nmodel: %v\nreasons: %v\nstate: %+v", what, r.room.all(), r.model.calls, r.host.reasons(), r.stateOrZero())
}

func (r *rig) stateOrZero() SpotState {
	st, _ := r.engine().State(context.Background())
	return st
}

// A person's message is answered while a long turn is still working; the long
// turn is not cut and its post comes after.
func TestAMessageIsAnsweredWhileALongTurnWorks(t *testing.T) {
	r := newRig(t, nil)
	r.model.fn = func(first string, n int, _ string) llm.Response {
		switch {
		case strings.Contains(first, "long job"):
			switch {
			case n < 4:
				return use(llm.Call{Name: "core__slow_read"})
			case n == 4:
				return use(say("long job done"))
			}
		case strings.Contains(first, "Wake reason: human") && n == 0:
			return use(say("hi"), finish("said hi"))
		}
		return use(finish("done"))
	}
	r.wake(ReasonEdge, "long job please")
	time.Sleep(900 * time.Millisecond)
	r.wake(ReasonHuman, "hello")
	r.waitPost("hi", 4*time.Second)
	if r.room.count("long job done") != 0 {
		t.Fatal("the long turn had already ended: the test proves nothing")
	}
	r.waitPost("long job done", 20*time.Second)
	if p := r.room.all(); p[0] != "hi" || p[len(p)-1] != "long job done" {
		t.Fatalf("order: %v", p)
	}
	st := r.state()
	if st.Wakes != 2 {
		t.Fatalf("state: %+v", st)
	}
}

// A wake a person asked for is a durable timer: it comes due while no worker
// runs, and the next worker runs it, once.
func TestAPromisedWakeSurvivesAWorkerRestart(t *testing.T) {
	r := newRig(t, nil)
	r.model.fn = func(first string, n int, _ string) llm.Response {
		switch {
		case strings.Contains(first, "note in"):
			if n == 0 {
				return use(say("ack: note coming"), askLater("2s", "the note"))
			}
		case strings.Contains(first, "Your reason: the note"):
			if n == 0 {
				return use(say("THE NOTE"))
			}
		}
		return use(finish("done"))
	}
	r.wake(ReasonHuman, "note in 2s please")
	r.waitPost("ack: note coming", 4*time.Second)
	r.stopWorker()
	time.Sleep(3500 * time.Millisecond) // it comes due with nobody there
	if r.room.count("THE NOTE") != 0 {
		t.Fatal("the note was given with no worker")
	}
	r.startWorker()
	r.waitPost("THE NOTE", 15*time.Second)
	time.Sleep(600 * time.Millisecond)
	if n := r.room.count("THE NOTE"); n != 1 {
		t.Fatalf("the note was given %d times", n)
	}
}

// Under a hold rule an edge wakes no model; a person's message has the limited
// tools; a check they ask for is a wake the spot schedules, and that one has
// every tool.
func TestAHoldRuleDropsEdgesAndLimitsAPersonsTurn(t *testing.T) {
	r := newRig(t, nil)
	r.host.setHold(true)
	r.model.fn = func(first string, n int, last string) llm.Response {
		switch {
		case strings.Contains(first, "Wake reason: human"):
			switch n {
			case 0:
				return use(llm.Call{Name: "core__get_tenant_situation"})
			case 1:
				return use(say("I cannot check now; I will schedule it"), askLater("2s", "check the cluster"))
			}
		case strings.Contains(first, "Your reason: check the cluster"):
			switch n {
			case 0:
				return use(llm.Call{Name: "core__get_tenant_situation"})
			case 1:
				return use(say("cluster: " + strings.TrimSpace(last)))
			}
		}
		return use(finish("done"))
	}
	r.wake(ReasonEdge, "new issue a")
	r.wake(ReasonEdge, "new issue b")
	time.Sleep(1500 * time.Millisecond)
	if r.model.count("") != 0 {
		t.Fatalf("a model call under a hold rule: %v", r.model.calls)
	}
	if st := r.state(); st.Held < 1 {
		t.Fatalf("state: %+v", st)
	}
	r.wake(ReasonHuman, "check the cluster now")
	r.waitPost("I cannot check now; I will schedule it", 5*time.Second)
	// The limited turn was refused the cluster read: the tool was not in its list.
	r.waitPost("cluster: get_tenant_situation ok", 10*time.Second)
	if r.room.count("cluster: get_tenant_situation ok") != 1 {
		t.Fatalf("posts: %v", r.room.all())
	}
}

// A crash in the middle of a turn resumes at the step: no step of the model is
// sent twice and a post is made once.
func TestACrashInATurnResumesAtTheStep(t *testing.T) {
	r := newRig(t, nil)
	r.model.fn = func(first string, n int, _ string) llm.Response {
		switch {
		case n < 4:
			return use(llm.Call{Name: "core__slow_read"})
		case n == 4:
			return use(say("long job done"))
		}
		return use(finish("done"))
	}
	r.wake(ReasonEdge, "long job please")
	time.Sleep(1500 * time.Millisecond) // inside the slow reads
	r.stopWorker()
	time.Sleep(300 * time.Millisecond)
	r.startWorker()
	r.waitPost("long job done", 30*time.Second)
	time.Sleep(500 * time.Millisecond)
	if n := r.room.count("long job done"); n != 1 {
		t.Fatalf("posted %d times", n)
	}
	// Steps 0..5 each once, the one cut by the crash at most twice.
	seen := map[string]int{}
	for _, c := range r.model.calls {
		seen[c]++
	}
	again := 0
	for _, n := range seen {
		if n > 1 {
			again++
		}
	}
	if again > 1 {
		t.Fatalf("steps were sent again: %v", seen)
	}
}

// A call that may have an effect is run once: if the worker dies in it, the
// model is told it may or may not have run and it is not repeated by the engine.
func TestAWriteCutByACrashIsReportedAndNotRepeated(t *testing.T) {
	r := newRig(t, nil)
	r.room.slow["the post"] = 1500 * time.Millisecond
	told := make(chan string, 1)
	r.model.fn = func(first string, n int, last string) llm.Response {
		switch n {
		case 0:
			return use(say("the post"))
		case 1:
			select {
			case told <- last:
			default:
			}
		}
		return use(finish("done"))
	}
	r.wake(ReasonEdge, "post something")
	time.Sleep(600 * time.Millisecond) // inside the post
	r.stopWorker()
	r.startWorker()
	select {
	case last := <-told:
		if !strings.Contains(last, "did not finish") || !strings.Contains(last, "may or may not have run") {
			t.Fatalf("the model was told: %q", last)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("the turn did not go on; model: %v", r.model.calls)
	}
	time.Sleep(2 * time.Second)
	if n := r.room.count("the post"); n > 1 {
		t.Fatalf("the post was made %d times", n)
	}
}

// The tick is a durable timer of the workflow: the agent asks for the next wake
// and it comes. A restart that finds the schedule alive does not wake it.
func TestTheNextWakeComesAndAStartupIsSkippedWhenTheScheduleSurvived(t *testing.T) {
	r := newRig(t, nil)
	r.model.fn = func(first string, n int, _ string) llm.Response {
		if n == 0 {
			return use(askLater("1s", "look again"))
		}
		return use(finish("done"))
	}
	r.wake(ReasonStartup)
	r.waitFor("the tick", 10*time.Second, func() bool { return r.host.lanes(LaneLong) >= 2 })
	if got := r.host.reasons(); got[0] != "long:startup" || got[1] != "long:tick" {
		t.Fatalf("reasons: %v", got)
	}
	r.host.mu.Lock()
	p := r.host.prepared[1]
	r.host.mu.Unlock()
	if p.Why != "look again" {
		t.Fatalf("the tick was not told what the agent left for itself: %+v", p)
	}
	// A second startup signal while the schedule is alive wakes nothing.
	before := r.host.lanes(LaneLong)
	r.host.mu.Lock()
	r.host.min = time.Hour // the next wake is far
	r.host.mu.Unlock()
	r.model.fn = func(string, int, string) llm.Response { return use(askLater("1h", "idle")) }
	r.waitFor("a wake", 5*time.Second, func() bool { return r.host.lanes(LaneLong) > before })
	time.Sleep(1500 * time.Millisecond)
	n := r.host.lanes(LaneLong)
	r.wake(ReasonStartup)
	time.Sleep(1200 * time.Millisecond)
	if r.host.lanes(LaneLong) != n {
		t.Fatalf("a startup woke a spot whose schedule was alive: %v", r.host.reasons())
	}
}

// The daily ceiling holds the agent back: no model call, and the next wake is
// the time the ceiling lifts.
func TestAPausedSpotMakesNoModelCall(t *testing.T) {
	r := newRig(t, nil)
	until := time.Now().Add(time.Hour)
	r.host.mu.Lock()
	r.host.paused = until
	r.host.mu.Unlock()
	r.model.fn = func(string, int, string) llm.Response { return use(finish("never")) }
	r.wake(ReasonEdge, "an issue")
	r.waitFor("the announcement", 5*time.Second, func() bool { r.host.mu.Lock(); defer r.host.mu.Unlock(); return len(r.host.announces) > 0 })
	st := r.state()
	if len(r.model.calls) != 0 || st.Next.Sub(until).Abs() > time.Second {
		t.Fatalf("model calls %v, state %+v", r.model.calls, st)
	}
}

// historyHolds reports whether the history of the spot workflow or of its first
// wake holds the text, as the Temporal service has it (no converter).
func historyHolds(t *testing.T, r *rig, text string) bool {
	t.Helper()
	raw, err := client.Dial(client.Options{HostPort: srv.FrontendHostPort(), Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	events := 0
	found := false
	for _, id := range []string{r.id, r.opts.Name + "-wake-1"} {
		it := raw.GetWorkflowHistory(context.Background(), id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for it.HasNext() {
			ev, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			events++
			b, _ := ev.Marshal()
			found = found || strings.Contains(string(b), text)
		}
	}
	if events == 0 {
		t.Fatal("no history")
	}
	return found
}

func secretTurn(r *rig, secret string) {
	r.model.fn = func(first string, n int, _ string) llm.Response {
		if n == 0 {
			return use(say("I will look into: " + secret))
		}
		return use(finish("done"))
	}
	r.wake(ReasonEdge, secret)
	r.waitPost("I will look into: "+secret, 10*time.Second)
	time.Sleep(500 * time.Millisecond)
}

// With a key the history holds no clear text: the Temporal service sees only
// ciphertext.
func TestTheHistoryHoldsNoClearText(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(keyFile, []byte(strings.Repeat("0f", 32)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newRig(t, func(o *Options) { o.PayloadKeyFile = keyFile })
	secretTurn(r, "the payments database is down in the east cluster")
	if historyHolds(t, r, "payments") || historyHolds(t, r, "east cluster") {
		t.Fatal("clear text in the history")
	}
}

// The control: without a key the same check finds the text, so the test above
// can fail.
func TestTheHistoryHoldsTheTextWithoutAKey(t *testing.T) {
	r := newRig(t, nil)
	secretTurn(r, "the payments database is down in the east cluster")
	if !historyHolds(t, r, "payments") {
		t.Fatal("the check cannot see the text even when it is there")
	}
}

// The "typing…" indicator is shown for the turn and cleared when it ends, and
// ending the turn does not wait for a renewal.
func TestTheIndicatorIsShownAndClearedWhenTheTurnEnds(t *testing.T) {
	r := newRig(t, nil)
	r.model.fn = func(first string, n int, _ string) llm.Response {
		if n == 0 {
			return use(llm.Call{Name: "core__slow_read"})
		}
		return use(finish("done"))
	}
	began := time.Now()
	r.wake(ReasonEdge, "look")
	r.waitFor("the turn", 10*time.Second, func() bool { return r.host.lanes(LaneLong) == 1 })
	if d := time.Since(began); d > 5*time.Second {
		t.Fatalf("the turn took %s to end", d)
	}
	r.host.mu.Lock()
	got := fmt.Sprint(r.host.typingCall)
	r.host.mu.Unlock()
	if got != "[true false]" {
		t.Fatalf("indicator calls: %s", got)
	}
}
