package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/testsuite"

	"github.com/triageagent-dev/spot/internal/config"
	"github.com/triageagent-dev/spot/internal/corecli"
	"github.com/triageagent-dev/spot/internal/durable"
	"github.com/triageagent-dev/spot/internal/ledger"
	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/tools"
	"github.com/triageagent-dev/spot/internal/triggers"
)

var bridgeSrv *testsuite.DevServer

// bridgeServer starts the Temporal dev server once, for the tests that use it.
var bridgeOnce sync.Once

func devServer(t *testing.T) *testsuite.DevServer {
	t.Helper()
	bridgeOnce.Do(func() {
		var err error
		bridgeSrv, err = testsuite.StartDevServer(context.Background(), testsuite.DevServerOptions{LogLevel: "error"})
		if err != nil {
			panic(err)
		}
	})
	return bridgeSrv
}

func TestMain(m *testing.M) {
	code := m.Run()
	if bridgeSrv != nil {
		_ = bridgeSrv.Stop()
	}
	os.Exit(code)
}

// bmodel is the scripted model of the bridge tests: fn gets the first message of
// the turn and how many answers it has had.
type bmodel struct {
	mu      sync.Mutex
	fn      func(first string, n int) (llm.Response, error)
	prompts []string
}

func (m *bmodel) Complete(_ context.Context, req llm.Request) (llm.Response, error) {
	first := req.Messages[0].Content[0].Text
	n := 0
	for _, msg := range req.Messages {
		if msg.Role == llm.RoleAssistant {
			n++
		}
	}
	if n == 0 {
		m.mu.Lock()
		m.prompts = append(m.prompts, first)
		m.mu.Unlock()
	}
	r, err := m.fn(first, n)
	r.Model, r.Usage = "fake-model", llm.Usage{Input: 100, Output: 10}
	return r, err
}

func (m *bmodel) turns() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.prompts...)
}

func bfinish(summary, after string) llm.Response {
	return llm.ToolUse(llm.Usage{},
		llm.Call{Name: "schedule_next", Input: fmt.Sprintf(`{"after":%q,"why":"look again"}`, after)},
		llm.Call{Name: "finish", Input: fmt.Sprintf(`{"summary":%q}`, summary)},
	).Response
}

type bridgeRig struct {
	t      *testing.T
	h      *harness
	sup    *Supervisor
	model  *bmodel
	cancel context.CancelFunc
	done   chan error
	logs   safeBuf
	name   string
}

func newBridgeRig(t *testing.T, mut func(*config.Config)) *bridgeRig {
	t.Helper()
	return newBridgeRigOn(t, mut, nil)
}

// newBridgeRigOn is newBridgeRig with the core's push channel, when trig is set.
func newBridgeRigOn(t *testing.T, mut func(*config.Config), trig Trigger) *bridgeRig {
	t.Helper()
	return newBridgeRigIn(t, mut, trig, "")
}

// newBridgeRigIn is newBridgeRigOn with the engine keeping its payloads in
// payloadDir (and sending references), when it is set.
func newBridgeRigIn(t *testing.T, mut func(*config.Config), trig Trigger, payloadDir string) *bridgeRig {
	t.Helper()
	h := newHarness(t, mut)
	mcpSrv := server.NewMCPServer("example", "1")
	for _, n := range []string{"search_memory", "say_in_workspace"} {
		mcpSrv.AddTool(mcp.NewTool(n), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(n + " ok"), nil
		})
	}
	ts := server.NewTestStreamableHTTPServer(mcpSrv)
	t.Cleanup(ts.Close)

	led, err := ledger.New(h.state)
	must(t, err)
	r := &bridgeRig{t: t, h: h, model: &bmodel{}, done: make(chan error, 1)}
	log := slog.New(slog.NewTextHandler(&r.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r.sup = New(h.cfg, led, h.core, nil, log)
	if trig != nil {
		r.sup.SetTrigger(trig)
		r.sup.triggerWait = 10 * time.Millisecond
	}

	name := fmt.Sprintf("b%s-%d", strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name())), time.Now().UnixNano()%1_000_000)
	newTools := func() *tools.Registry {
		return tools.New(tools.Options{
			Core:        tools.Server{Name: tools.CoreServer, URL: ts.URL, TokenFile: h.cfg.CoreTokenFile, Allow: []string{"search_memory", "say_in_workspace"}},
			CallTimeout: 10 * time.Second, Tasks: true,
		})
	}
	r.name = name
	cl := devServer(t).Client()
	if payloadDir != "" {
		var err error
		cl, err = durable.Dial(durable.Options{Address: devServer(t).FrontendHostPort(), Namespace: "default", PayloadDir: payloadDir, Name: name})
		must(t, err)
		t.Cleanup(cl.Close)
	}
	eng := durable.New(cl, durable.Options{PayloadDir: payloadDir, Name: name, TaskQueue: "tq-" + name, WorkflowID: "spot-" + name, StopTimeout: 100 * time.Millisecond},
		durable.Deps{LLM: r.model, NewTools: newTools, System: []string{"system"}, Model: "fake-model", Host: r.sup.Host()})
	must(t, eng.Start())
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() { r.done <- r.sup.RunBridge(ctx, eng) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-r.done:
			if err != nil {
				t.Errorf("the bridge stopped with %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("the bridge did not stop")
		}
		eng.Close()
	})
	return r
}

func (r *bridgeRig) waitFor(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	r.t.Fatalf("timed out waiting for %s\nmodel turns: %d\nstatus: %+v\nlogs:\n%s", what, len(r.model.turns()), r.h.status(), r.logs.String())
}

func (r *bridgeRig) idleWithTurns(n int) {
	r.t.Helper()
	r.waitFor(fmt.Sprintf("%d turn(s) and idle", n), func() bool {
		st := r.h.status()
		return len(r.model.turns()) == n && st.State == "idle" && st.LastTurn != nil
	})
}

// The startup wake runs in the workflow and the supervisor books it: the
// tokens, the digest, the heartbeat, and the next wake the agent asked for.
func TestTheBridgeRunsAStartupWakeAndBooksIt(t *testing.T) {
	r := newBridgeRig(t, nil)
	r.model.fn = func(string, int) (llm.Response, error) { return bfinish("all quiet", "3m"), nil }
	r.idleWithTurns(1)
	st := r.h.status()
	if st.LastTurn.Reason != "startup" || st.LastTurn.Summary != "all quiet" || st.LastTurn.IsError {
		t.Fatalf("last turn: %+v", st.LastTurn)
	}
	if st.NextWake == nil || st.NextWake.Why != "look again" || st.NextWake.Source != "agent" {
		t.Fatalf("next wake: %+v", st.NextWake)
	}
	if u := r.sup.led.Usage(); u.Tokens != 110 {
		t.Fatalf("usage: %+v", u)
	}
	if d := r.sup.led.LoadDigest(); len(d) != 1 || d[0].Summary != "all quiet" || d[0].Next != "look again" {
		t.Fatalf("digest: %+v", d)
	}
	if r.h.core.beatCount() != 1 {
		t.Fatalf("heartbeats: %d", r.h.core.beatCount())
	}
	if p := r.model.turns()[0]; !strings.Contains(p, "Wake reason: startup") || !strings.Contains(p, "You have at most") {
		t.Fatalf("prompt: %s", p)
	}
}

// A standing rule keeps the model out of a startup wake; a person's message
// still wakes it, with the limited tools, on the short lane.
func TestTheBridgeHoldsUnderARuleAndAnswersAPerson(t *testing.T) {
	r := newBridgeRig(t, nil)
	r.h.core.mu.Lock()
	r.h.core.hold = true
	r.h.core.rules = []string{"RULE: check nothing until I ask"}
	r.h.core.roomMsgs["team"] = []corecli.RoomMessage{say("alice", "alice", "old")}
	r.h.core.mu.Unlock()
	r.model.fn = func(string, int) (llm.Response, error) { return bfinish("answered", "2h"), nil }

	r.waitFor("the held startup", func() bool {
		st := r.h.status()
		return st.State == "idle" && st.NextWake != nil && st.NextWake.Source == "hold"
	})
	time.Sleep(300 * time.Millisecond) // the first poll primes the room cursor
	if n := len(r.model.turns()); n != 0 {
		t.Fatalf("a model call under a hold rule: %d", n)
	}

	r.h.post("team", say("alice", "alice", "are you there, example-spot?"))
	r.waitFor("the answer", func() bool { return len(r.model.turns()) == 1 })
	p := r.model.turns()[0]
	if !strings.Contains(p, "Wake reason: human") || !strings.Contains(p, "you have only your notes, the room, posting and scheduling") ||
		!strings.Contains(p, "short lane") || !strings.Contains(p, "RULE: check nothing until I ask") {
		t.Fatalf("prompt of the person's wake: %s", p)
	}
}

// A turn the API refuses fails: no heartbeat, the failure is counted, and the
// next wake waits at least the backoff.
func TestTheBridgeCountsAFailedTurnAndBacksOff(t *testing.T) {
	r := newBridgeRig(t, nil)
	r.model.fn = func(string, int) (llm.Response, error) {
		return llm.Response{}, &llm.APIError{Status: 400, Message: "bad request"}
	}
	r.waitFor("the failed turn", func() bool {
		st := r.h.status()
		return st.State == "idle" && st.LastTurn != nil && st.LastTurn.IsError
	})
	st := r.h.status()
	if st.Failures != 1 || st.LastError == "" || r.h.core.beatCount() != 0 {
		t.Fatalf("status: %+v beats=%d", st, r.h.core.beatCount())
	}
	at, _ := time.Parse(time.RFC3339, st.NextWake.At)
	if d := time.Until(at); d < r.h.cfg.MinWake-5*time.Second {
		t.Fatalf("next wake in %s, want at least the backoff %s", d, r.h.cfg.MinWake)
	}
	if u := r.sup.led.Usage(); u.Tokens != 0 {
		t.Fatalf("usage: %+v", u)
	}
}

// What the intake sees while the long turn works becomes a wake of its own: a
// person's message is answered by the short lane without waiting.
func TestTheBridgeAnswersAPersonWhileALongTurnWorks(t *testing.T) {
	r := newBridgeRig(t, func(c *config.Config) { c.EdgeCooldown = 0 })
	release := make(chan struct{})
	r.model.fn = func(first string, n int) (llm.Response, error) {
		if strings.Contains(first, "Wake reason: startup") {
			<-release // the long turn is busy
		}
		return bfinish("done", "2h"), nil
	}
	r.h.core.mu.Lock()
	r.h.core.roomMsgs["team"] = []corecli.RoomMessage{say("alice", "alice", "old")}
	r.h.core.mu.Unlock()
	r.waitFor("the long turn", func() bool { return len(r.model.turns()) == 1 })
	time.Sleep(300 * time.Millisecond)
	r.h.post("team", say("alice", "alice", "quick question, example-spot"))
	r.waitFor("the short lane's answer", func() bool { return len(r.model.turns()) == 2 })
	if p := r.model.turns()[1]; !strings.Contains(p, "Wake reason: human") || !strings.Contains(p, "short lane") {
		t.Fatalf("prompt: %s", p)
	}
	close(release)
	r.waitFor("both done", func() bool {
		st := r.h.status()
		return st.State == "idle" && r.sup.led.Usage().Turns == 2
	})
}

// With the core's stream connected the loop waits on the stream and not on a
// sleep; the workflow's calls must be served there too, or the first wake never
// starts.
func TestTheBridgeServesTheWorkflowWhileWatchingTheStream(t *testing.T) {
	r := newBridgeRigOn(t, func(c *config.Config) { c.PollInterval = time.Hour }, newFakeTrigger(true))
	r.model.fn = func(string, int) (llm.Response, error) { return bfinish("all quiet", "2h"), nil }
	r.idleWithTurns(1)
	if st := r.h.status(); st.Triggers != "stream" {
		t.Fatalf("watching by %q, want the stream", st.Triggers)
	}
}

// The durable engine skips a quiet startup the same way: the workflow ends the
// wake at its head and schedules the longest wait.
func TestTheBridgeMakesNoModelCallOnAQuietStartup(t *testing.T) {
	r := newBridgeRig(t, func(c *config.Config) { c.SkipQuietWakes = true })
	r.model.fn = func(string, int) (llm.Response, error) { return bfinish("x", "2h"), nil }
	r.waitFor("the quiet startup", func() bool {
		st := r.h.status()
		return st.State == "idle" && st.NextWake != nil && st.NextWake.Source == "quiet"
	})
	if n := len(r.model.turns()); n != 0 {
		t.Fatalf("a model call on a quiet startup: %d", n)
	}
}

// The durable engine reports the same line of activity per wake: a quiet
// startup, then a turn with its counts.
func TestTheBridgeReportsEachWakeToTheCore(t *testing.T) {
	trig := newFakeTrigger(true)
	r := newBridgeRigOn(t, func(c *config.Config) { c.SkipQuietWakes = true; c.PollInterval = time.Hour }, trig)
	r.model.fn = func(string, int) (llm.Response, error) { return bfinish("x", "2h"), nil }
	r.waitFor("the quiet line", func() bool { return len(trig.wakeLines()) >= 1 })
	if w := trig.wakeLines()[0]; w.Outcome != "quiet" || w.Reason != "startup" || w.Lane != "long" || w.Steps != 0 || w.NextSource != "quiet" {
		t.Fatalf("quiet line: %+v", w)
	}
	time.Sleep(100 * time.Millisecond) // the supervisor has read the empty core
	trig.say(triggers.Pending{Issues: true})
	time.Sleep(100 * time.Millisecond)
	r.h.core.setIssues(issue("a", "critical", 0))
	trig.say(triggers.Pending{Issues: true})
	r.waitFor("the turn line", func() bool { return len(trig.wakeLines()) >= 2 })
	if w := trig.wakeLines()[1]; w.Outcome != "turn" || w.Lane != "long" || w.Steps == 0 || w.Next == 0 {
		t.Fatalf("turn line: %+v", w)
	}
}

// With the engine keeping its payloads, the workflow's history holds no content:
// not the wake prompt, not the model's summary, not as ciphertext either.
func TestTheHistoryHoldsNoContentWhenPayloadsAreKept(t *testing.T) {
	dir := t.TempDir()
	r := newBridgeRigIn(t, func(c *config.Config) { c.SkipQuietWakes = false }, nil, dir)
	r.model.fn = func(string, int) (llm.Response, error) { return bfinish("the secret summary of this wake", "2h"), nil }
	r.idleWithTurns(1)

	// The default client has no codec: it reads the history as the service holds it.
	raw := devServer(t).Client()
	var all []byte
	for _, id := range []string{"spot-" + r.name, r.name + "-wake-1"} {
		it := raw.GetWorkflowHistory(context.Background(), id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for it.HasNext() {
			ev, err := it.Next()
			must(t, err)
			b, err := ev.Marshal()
			must(t, err)
			all = append(all, b...)
		}
	}
	if len(all) == 0 {
		t.Fatal("no history was read")
	}
	for _, leak := range []string{"Wake reason", "the secret summary", "schedule_next", "search_memory"} {
		if bytes.Contains(all, []byte(leak)) {
			t.Errorf("the history holds %q", leak)
		}
	}
	if !bytes.Contains(all, []byte("binary/claim")) {
		t.Error("the history holds no reference")
	}
	if ents, _ := os.ReadDir(dir); len(ents) == 0 {
		t.Error("nothing was kept in the payload directory")
	}
}

// A turn starts tasks; each runs in a turn of its own with only its goal, and its
// result reaches the digest the main agent reads. The core sees one line for it.
func TestTheBridgeRunsATaskAndKeepsItsResultInTheDigest(t *testing.T) {
	trig := newFakeTrigger(true)
	r := newBridgeRigOn(t, func(c *config.Config) { c.MaxTasks = 2 }, trig)
	r.model.fn = func(first string, n int) (llm.Response, error) {
		switch {
		case strings.Contains(first, "Wake reason: task"):
			if !strings.Contains(first, "Task: Check pods") || !strings.Contains(first, "Goal:\nlist the pods in the namespace") {
				t.Errorf("task prompt: %s", first)
			}
			if strings.Contains(first, "End this turn by calling schedule_next") {
				t.Error("a task is told to schedule")
			}
			return llm.ToolUse(llm.Usage{}, llm.Call{Name: "finish", Input: `{"summary":"3 pods, all ready"}`}).Response, nil
		case n == 0:
			return llm.ToolUse(llm.Usage{},
				llm.Call{Name: "start_task", Input: `{"title":"Check pods","goal":"list the pods in the namespace"}`},
				llm.Call{Name: "schedule_next", Input: `{"after":"2h","why":"look again"}`},
				llm.Call{Name: "finish", Input: `{"summary":"started a task"}`},
			).Response, nil
		}
		return bfinish("done", "2h"), nil
	}
	r.waitFor("the task's line", func() bool {
		for _, w := range trig.wakeLines() {
			if w.Lane == "task" {
				return true
			}
		}
		return false
	})
	var line triggers.Wake
	for _, w := range trig.wakeLines() {
		if w.Lane == "task" {
			line = w
		}
	}
	if line.Reason != "task: Check pods" || line.Outcome != "turn" || line.Steps == 0 || line.Next != 0 {
		t.Fatalf("task line: %+v", line)
	}
	if line.Title != "Check pods" || line.Key == "" {
		t.Fatalf("task line names no task: %+v", line)
	}
	r.waitFor("the digest", func() bool {
		for _, d := range r.sup.led.LoadDigest() {
			if d.Reason == "task Check pods" && d.Summary == "3 pods, all ready" {
				return true
			}
		}
		return false
	})
	if n := len(r.model.turns()); n != 2 {
		t.Fatalf("turns: %d", n)
	}
}

// The core is told the work in hand with the stats: a task that runs is in the
// snapshot under the key its activity line will carry, and the snapshot after
// it ends does not list it.
func TestTheBridgeTellsTheCoreItsTasksInTheStats(t *testing.T) {
	trig := newFakeTrigger(true)
	r := newBridgeRigOn(t, func(c *config.Config) { c.MaxTasks = 2 }, trig)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	r.model.fn = func(first string, n int) (llm.Response, error) {
		switch {
		case strings.Contains(first, "Wake reason: task"):
			<-release
			return llm.ToolUse(llm.Usage{}, llm.Call{Name: "finish", Input: `{"summary":"done"}`}).Response, nil
		case n == 0:
			return llm.ToolUse(llm.Usage{},
				llm.Call{Name: "start_task", Input: `{"title":"Check pods","goal":"list the pods"}`},
				llm.Call{Name: "schedule_next", Input: `{"after":"2h","why":"look again"}`},
				llm.Call{Name: "finish", Input: `{"summary":"started a task"}`},
			).Response, nil
		}
		return bfinish("done", "2h"), nil
	}
	tasks := func() []triggers.TaskLine {
		trig.mu.Lock()
		defer trig.mu.Unlock()
		if len(trig.stats) == 0 {
			return nil
		}
		return trig.stats[len(trig.stats)-1].Tasks
	}
	var key string
	r.waitFor("the running task in the stats", func() bool {
		for _, l := range tasks() {
			if l.Title == "Check pods" && l.Lane == "task" && l.State == "running" && l.StartedMs > 0 {
				key = l.Key
				return true
			}
		}
		return false
	})
	once.Do(func() { close(release) })
	r.waitFor("the stats without the task", func() bool { return len(tasks()) == 0 })
	for _, w := range trig.wakeLines() {
		if w.Lane == "task" && w.Key != key {
			t.Fatalf("the activity line names %q, the task list %q", w.Key, key)
		}
	}
}

type noSink struct{}

func (noSink) Wake(context.Context, durable.WakeSignal) error { return nil }

// The workflow owns the schedule: a next wake that is already past (a wake that
// could not finish) must not make the supervisor read the core in a loop while
// the stream is down. It reads once per poll interval.
func TestTheBridgeDoesNotSpinOnAPastScheduleWhileTheCoreFails(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.PollInterval = 50 * time.Millisecond })
	h.core.listErr = errors.New("401 unauthorized")
	led, err := ledger.New(h.state)
	must(t, err)
	s := New(h.cfg, led, h.core, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.nextTick = s.now().Add(-time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.RunBridge(ctx, noSink{}) }()
	time.Sleep(600 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	h.core.mu.Lock()
	n := h.core.lists
	h.core.mu.Unlock()
	if n > 40 {
		t.Fatalf("the core was read %d times in 600ms with a %s poll interval", n, h.cfg.PollInterval)
	}
	if n < 3 {
		t.Fatalf("the core was read %d times: the poll stopped", n)
	}
}
