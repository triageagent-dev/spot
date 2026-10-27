package supervisor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"go.temporal.io/sdk/testsuite"

	"github.com/triageagent-dev/spot/internal/config"
	"github.com/triageagent-dev/spot/internal/corecli"
	"github.com/triageagent-dev/spot/internal/durable"
	"github.com/triageagent-dev/spot/internal/ledger"
	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/tools"
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
}

func newBridgeRig(t *testing.T, mut func(*config.Config)) *bridgeRig {
	t.Helper()
	return newBridgeRigOn(t, mut, nil)
}

// newBridgeRigOn is newBridgeRig with the core's push channel, when trig is set.
func newBridgeRigOn(t *testing.T, mut func(*config.Config), trig Trigger) *bridgeRig {
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
			CallTimeout: 10 * time.Second,
		})
	}
	eng := durable.New(devServer(t).Client(), durable.Options{Name: name, TaskQueue: "tq-" + name, WorkflowID: "spot-" + name, StopTimeout: 100 * time.Millisecond},
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
