package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/triageagent-dev/tria-spot/internal/llm"
	"github.com/triageagent-dev/tria-spot/internal/tools"
)

// fakeTools is a Toolbox with canned tools and the real built-in behaviour of
// tools.Registry, so the loop is tested against what the supervisor reads.
type fakeTools struct {
	mu      sync.Mutex
	reg     *tools.Registry
	defs    []llm.ToolDef
	listErr error
	down    []string
	results map[string]string // tool name -> content; "!"-prefixed = error
	calls   []string
	inputs  []string
	onCall  func(name string)
}

func newFakeTools() *fakeTools {
	return &fakeTools{
		reg:     tools.New(tools.Options{Core: tools.Server{Name: "core"}}),
		defs:    []llm.ToolDef{{Name: "core__list_open_issues", InputSchema: json.RawMessage(`{"type":"object"}`)}},
		results: map[string]string{"core__list_open_issues": `{"issues":[]}`},
	}
}

func (f *fakeTools) Begin() { f.reg.Begin() }
func (f *fakeTools) End()   { f.reg.End() }
func (f *fakeTools) List(context.Context) ([]llm.ToolDef, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.defs, nil
}
func (f *fakeTools) Down() []string             { return f.down }
func (f *fakeTools) Schedule() *ScheduleRequest { return f.reg.Schedule() }
func (f *fakeTools) Finished() (string, bool)   { return f.reg.Finished() }
func (f *fakeTools) Call(ctx context.Context, name string, in json.RawMessage) (string, bool) {
	f.mu.Lock()
	f.calls = append(f.calls, name)
	f.inputs = append(f.inputs, string(in))
	cb := f.onCall
	f.mu.Unlock()
	if cb != nil {
		cb(name)
	}
	if name == tools.ScheduleNext || name == tools.Finish {
		return f.reg.Call(ctx, name, in)
	}
	r, ok := f.results[name]
	if !ok {
		return "unknown tool", true
	}
	if strings.HasPrefix(r, "!") {
		return r[1:], true
	}
	return r, false
}

func newAgent(l llm.Client, ft *fakeTools, mut func(*Options)) *Agent {
	o := Options{LLM: l, Tools: ft, System: []string{"the instruction"}, Model: "claude-x", Effort: "medium", MaxSteps: 10, Timeout: 20 * time.Second}
	if mut != nil {
		mut(&o)
	}
	return New(o)
}

var u = llm.Usage{Input: 100, Output: 10, CacheRead: 1000, CacheWrite: 50}

func TestMultiStepTurnWithToolsScheduleAndFinish(t *testing.T) {
	ft := newFakeTools()
	f := llm.NewFake(
		llm.ToolUse(u, llm.Call{Name: "core__list_open_issues", Input: `{"limit":15}`}, llm.Call{Name: "core__list_open_issues"}),
		llm.ToolUse(u, llm.Call{Name: "schedule_next", Input: `{"after":"15m","why":"check the rollout"}`}),
		llm.ToolUse(u, llm.Call{Name: "finish", Input: `{"summary":"all quiet"}`}),
	)
	res := newAgent(f, ft, nil).Run(context.Background(), Turn{Reason: "tick", Prompt: "Wake reason: tick"})

	if res.Failed || res.Err != nil || res.StepsExhausted || res.TimedOut || res.CoreDown {
		t.Fatalf("result: %+v", res)
	}
	if res.Steps != 3 || res.ToolCalls != 4 || res.Summary != "all quiet" {
		t.Errorf("steps %d calls %d summary %q", res.Steps, res.ToolCalls, res.Summary)
	}
	if res.Next == nil || res.Next.After != "15m" || res.Next.Why != "check the rollout" {
		t.Errorf("next: %+v", res.Next)
	}
	if want := (llm.Usage{Input: 300, Output: 30, CacheRead: 3000, CacheWrite: 150}); res.Usage != want {
		t.Errorf("usage: %+v", res.Usage)
	}
	if res.ContextTokens != 1150 || res.Model != "fake-model" {
		t.Errorf("context %d model %q", res.ContextTokens, res.Model)
	}
	if f.Left() != 0 {
		t.Errorf("unused steps: %d", f.Left())
	}

	reqs := f.Requests()
	if len(reqs) != 3 {
		t.Fatalf("requests: %d", len(reqs))
	}
	first := reqs[0]
	if len(first.System) != 1 || first.System[0] != "the instruction" || first.Effort != "medium" || len(first.Tools) != 1 {
		t.Errorf("first request: %+v", first)
	}
	if got := first.Messages[0].Content[0].Text; !strings.HasPrefix(got, "Wake reason: tick") || !strings.Contains(got, "at most 10 model steps") {
		t.Errorf("first message: %q", got)
	}
	// Step 2 carries the assistant message and ONE user message with BOTH results.
	second := reqs[1].Messages
	if len(second) != 3 || second[1].Role != llm.RoleAssistant || second[2].Role != llm.RoleUser {
		t.Fatalf("second request messages: %+v", second)
	}
	rs := second[2].Content
	if len(rs) != 2 || rs[0].Kind != llm.KindToolResult || rs[1].Kind != llm.KindToolResult ||
		rs[0].ToolUseID != "toolu_a" || rs[1].ToolUseID != "toolu_b" || rs[0].Content != `{"issues":[]}` {
		t.Errorf("tool results: %+v", rs)
	}
	if ft.inputs[0] != `{"limit":15}` || ft.inputs[1] != `{}` {
		t.Errorf("tool inputs: %v", ft.inputs)
	}
	// The system prompt and the tools do not change between steps.
	if fmt.Sprint(reqs[2].System, reqs[2].Tools) != fmt.Sprint(first.System, first.Tools) {
		t.Error("the cached prefix changed between steps")
	}
}

func TestEndTurnWithoutFinishNeedsNoSchedule(t *testing.T) {
	f := llm.NewFake(llm.EndTurn("nothing to do", u))
	res := newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"})
	if res.Failed || res.Steps != 1 || res.Text != "nothing to do" || res.Next != nil || res.Summary != "" {
		t.Errorf("%+v", res)
	}
}

func TestToolErrorsGoBackToTheModel(t *testing.T) {
	ft := newFakeTools()
	ft.results["core__get_pods"] = "!forbidden"
	f := llm.NewFake(llm.ToolUse(u, llm.Call{Name: "core__get_pods"}, llm.Call{Name: "core__nope"}), llm.EndTurn("ok", u))
	res := newAgent(f, ft, nil).Run(context.Background(), Turn{Prompt: "p"})
	if res.Failed {
		t.Fatalf("a tool error must not fail the turn: %+v", res)
	}
	rs := f.Requests()[1].Messages[2].Content
	if !rs[0].IsError || rs[0].Content != "forbidden" || !rs[1].IsError {
		t.Errorf("results: %+v", rs)
	}
}

func TestInvalidToolInputIsAnErrorResult(t *testing.T) {
	ft := newFakeTools()
	f := llm.NewFake(
		llm.Step{Response: llm.Response{StopReason: llm.StopToolUse, Content: []llm.Block{{Kind: llm.KindToolUse, ID: "t1", Name: "core__list_open_issues", Input: json.RawMessage(`{"limit":`)}}}},
		llm.EndTurn("ok", u))
	res := newAgent(f, ft, nil).Run(context.Background(), Turn{Prompt: "p"})
	if res.Failed || len(ft.calls) != 0 {
		t.Errorf("calls %v result %+v", ft.calls, res)
	}
	if rs := f.Requests()[1].Messages[2].Content; !rs[0].IsError {
		t.Errorf("results: %+v", rs)
	}
}

func TestStepsExhaustedIsNotAFailure(t *testing.T) {
	var steps []llm.Step
	for i := 0; i < 5; i++ {
		steps = append(steps, llm.ToolUse(u, llm.Call{Name: "core__list_open_issues"}))
	}
	f := llm.NewFake(steps...)
	res := newAgent(f, newFakeTools(), func(o *Options) { o.MaxSteps = 3 }).Run(context.Background(), Turn{Prompt: "p"})
	if !res.StepsExhausted || res.Failed || res.Err != nil || res.Steps != 3 || res.ToolCalls != 3 {
		t.Errorf("%+v", res)
	}
	if len(f.Requests()) != 3 {
		t.Errorf("requests: %d", len(f.Requests()))
	}
}

func TestRefusalEndsTheTurnFailedWithItsCategory(t *testing.T) {
	f := llm.NewFake(llm.Step{Response: llm.Response{StopReason: llm.StopRefusal, RefusalCategory: "cyber", Usage: u}})
	res := newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"})
	if !res.Failed || res.Err == nil || !strings.Contains(res.Err.Error(), "cyber") || res.Steps != 1 || res.Usage.Output != 10 {
		t.Errorf("%+v", res)
	}
}

func TestMaxTokensContinuesOnceThenFails(t *testing.T) {
	f := llm.NewFake(
		llm.Step{Response: llm.Response{StopReason: llm.StopMaxTokens, Usage: u, Content: []llm.Block{
			llm.Text("part one"), {Kind: llm.KindToolUse, ID: "cut", Name: "core__list_open_issues", Input: json.RawMessage(`{}`)}}}},
		llm.EndTurn("part two", u))
	res := newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"})
	if res.Failed || res.Steps != 2 || res.Text != "part two" {
		t.Fatalf("%+v", res)
	}
	msgs := f.Requests()[1].Messages
	if len(msgs) != 3 || msgs[2].Content[0].Text != "continue" {
		t.Fatalf("messages: %+v", msgs)
	}
	for _, b := range msgs[1].Content {
		if b.Kind == llm.KindToolUse {
			t.Error("a tool call cut at the cap must not be sent back")
		}
	}

	f = llm.NewFake(llm.Stopped(llm.StopMaxTokens, "a", u), llm.Stopped(llm.StopMaxTokens, "b", u), llm.EndTurn("never", u))
	res = newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"})
	if !res.Failed || res.Steps != 2 || f.Left() != 1 {
		t.Errorf("a second cut must fail the turn: %+v", res)
	}
}

func TestPauseTurnIsResentOnce(t *testing.T) {
	f := llm.NewFake(llm.Stopped(llm.StopPauseTurn, "thinking", u), llm.EndTurn("done", u))
	res := newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"})
	if res.Failed || res.Steps != 2 {
		t.Errorf("%+v", res)
	}
	if m := f.Requests()[1].Messages; len(m) != 2 || m[1].Role != llm.RoleAssistant {
		t.Errorf("a paused turn is resent with the assistant message last: %+v", m)
	}
	f = llm.NewFake(llm.Stopped(llm.StopPauseTurn, "a", u), llm.Stopped(llm.StopPauseTurn, "b", u))
	if res = newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"}); !res.Failed {
		t.Errorf("a second pause fails: %+v", res)
	}
}

func TestCoreDownFailsTheTurnWithoutAModelCall(t *testing.T) {
	ft := newFakeTools()
	ft.listErr = fmt.Errorf("%w: connection refused", tools.ErrCoreDown)
	f := llm.NewFake(llm.EndTurn("x", u))
	res := newAgent(f, ft, nil).Run(context.Background(), Turn{Prompt: "p"})
	if !res.CoreDown || !res.Failed || res.Err == nil || res.Steps != 0 || len(f.Requests()) != 0 {
		t.Errorf("%+v", res)
	}
}

func TestDownConnectorsAreNamedInThePrompt(t *testing.T) {
	ft := newFakeTools()
	ft.down = []string{"example-kube"}
	f := llm.NewFake(llm.EndTurn("x", u))
	newAgent(f, ft, nil).Run(context.Background(), Turn{Prompt: "p"})
	if got := f.Requests()[0].Messages[0].Content[0].Text; !strings.Contains(got, "example-kube") || !strings.Contains(got, "could not be reached") {
		t.Errorf("prompt: %q", got)
	}
}

func TestAuthFailureIsFlagged(t *testing.T) {
	for _, status := range []int{401, 403} {
		f := llm.NewFake(llm.Step{Err: &llm.APIError{Status: status}})
		res := newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"})
		if !res.AuthFailed || !res.Failed {
			t.Errorf("%d: %+v", status, res)
		}
	}
	f := llm.NewFake(llm.Step{Err: &llm.APIError{Status: 529}})
	res := newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"})
	if res.AuthFailed || !res.Failed {
		t.Errorf("%+v", res)
	}
}

func TestTimeoutAndShutdown(t *testing.T) {
	f := llm.NewFake(llm.Step{Hang: true})
	res := newAgent(f, newFakeTools(), func(o *Options) { o.Timeout = 50 * time.Millisecond }).Run(context.Background(), Turn{Prompt: "p"})
	if !res.TimedOut || !res.Failed || res.Canceled {
		t.Errorf("timeout: %+v", res)
	}

	ctx, cancel := context.WithCancel(context.Background())
	f = llm.NewFake(llm.Step{Hang: true})
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	res = newAgent(f, newFakeTools(), nil).Run(ctx, Turn{Prompt: "p"})
	if !res.Canceled || res.TimedOut || res.Failed {
		t.Errorf("shutdown: %+v", res)
	}
}

func TestThinkingBlocksGoBackWithTheToolResults(t *testing.T) {
	f := llm.NewFake(
		llm.Step{Response: llm.Response{StopReason: llm.StopToolUse, Usage: u, Content: []llm.Block{
			{Kind: llm.KindThinking, Thinking: "hm", Signature: "sig"},
			{Kind: llm.KindToolUse, ID: "t1", Name: "core__list_open_issues", Input: json.RawMessage(`{}`)}}}},
		llm.EndTurn("ok", u))
	newAgent(f, newFakeTools(), nil).Run(context.Background(), Turn{Prompt: "p"})
	as := f.Requests()[1].Messages[1].Content
	if as[0].Kind != llm.KindThinking || as[0].Signature != "sig" {
		t.Errorf("assistant message: %+v", as)
	}
}

func TestInjectedMessageReachesTheNextStep(t *testing.T) {
	ft := newFakeTools()
	ag := (*Agent)(nil)
	ft.onCall = func(string) {
		if !ag.Inject("Someone wrote to you") {
			t.Error("a running turn takes a message")
		}
	}
	f := llm.NewFake(llm.ToolUse(u, llm.Call{Name: "core__list_open_issues"}), llm.EndTurn("ok", u))
	ag = newAgent(f, ft, nil)
	res := ag.Run(context.Background(), Turn{Prompt: "p"})
	rs := f.Requests()[1].Messages[2].Content
	if len(rs) != 2 || rs[0].Kind != llm.KindToolResult || rs[1].Kind != llm.KindText || rs[1].Text != "Someone wrote to you" {
		t.Errorf("tool results come first, then the message: %+v", rs)
	}
	if len(res.Undelivered) != 0 {
		t.Errorf("undelivered: %v", res.Undelivered)
	}
	if ag.Inject("late") {
		t.Error("no turn is running: the message is refused")
	}
}

func TestInjectedMessageBeforeEndTurnKeepsTheTurnGoing(t *testing.T) {
	var ag *Agent
	f := llm.NewFake(llm.EndTurn("done", u), llm.EndTurn("answered", u))
	ft := newFakeTools()
	ag = newAgent(&injectingLLM{Fake: f, ag: ag, at: 1, text: "Someone wrote to you"}, ft, nil)
	ag.o.LLM.(*injectingLLM).ag = ag
	res := ag.Run(context.Background(), Turn{Prompt: "p"})
	if res.Steps != 2 || res.Text != "answered" {
		t.Errorf("%+v", res)
	}
	last := f.Requests()[1].Messages
	if last[len(last)-1].Role != llm.RoleUser || last[len(last)-1].Content[0].Text != "Someone wrote to you" {
		t.Errorf("messages: %+v", last)
	}
}

// injectingLLM injects a message while request number at is being answered.
type injectingLLM struct {
	*llm.Fake
	ag   *Agent
	at   int
	n    int
	text string
}

func (i *injectingLLM) Complete(ctx context.Context, r llm.Request) (llm.Response, error) {
	i.n++
	if i.n == i.at {
		i.ag.Inject(i.text)
	}
	return i.Fake.Complete(ctx, r)
}

func TestMessageLeftOverWhenTheTurnFailsIsReturned(t *testing.T) {
	var ag *Agent
	f := llm.NewFake(llm.Step{Err: errors.New("boom")})
	ag = newAgent(&injectingLLM{Fake: f, at: 1, text: "Someone wrote to you"}, newFakeTools(), nil)
	ag.o.LLM.(*injectingLLM).ag = ag
	res := ag.Run(context.Background(), Turn{Prompt: "p"})
	if !res.Failed || len(res.Undelivered) != 1 || res.Undelivered[0] != "Someone wrote to you" {
		t.Errorf("%+v", res)
	}
}
