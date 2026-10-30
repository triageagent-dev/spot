// Package agent runs one turn of the spot: the loop that sends a wake prompt to
// the model, runs the tools it calls and sends the results back, until the
// model is done, the step cap is reached or the turn's time is up. Every turn
// is an empty conversation: nothing is resumed.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/telemetry"
	"github.com/triageagent-dev/spot/internal/tools"
)

// ScheduleRequest is what the model asked schedule_next for.
type ScheduleRequest = tools.ScheduleRequest

// Toolbox is what the loop needs from internal/tools. *tools.Registry is one.
type Toolbox interface {
	// Begin starts a turn, End closes what it opened.
	Begin()
	End()
	// List returns the tools of the turn; an error wrapping tools.ErrCoreDown
	// means the core's tools are not there.
	List(ctx context.Context) ([]llm.ToolDef, error)
	// Call runs a tool. It never fails the turn.
	Call(ctx context.Context, name string, input json.RawMessage) (content string, isError bool)
	// Down names the servers left out of the turn.
	Down() []string
	// Schedule and Finished report what the built-in tools recorded.
	Schedule() *ScheduleRequest
	Finished() (summary string, ok bool)
}

// Turn is one wake.
type Turn struct {
	Reason string
	// Prompt is the first user message, built by internal/prompt.
	Prompt string
	// Room is where the spot shows that it is working; the loop does not use it.
	Room string
	// Limited gives the turn only the notes, room, post and status tools and
	// the built-ins, when the toolbox can narrow itself.
	Limited bool
	// Task marks a task's turn, which cannot schedule a wake or start a task,
	// when the toolbox can say so.
	Task bool
	// NotesRead says the supervisor read the spot's standing notes for this wake
	// and put them in the prompt, so the toolbox need not hold the model's first
	// call back until it searches them again.
	NotesRead bool
}

// Result is what a turn did.
type Result struct {
	// Text is the final assistant text, for logs; nobody reads it.
	Text string
	// Summary is what the model passed to finish.
	Summary string
	// Next is the last schedule_next call, if any.
	Next *ScheduleRequest
	// Tasks are the tasks the turn asked start_task for, to be started when it
	// ends.
	Tasks []tools.TaskRequest
	// Trace is one line per model request and tool call of the turn, for the
	// core's view of a run: names and numbers, never arguments or results.
	Trace     []Step
	Steps     int // model requests made
	ToolCalls int
	Usage     llm.Usage
	// ContextTokens is the size of the conversation at the last request.
	ContextTokens int64
	Model         string
	Duration      time.Duration
	// StepsExhausted is set when the turn stopped at the step cap with work
	// unfinished. It is the work stopping, not a failure.
	StepsExhausted bool
	// ToolLimit is set when the turn ended because the model kept calling
	// tools after it was told the tool-call cap was reached. Work stopping,
	// not a failure.
	ToolLimit bool
	// TokenLimit is set, with ToolLimit, when the turn ended at the token cap
	// and not at the tool-call cap.
	TokenLimit bool
	TimedOut   bool
	Canceled   bool // the parent context ended (shutdown)
	AuthFailed bool // the model API refused the credentials
	CoreDown   bool // the core's tools were not there: the turn did not run
	// Failed is set when the turn did not end well for any reason other than
	// shutdown or the step cap. Err says why.
	Failed bool
	Err    error
	// Undelivered are messages handed to Inject that the turn ended without
	// showing to the model.
	Undelivered []string
}

// Step is one model request or tool call of a turn.
type Step struct {
	Kind   string // model | tool | cut
	Name   string
	Ms     int64
	Tokens int64 // billable tokens of a model request
}

// Trace limits: a turn keeps the first MaxTrace-1 lines and, when there were
// more, a last line of kind "cut" that says how many were left out.
const (
	MaxTrace     = 40
	maxTraceName = 64
)

type trace struct {
	mu    sync.Mutex
	lines []Step
	extra int
}

func (t *trace) add(kind, name string, began time.Time, tokens int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.lines) >= MaxTrace-1 {
		t.extra++
		return
	}
	if len(name) > maxTraceName {
		name = strings.ToValidUTF8(name[:maxTraceName], "")
	}
	t.lines = append(t.lines, Step{Kind: kind, Name: name, Ms: time.Since(began).Milliseconds(), Tokens: tokens})
}

func (t *trace) done() []Step {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := append([]Step(nil), t.lines...)
	if t.extra > 0 {
		out = append(out, Step{Kind: "cut", Name: fmt.Sprintf("%d cut", t.extra)})
	}
	return out
}

// Options configure an Agent.
type Options struct {
	LLM   llm.Client
	Tools Toolbox
	// System is the instruction. It must not change between wakes: it is the
	// cached prefix.
	System []string
	// Model is the name used for spans and for Result.Model until the API names
	// the model it used.
	Model     string
	Effort    string
	MaxTokens int
	// MaxSteps caps the model requests of a turn (default 40).
	MaxSteps int
	// MaxToolCalls caps the tool calls of a turn (default 20). Every step
	// sends the whole conversation again, so a long run of calls is what makes
	// a turn expensive. schedule_next and finish are never refused.
	MaxToolCalls int
	// MaxTurnTokens caps the billable tokens (input without cache reads, output,
	// cache writes) of a turn; zero = none. Past it the tools are refused like
	// at the tool-call cap and the model is told to wrap up. The check runs
	// after each step, so a turn may pass it by the cost of one step.
	MaxTurnTokens int64
	// Timeout caps the wall-clock time of a turn; zero = none.
	Timeout   time.Duration
	Telemetry *telemetry.Provider
	Logger    *slog.Logger
	// LogContent lets debug logs carry tool input and the model's text.
	LogContent bool
}

// Defaults of the per-turn caps.
const (
	DefaultMaxSteps     = 40
	DefaultMaxToolCalls = 20
)

// FirstMessage is the first user message of a turn: the wake prompt, the
// servers whose tools are not there and the turn's caps, so the model knows how
// much room it has.
func FirstMessage(prompt string, down []string, c Caps) string {
	first := prompt
	if len(down) > 0 {
		first += fmt.Sprintf("\nThe tools of these servers are not available in this turn, because they could not be reached: %s.\n", strings.Join(down, ", "))
	}
	first += fmt.Sprintf("\nYou have at most %d model steps and %d tool calls in this turn.\n", c.MaxSteps, c.MaxToolCalls)
	if c.MaxTurnTokens > 0 {
		first += fmt.Sprintf("The turn also ends at about %dk tokens: every step sends the whole conversation again, so a long run costs most.\n", c.MaxTurnTokens/1000)
	}
	return first
}

// toolLimitText is the result of a tool call refused at the cap.
const toolLimitText = "not run: this turn has used its %d tool calls. Do not call more tools. " +
	"Answer from what you have, say what is still open, then call schedule_next and finish."

// tokenLimitText is the result of a tool call refused at the token cap.
const tokenLimitText = "not run: this turn has used its token budget (%d tokens, every step sends the whole conversation again). " +
	"Do not call more tools. Answer from what you have, say what is still open, then call schedule_next and finish."

// Agent runs turns, one at a time.
type Agent struct {
	o Options

	mu      sync.Mutex
	active  bool
	pending []string
}

// New returns an Agent.
func New(o Options) *Agent {
	if o.MaxSteps <= 0 {
		o.MaxSteps = DefaultMaxSteps
	}
	if o.MaxToolCalls <= 0 {
		o.MaxToolCalls = DefaultMaxToolCalls
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Agent{o: o}
}

// Inject hands a message to the running turn: the model sees it with the next
// tool results, or before the turn would have ended. It reports whether a turn
// was running to take it.
func (a *Agent) Inject(text string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.active {
		return false
	}
	a.pending = append(a.pending, text)
	return true
}

func (a *Agent) take() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := a.pending
	a.pending = nil
	return p
}

// finishOrTake ends the injection window if nothing is waiting, else returns
// what is. The check and the closing are one step, so a message is either seen
// by the turn or refused by Inject.
func (a *Agent) finishOrTake() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.pending) == 0 {
		a.active = false
		return nil
	}
	p := a.pending
	a.pending = nil
	return p
}

func (a *Agent) open() {
	a.mu.Lock()
	a.active, a.pending = true, nil
	a.mu.Unlock()
}

func (a *Agent) close() (undelivered []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active = false
	undelivered, a.pending = a.pending, nil
	return undelivered
}

// continueText is what the model is told when its answer was cut at the output
// cap.
const continueText = "continue"

// Run runs one turn. It always returns: failures are in the Result.
func (a *Agent) Run(ctx context.Context, t Turn) (res Result) {
	start := time.Now()
	res.Model = a.o.Model
	log := a.o.Logger

	a.open()
	defer func() { res.Undelivered = append(res.Undelivered, a.close()...) }()
	tr := &trace{}

	parent := ctx
	if a.o.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.o.Timeout)
		defer cancel()
	}
	tctx, root := a.o.Telemetry.StartTurn(ctx, t.Reason, t.Prompt)
	ctx = tctx
	var spanErr error
	defer func() {
		root.End(res.Text, spanErr)
		fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.o.Telemetry.Flush(fctx); err != nil {
			log.Debug("flushing spans", "err", err)
		}
	}()

	if l, ok := a.o.Tools.(interface{ Limit(bool) }); ok {
		l.Limit(t.Limited)
	}
	if k, ok := a.o.Tools.(interface{ Task(bool) }); ok {
		k.Task(t.Task)
	}
	a.o.Tools.Begin()
	defer a.o.Tools.End()
	if k, ok := a.o.Tools.(interface{ NotesRead() }); ok && t.NotesRead {
		k.NotesRead()
	}
	defer func() {
		res.Next = a.o.Tools.Schedule()
		res.Trace = tr.done()
		if k, ok := a.o.Tools.(interface{ Tasks() []tools.TaskRequest }); ok {
			res.Tasks = k.Tasks()
		}
		res.Summary, _ = a.o.Tools.Finished()
		res.Duration = time.Since(start)
		res.Canceled = parent.Err() != nil
		res.TimedOut = !res.Canceled && errors.Is(ctx.Err(), context.DeadlineExceeded)
		if res.TimedOut {
			res.Failed = true
			if res.Err == nil {
				res.Err = fmt.Errorf("the turn timed out after %s", a.o.Timeout)
			}
		}
		if res.Failed || res.Canceled {
			spanErr = res.Err
			if spanErr == nil {
				spanErr = errors.New("the turn was cut off")
			}
		}
	}()

	defs, err := a.o.Tools.List(ctx)
	if err != nil {
		res.Err = err
		res.CoreDown = errors.Is(err, tools.ErrCoreDown)
		res.Failed = !isCtxErr(ctx, err)
		return res
	}

	first := FirstMessage(t.Prompt, a.o.Tools.Down(), Caps{MaxSteps: a.o.MaxSteps, MaxToolCalls: a.o.MaxToolCalls, MaxTurnTokens: a.o.MaxTurnTokens})
	steps := Steps{
		Complete: func(req llm.Request) (llm.Response, error) {
			chat := root.Chat(a.o.Model, req)
			began := time.Now()
			resp, err := a.o.LLM.Complete(ctx, req)
			chat.End(resp, err)
			name := resp.Model
			if name == "" {
				name = a.o.Model
			}
			tr.add("model", name, began, resp.Usage.Billable())
			return resp, err
		},
		Call: func(c llm.Block) (string, bool) {
			span := root.Tool(c.Name, c.ID, c.Input)
			began := time.Now()
			content, isErr := a.o.Tools.Call(ctx, c.Name, c.Input)
			span.End(content, isErr)
			tr.add("tool", c.Name, began, 0)
			attrs := []any{"tool", c.Name, "ms", time.Since(began).Milliseconds(), "error", isErr}
			if a.o.LogContent {
				attrs = append(attrs, "input", clip(string(c.Input), 600))
			}
			log.Debug("tool call", attrs...)
			return content, isErr
		},
		Finished: func() bool { _, done := a.o.Tools.Finished(); return done },
		Stopped:  ctx.Err,
		IsStop:   func(err error) bool { return isCtxErr(ctx, err) },
		Take:     a.take, FinishOrTake: a.finishOrTake,
	}
	if a.o.LogContent {
		steps.Text = func(step int, text string) { log.Debug("model text", "step", step, "text", clip(text, 600)) }
	}
	loop := RunLoop(steps, Caps{MaxSteps: a.o.MaxSteps, MaxToolCalls: a.o.MaxToolCalls, MaxTurnTokens: a.o.MaxTurnTokens},
		Request{System: a.o.System, Tools: defs, MaxTokens: a.o.MaxTokens, Effort: a.o.Effort, Model: a.o.Model}, first, log)
	// The deferred closure above fills the rest of res from the world.
	res.Text, res.Steps, res.ToolCalls, res.Usage, res.ContextTokens, res.Model = loop.Text, loop.Steps, loop.ToolCalls, loop.Usage, loop.ContextTokens, loop.Model
	res.StepsExhausted, res.ToolLimit, res.TokenLimit, res.Failed, res.AuthFailed, res.Err = loop.StepsExhausted, loop.ToolLimit, loop.TokenLimit, loop.Failed, loop.AuthFailed, loop.Err
	return res
}

func onlyBuiltins(calls []llm.Block) bool {
	for _, c := range calls {
		if !tools.IsBuiltin(c.Name) {
			return false
		}
	}
	return true
}

func isCtxErr(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func textOf(blocks []llm.Block) string {
	var parts []string
	for _, b := range blocks {
		if b.Kind == llm.KindText {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func toolUses(blocks []llm.Block) []llm.Block {
	var out []llm.Block
	for _, b := range blocks {
		if b.Kind == llm.KindToolUse {
			out = append(out, b)
		}
	}
	return out
}

func withoutToolUses(blocks []llm.Block) []llm.Block {
	var out []llm.Block
	for _, b := range blocks {
		if b.Kind != llm.KindToolUse {
			out = append(out, b)
		}
	}
	return out
}

func categoryText(c string) string {
	if c == "" {
		return ""
	}
	return " (category " + c + ")"
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// Old tool results are cut down to their head, so a long turn does not send
// every big result again at every step: the model has read it and said what it
// took from it. The last elideKeep result messages stay whole; a result under
// elideMin bytes is never touched. A cut result keeps its closing data tag.
const (
	elideKeep = 2
	elideMin  = 1500
	elideHead = 800
)

// elideOld shortens the tool results of msgs older than the last elideKeep
// messages that carry results. It changes msgs in place, once per result, so
// the conversation prefix stays the same from then on.
func elideOld(msgs []llm.Message) {
	seen := 0
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m.Role != llm.RoleUser {
			continue
		}
		has := false
		for _, b := range m.Content {
			if b.Kind == llm.KindToolResult {
				has = true
				break
			}
		}
		if !has {
			continue
		}
		seen++
		if seen <= elideKeep {
			continue
		}
		for j, b := range m.Content {
			if b.Kind != llm.KindToolResult || len(b.Content) < elideMin || strings.Contains(b.Content, "[elided: ") {
				continue
			}
			m.Content[j].Content = elided(b.Content)
		}
	}
}

func elided(text string) string {
	head := strings.ToValidUTF8(text[:elideHead], "")
	out := fmt.Sprintf("%s\n[elided: %d of %d bytes, to save tokens; call the tool again if you need the rest]", head, len(text)-len(head), len(text))
	if strings.HasPrefix(text, "<data id=") {
		if i := strings.LastIndex(text, "\n</data id="); i >= 0 {
			out += text[i:]
		}
	}
	return out
}
