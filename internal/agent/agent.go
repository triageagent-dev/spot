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
}

// Result is what a turn did.
type Result struct {
	// Text is the final assistant text, for logs; nobody reads it.
	Text string
	// Summary is what the model passed to finish.
	Summary string
	// Next is the last schedule_next call, if any.
	Next      *ScheduleRequest
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
	TimedOut       bool
	Canceled       bool // the parent context ended (shutdown)
	AuthFailed     bool // the model API refused the credentials
	CoreDown       bool // the core's tools were not there: the turn did not run
	// Failed is set when the turn did not end well for any reason other than
	// shutdown or the step cap. Err says why.
	Failed bool
	Err    error
	// Undelivered are messages handed to Inject that the turn ended without
	// showing to the model.
	Undelivered []string
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
	// Timeout caps the wall-clock time of a turn; zero = none.
	Timeout   time.Duration
	Telemetry *telemetry.Provider
	Logger    *slog.Logger
	// LogContent lets debug logs carry tool input and the model's text.
	LogContent bool
}

// DefaultMaxSteps is the step cap when none is set.
const DefaultMaxSteps = 40

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

	a.o.Tools.Begin()
	defer a.o.Tools.End()
	defer func() {
		res.Next = a.o.Tools.Schedule()
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

	first := t.Prompt
	if down := a.o.Tools.Down(); len(down) > 0 {
		first += fmt.Sprintf("\nThe tools of these servers are not available in this turn, because they could not be reached: %s.\n", strings.Join(down, ", "))
	}
	first += fmt.Sprintf("\nYou have at most %d model steps in this turn.\n", a.o.MaxSteps)
	msgs := []llm.Message{llm.UserText(first)}
	truncated, paused := false, false

	for res.Steps < a.o.MaxSteps {
		if err := ctx.Err(); err != nil {
			res.Err = err
			return res
		}
		req := llm.Request{
			System: a.o.System, Messages: msgs, Tools: defs, MaxTokens: a.o.MaxTokens, Effort: a.o.Effort,
		}
		chat := root.Chat(a.o.Model, req)
		resp, err := a.o.LLM.Complete(ctx, req)
		chat.End(resp, err)
		if err != nil {
			res.Err = err
			if isCtxErr(ctx, err) {
				return res
			}
			res.Failed = true
			res.AuthFailed = llm.IsAuth(err)
			return res
		}
		res.Steps++
		res.Usage = res.Usage.Add(resp.Usage)
		res.ContextTokens = resp.Usage.Context()
		if resp.Model != "" {
			res.Model = resp.Model
		}
		res.Text = textOf(resp.Content)
		if a.o.LogContent && res.Text != "" {
			log.Debug("model text", "step", res.Steps, "text", clip(res.Text, 600))
		}

		switch resp.StopReason {
		case llm.StopToolUse:
			calls := toolUses(resp.Content)
			if len(calls) == 0 {
				if a.endOrInject(&msgs, resp) {
					return res
				}
				continue
			}
			msgs = append(msgs, llm.Message{Role: llm.RoleAssistant, Content: resp.Content})
			results := a.runTools(ctx, root, calls, &res)
			if _, done := a.o.Tools.Finished(); done {
				return res
			}
			if len(results) == 0 {
				return res
			}
			for _, extra := range a.take() {
				results = append(results, llm.Text(extra))
			}
			msgs = append(msgs, llm.Message{Role: llm.RoleUser, Content: results})

		case llm.StopMaxTokens:
			if truncated {
				res.Failed, res.Err = true, errors.New("the model's answer was cut at the output cap twice")
				return res
			}
			truncated = true
			// A tool call cut in the middle has no valid input: drop it. What is
			// left (thinking, text) is kept and the model is asked to go on.
			keep := withoutToolUses(resp.Content)
			if len(keep) > 0 {
				msgs = append(msgs, llm.Message{Role: llm.RoleAssistant, Content: keep})
			}
			msgs = append(msgs, llm.UserText(continueText))

		case llm.StopRefusal:
			res.Failed = true
			res.Err = errors.New("the model refused the request" + categoryText(resp.RefusalCategory))
			return res

		case llm.StopPauseTurn:
			if paused {
				res.Failed, res.Err = true, errors.New("the model paused the turn twice")
				return res
			}
			paused = true
			msgs = append(msgs, llm.Message{Role: llm.RoleAssistant, Content: resp.Content})

		default: // end_turn, and any stop with nothing left to do
			if a.endOrInject(&msgs, resp) {
				return res
			}
		}
	}
	res.StepsExhausted = true
	log.Warn("the turn stopped at the step cap", "steps", res.Steps)
	return res
}

// endOrInject ends the turn, unless a person's message arrived while the model
// was answering: then it is added to the conversation and the turn goes on.
// It reports whether the turn ends.
func (a *Agent) endOrInject(msgs *[]llm.Message, resp llm.Response) bool {
	extra := a.finishOrTake()
	if len(extra) == 0 {
		return true
	}
	if len(resp.Content) > 0 {
		*msgs = append(*msgs, llm.Message{Role: llm.RoleAssistant, Content: resp.Content})
	}
	blocks := make([]llm.Block, 0, len(extra))
	for _, e := range extra {
		blocks = append(blocks, llm.Text(e))
	}
	*msgs = append(*msgs, llm.Message{Role: llm.RoleUser, Content: blocks})
	return false
}

// runTools runs the calls of one assistant message, in order, and returns the
// tool_result blocks for all of them.
func (a *Agent) runTools(ctx context.Context, root *telemetry.Turn, calls []llm.Block, res *Result) []llm.Block {
	out := make([]llm.Block, 0, len(calls))
	for _, c := range calls {
		span := root.Tool(c.Name, c.ID, c.Input)
		began := time.Now()
		var content string
		var isErr bool
		if !json.Valid(c.Input) {
			content, isErr = "the tool input was not valid JSON", true
		} else {
			content, isErr = a.o.Tools.Call(ctx, c.Name, c.Input)
		}
		span.End(content, isErr)
		res.ToolCalls++
		attrs := []any{"tool", c.Name, "ms", time.Since(began).Milliseconds(), "error", isErr}
		if a.o.LogContent {
			attrs = append(attrs, "input", clip(string(c.Input), 600))
		}
		a.o.Logger.Debug("tool call", attrs...)
		out = append(out, llm.ToolResult(c.ID, content, isErr))
	}
	return out
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
