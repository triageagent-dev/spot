package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/telemetry"
	"github.com/triageagent-dev/spot/internal/tools"
)

// Deps are what the activities work with. They all live in this process.
type Deps struct {
	LLM llm.Client
	// NewTools builds the tool registry of one wake: its own connections and its
	// own gates, so wakes run side by side.
	NewTools  func() *tools.Registry
	Telemetry *telemetry.Provider
	// System is the instruction: the cached prefix of every request, so it is
	// here and not in the history.
	System    []string
	Model     string
	Effort    string
	MaxTokens int
	Host      Host
	Logger    *slog.Logger
	// LogContent lets debug logs carry tool input.
	LogContent bool
}

// Acts holds the activities. Register it with the worker.
type Acts struct {
	d Deps

	mu    sync.Mutex
	wakes map[string]*wakeTools
	stop  chan struct{}
}

// wakeTools is the tool state of one wake in this process: the connections, the
// tool list and the trace. It is rebuilt, from the list and the state the
// workflow keeps, when a call lands on a process that does not have it.
type wakeTools struct {
	reg  *tools.Registry
	defs []llm.ToolDef
	down []string
	root *telemetry.Turn
	last time.Time
}

// idleWake is how long a wake's tools stay open without a call before the
// janitor closes them: a turn whose worker died leaves them behind.
const idleWake = 10 * time.Minute

// NewActs returns the activities and starts the janitor that closes the tools
// of wakes that were left.
func NewActs(d Deps) *Acts {
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	a := &Acts{d: d, wakes: map[string]*wakeTools{}, stop: make(chan struct{})}
	go a.janitor()
	return a
}

// shutdown stops the janitor and closes what is open. It is not exported: every
// exported method of Acts is an activity.
func (a *Acts) shutdown() {
	close(a.stop)
	a.mu.Lock()
	ws := a.wakes
	a.wakes = map[string]*wakeTools{}
	a.mu.Unlock()
	for _, w := range ws {
		w.reg.End()
	}
}

func (a *Acts) janitor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.mu.Lock()
			var old []*wakeTools
			for k, w := range a.wakes {
				if time.Since(w.last) > idleWake {
					old = append(old, w)
					delete(a.wakes, k)
				}
			}
			a.mu.Unlock()
			for _, w := range old {
				w.reg.End()
				w.root.End("", errors.New("the wake's worker was lost"))
			}
		}
	}
}

// ListIn opens a wake's tools.
type ListIn struct {
	Key     string
	Reason  string
	Limited bool
}

// ListOut says which servers are down, or that the core is.
type ListOut struct {
	Down     []string
	Tools    int
	CoreDown bool
	Err      string
}

// ListTools connects to the servers and lists the tools of the wake.
func (a *Acts) ListTools(ctx context.Context, in ListIn) (ListOut, error) {
	w, err := a.open(ctx, in.Key, in.Reason, in.Limited)
	if errors.Is(err, tools.ErrCoreDown) {
		return ListOut{CoreDown: true, Err: err.Error()}, nil
	}
	if err != nil {
		return ListOut{}, err
	}
	return ListOut{Down: w.down, Tools: len(w.defs)}, nil
}

// open builds the registry of a wake and lists the tools, replacing what the
// key held.
func (a *Acts) open(ctx context.Context, key, reason string, limited bool) (*wakeTools, error) {
	reg := a.d.NewTools()
	reg.Limit(limited)
	reg.Begin()
	defs, err := reg.List(ctx)
	if err != nil {
		reg.End()
		return nil, err
	}
	// The trace has no parent when the wake moved to another process; it is a
	// trace of the rest of the turn.
	_, root := a.d.Telemetry.StartTurn(context.Background(), reason, "")
	w := &wakeTools{reg: reg, defs: defs, down: reg.Down(), root: root, last: time.Now()}
	a.mu.Lock()
	old := a.wakes[key]
	a.wakes[key] = w
	a.mu.Unlock()
	if old != nil {
		old.reg.End()
	}
	return w, nil
}

// get returns the wake's tools, building them again when this process has not
// seen the wake.
func (a *Acts) get(ctx context.Context, key, reason string, limited bool) (*wakeTools, error) {
	a.mu.Lock()
	w := a.wakes[key]
	if w != nil {
		w.last = time.Now()
	}
	a.mu.Unlock()
	if w != nil {
		return w, nil
	}
	a.d.Logger.Info("a wake's tools are built again: this process had not seen it", "wake", key)
	return a.open(ctx, key, reason, limited)
}

// ModelIn is one model request: the conversation. The instruction and the tool
// list are not in it; they are the process's.
type ModelIn struct {
	Key       string
	Reason    string
	Limited   bool
	Messages  []llm.Message
	MaxTokens int
	Effort    string
}

// ModelOut is the model's answer.
type ModelOut struct{ Resp llm.Response }

// Error types the workflow reads.
const (
	errAuth       = "auth"
	errBadRequest = "bad_request"
)

// Model sends one request. A refusal of the credentials and a request the API
// calls wrong are not retried; anything else is.
func (a *Acts) Model(ctx context.Context, in ModelIn) (ModelOut, error) {
	w, err := a.get(ctx, in.Key, in.Reason, in.Limited)
	if err != nil {
		return ModelOut{}, err
	}
	req := llm.Request{System: a.d.System, Messages: in.Messages, Tools: w.defs, MaxTokens: in.MaxTokens, Effort: in.Effort}
	var resp llm.Response
	err = withHeartbeat(ctx, func(ctx context.Context) error {
		chat := w.root.Chat(a.d.Model, req)
		var cerr error
		resp, cerr = a.d.LLM.Complete(ctx, req)
		chat.End(resp, cerr)
		return cerr
	})
	if err != nil {
		var ae *llm.APIError
		switch {
		case llm.IsAuth(err):
			return ModelOut{}, temporal.NewNonRetryableApplicationError(err.Error(), errAuth, err)
		case errors.As(err, &ae) && ae.Status >= 400 && ae.Status < 500 && ae.Status != 429 && ae.Status != 408:
			return ModelOut{}, temporal.NewNonRetryableApplicationError(err.Error(), errBadRequest, err)
		}
		return ModelOut{}, err
	}
	return ModelOut{Resp: resp}, nil
}

// ToolIn is one tool call, with the state of the gates the turn has passed.
type ToolIn struct {
	Key     string
	Reason  string
	Limited bool
	Name    string
	Input   json.RawMessage
	State   tools.State
}

// ToolOut is the result, and the state after it.
type ToolOut struct {
	Content string
	IsError bool
	State   tools.State
}

// Tool runs one call. The tool layer never fails a turn: an error is a result.
func (a *Acts) Tool(ctx context.Context, in ToolIn) (ToolOut, error) {
	w, err := a.get(ctx, in.Key, in.Reason, in.Limited)
	if err != nil {
		return ToolOut{}, err
	}
	w.reg.SetState(in.State)
	var out ToolOut
	err = withHeartbeat(ctx, func(ctx context.Context) error {
		span := w.root.Tool(in.Name, "", in.Input)
		began := time.Now()
		out.Content, out.IsError = w.reg.Call(ctx, in.Name, in.Input)
		span.End(out.Content, out.IsError)
		attrs := []any{"tool", in.Name, "ms", time.Since(began).Milliseconds(), "error", out.IsError}
		if a.d.LogContent {
			attrs = append(attrs, "input", clip(string(in.Input), 600))
		}
		a.d.Logger.Debug("tool call", attrs...)
		return nil
	})
	if err != nil {
		return ToolOut{}, err
	}
	if ctx.Err() != nil {
		// The process is stopping or the turn was cancelled: the tool layer turned that
		// into a result, but it is not one the model should read.
		return ToolOut{}, ctx.Err()
	}
	out.State = w.reg.State()
	return out, nil
}

// EndIn closes a wake's tools and its trace.
type EndIn struct {
	Key  string
	Text string
	Err  string
}

// EndWake closes the connections of the wake and ends its trace.
func (a *Acts) EndWake(ctx context.Context, in EndIn) error {
	a.mu.Lock()
	w := a.wakes[in.Key]
	delete(a.wakes, in.Key)
	a.mu.Unlock()
	if w == nil {
		return nil
	}
	w.reg.End()
	var terr error
	if in.Err != "" {
		terr = errors.New(in.Err)
	}
	w.root.End(in.Text, terr)
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := a.d.Telemetry.Flush(fctx); err != nil {
		a.d.Logger.Debug("flushing spans", "err", err)
	}
	return nil
}

// Prepare, Account and Announce are the supervisor's: the wake's head (notes,
// facts, prompt), its books and its announcement.
func (a *Acts) Prepare(_ context.Context, in PrepareIn) (PrepareOut, error) {
	return a.d.Host.Prepare(in)
}

// Account books a turn that ended.
func (a *Acts) Account(_ context.Context, in AccountIn) (AccountOut, error) {
	return a.d.Host.Account(in)
}

// Announce tells the process the next wake.
func (a *Acts) Announce(_ context.Context, in AnnounceIn) error {
	return a.d.Host.Announce(in)
}

// TypingIn shows or clears the spot's "typing…" indicator in a room.
type TypingIn struct {
	Room string
	On   bool
}

// typingEvery is how often the workflow renews the indicator; the core clears
// it after about 30 seconds without one.
const typingEvery = 20 * time.Second

// Typing sets the indicator and reports whether it could (a core without the
// tool has none, and the workflow stops asking). It is model-free and best
// effort.
func (a *Acts) Typing(ctx context.Context, in TypingIn) (bool, error) {
	room := in.Room
	if room == "" {
		room = "agents"
	}
	return a.d.Host.Typing(ctx, room, in.On) == nil, nil
}

// heartbeatEvery is how often a long activity tells Temporal it is alive; the
// workflow gives it three times that. A variable so a test can shorten it.
var heartbeatEvery = 5 * time.Second

// withHeartbeat runs f while a ticker heartbeats, so a worker that dies is
// noticed in seconds and not at the call's own timeout. When Temporal cancels
// the activity (the heartbeat is how it says so) f's context ends.
func withHeartbeat(ctx context.Context, f func(ctx context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(heartbeatEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	defer close(done)
	return f(ctx)
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// errText is what a failed activity says, without the wrapping.
func errText(err error) string {
	var ae *temporal.ApplicationError
	if errors.As(err, &ae) {
		return ae.Message()
	}
	var te *temporal.TimeoutError
	if errors.As(err, &te) {
		return fmt.Sprintf("timed out (%s)", te.TimeoutType())
	}
	return err.Error()
}
