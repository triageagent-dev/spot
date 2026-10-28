package durable

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

// Options say where the Temporal service is and who the spot is. Nothing here
// is a credential: the API key, the TLS key and the payload key are files.
type Options struct {
	// Address is the frontend host:port; Namespace the Temporal namespace.
	Address   string
	Namespace string
	// APIKeyFile holds a Temporal Cloud API key (read again at each request, so
	// a rotated key is taken without a restart); it turns TLS on.
	APIKeyFile string
	// TLS turns TLS on without an API key (a service of one's own).
	// TLSCertFile and TLSKeyFile are a client certificate for mutual TLS.
	TLS         bool
	TLSCertFile string
	TLSKeyFile  string
	// PayloadKeyFile holds the key that encrypts every payload before it leaves
	// the process; empty = payloads are sent as they are.
	PayloadKeyFile string
	// PayloadDir keeps every payload in this directory and sends the service
	// only a reference to it (no content, not even encrypted, leaves the
	// process); it wins over PayloadKeyFile. A payload nobody read for
	// PayloadTTL is swept.
	PayloadDir string
	// TaskQueue and WorkflowID default to names made of the spot's name.
	TaskQueue  string
	WorkflowID string
	Name       string
	// PayloadTTL is how long a kept payload lives unread; zero = 21 days, more
	// than the longest run of the spot's workflow (see maxRunAge).
	PayloadTTL time.Duration
	// StopTimeout is how long a stopping worker lets its running activities
	// finish; zero = 30 seconds.
	StopTimeout time.Duration
	Logger      *slog.Logger
}

func (o *Options) defaults() {
	if o.TaskQueue == "" {
		o.TaskQueue = "tria-spot-" + o.Name
	}
	if o.WorkflowID == "" {
		o.WorkflowID = "spot-" + o.Name
	}
	if o.PayloadTTL == 0 {
		o.PayloadTTL = 21 * 24 * time.Hour
	}
	if o.StopTimeout == 0 {
		o.StopTimeout = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

// Dial connects to the Temporal service.
func Dial(o Options) (client.Client, error) {
	o.defaults()
	dc, fc, err := converters(o.PayloadKeyFile, o.PayloadDir)
	if err != nil {
		return nil, err
	}
	co := client.Options{HostPort: o.Address, Namespace: o.Namespace, DataConverter: dc, FailureConverter: fc, Logger: sdklog.NewStructuredLogger(o.Logger)}
	if o.APIKeyFile != "" {
		file := o.APIKeyFile
		co.Credentials = client.NewAPIKeyDynamicCredentials(func(context.Context) (string, error) {
			b, err := os.ReadFile(file)
			if err != nil {
				return "", fmt.Errorf("temporal API key file: %w", err)
			}
			key := strings.TrimSpace(string(b))
			if key == "" {
				return "", errors.New("temporal API key file is empty")
			}
			return key, nil
		})
		if _, err := os.Stat(file); err != nil {
			return nil, fmt.Errorf("temporal API key file: %w", err)
		}
	}
	if o.APIKeyFile != "" || o.TLS || o.TLSCertFile != "" {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if o.TLSCertFile != "" {
			cert, err := tls.LoadX509KeyPair(o.TLSCertFile, o.TLSKeyFile)
			if err != nil {
				return nil, fmt.Errorf("temporal client certificate: %w", err)
			}
			cfg.Certificates = []tls.Certificate{cert}
		}
		co.ConnectionOptions = client.ConnectionOptions{TLS: cfg}
	}
	return client.Dial(co)
}

// Engine is the spot's worker and the way the intake reaches its workflow.
type Engine struct {
	o    Options
	c    client.Client
	w    worker.Worker
	acts *Acts
	own  bool // Close closes the client
	done chan struct{}
}

// New builds the worker on a connected client. Start runs it.
func New(c client.Client, o Options, d Deps) *Engine {
	o.defaults()
	e := &Engine{o: o, c: c, acts: NewActs(d), done: make(chan struct{})}
	e.w = worker.New(c, o.TaskQueue, worker.Options{
		WorkerStopTimeout: o.StopTimeout,
		// After a crash the workflow's next task waits this long on the dead
		// worker's own queue before any worker may take it (default 5 seconds).
		StickyScheduleToStartTimeout: 2 * time.Second,
	})
	e.w.RegisterWorkflow(SpotWorkflow)
	e.w.RegisterWorkflow(WakeWorkflow)
	e.w.RegisterActivity(e.acts)
	return e
}

// Open dials the service and builds the engine on the connection, which Close
// then closes.
func Open(o Options, d Deps) (*Engine, error) {
	c, err := Dial(o)
	if err != nil {
		return nil, fmt.Errorf("temporal: %w", err)
	}
	e := New(c, o, d)
	e.own = true
	return e, nil
}

// Start starts the worker.
func (e *Engine) Start() error {
	if err := e.w.Start(); err != nil {
		return err
	}
	if e.o.PayloadDir != "" {
		go e.sweepPayloads()
	}
	return nil
}

// sweepPayloads removes the kept payloads nobody has read for the TTL, now and
// every hour.
func (e *Engine) sweepPayloads() {
	c := &claimCodec{dir: e.o.PayloadDir}
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		if n := c.sweep(e.o.PayloadTTL); n > 0 {
			e.o.Logger.Info("payloads swept", "removed", n, "olderThan", e.o.PayloadTTL.String())
		}
		select {
		case <-e.done:
			return
		case <-t.C:
		}
	}
}

// Close stops the worker (the turns it is running are resumed by the next
// worker) and closes what the engine opened.
func (e *Engine) Close() {
	close(e.done)
	e.w.Stop()
	e.acts.shutdown()
	if e.own {
		e.c.Close()
	}
}

// WorkflowID is the id of the spot's workflow.
func (e *Engine) WorkflowID() string { return e.o.WorkflowID }

// Wake tells the spot's workflow that something wants the agent, and starts the
// workflow when it is not running.
func (e *Engine) Wake(ctx context.Context, w WakeSignal) error {
	_, err := e.c.SignalWithStartWorkflow(ctx, e.o.WorkflowID, SignalWake, w, client.StartWorkflowOptions{
		ID: e.o.WorkflowID, TaskQueue: e.o.TaskQueue,
	}, SpotWorkflow, e.o.Name, Carry{})
	return err
}

// State asks the workflow how it is doing.
func (e *Engine) State(ctx context.Context) (SpotState, error) {
	var st SpotState
	v, err := e.c.QueryWorkflow(ctx, e.o.WorkflowID, "", QueryState)
	if err != nil {
		return st, err
	}
	return st, v.Get(&st)
}

// controlWait is how long an operation waits to see its effect before it answers
// with what the spot has in hand.
const controlWait = 3 * time.Second

// Control acts on one wake of the spot (key) and answers with what the spot has
// in hand after it. A key the spot does not have is refused, so no other
// workflow can be named through it.
func (e *Engine) Control(ctx context.Context, op, key string) ControlResult {
	st, err := e.State(ctx)
	if err != nil {
		return ControlResult{Msg: "the spot's workflow does not answer: " + errText(err)}
	}
	if op == OpList {
		return ControlResult{OK: true, Work: st.Work}
	}
	var w *TaskInfo
	for i := range st.Work {
		if st.Work[i].Key == key {
			w = &st.Work[i]
		}
	}
	refuse := func(msg string) ControlResult { return ControlResult{Msg: msg, Work: st.Work} }
	switch {
	case w == nil:
		return refuse(fmt.Sprintf("%q is not running or waiting", key))
	case op == OpDrop && w.State != "queued":
		return refuse("only a task that has not started can be dropped; stop or kill it")
	case op != OpDrop && w.State != "running":
		return refuse("it has not started: drop it")
	case op == OpRestart && w.Lane != LaneTask:
		return refuse("only a task can be restarted")
	}
	switch op {
	case OpKill:
		err := e.c.TerminateWorkflow(ctx, key, "", "ended by an operator")
		var nf *serviceerror.NotFound
		if errors.As(err, &nf) {
			// Its turn has not begun (the wake is being prepared): the stop is kept
			// and cancels the turn before it starts.
			err = e.c.SignalWorkflow(ctx, e.o.WorkflowID, "", SignalControl, ControlSignal{Op: OpStop, Key: key})
		}
		if err != nil {
			return refuse("could not end it: " + errText(err))
		}
	case OpStop, OpRestart, OpDrop:
		if err := e.c.SignalWorkflow(ctx, e.o.WorkflowID, "", SignalControl, ControlSignal{Op: op, Key: key}); err != nil {
			return refuse("could not tell the workflow: " + errText(err))
		}
	default:
		return refuse(fmt.Sprintf("%q is not one of list, stop, kill, restart, drop", op))
	}
	// Wait for the effect: gone from the list, or (a restart) a different task.
	deadline := time.Now().Add(controlWait)
	for time.Now().Before(deadline) {
		time.Sleep(150 * time.Millisecond)
		now, err := e.State(ctx)
		if err != nil {
			break
		}
		st = now
		still := false
		for _, x := range st.Work {
			still = still || x.Key == key
		}
		if !still {
			return ControlResult{OK: true, Msg: op + " done", Work: st.Work}
		}
	}
	return ControlResult{OK: true, Msg: op + " asked: the turn ends at its next step", Work: st.Work}
}
