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
	// TaskQueue and WorkflowID default to names made of the spot's name.
	TaskQueue  string
	WorkflowID string
	Name       string
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
	dc, fc, err := dataConverter(o.PayloadKeyFile)
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
}

// New builds the worker on a connected client. Start runs it.
func New(c client.Client, o Options, d Deps) *Engine {
	o.defaults()
	e := &Engine{o: o, c: c, acts: NewActs(d)}
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
func (e *Engine) Start() error { return e.w.Start() }

// Close stops the worker (the turns it is running are resumed by the next
// worker) and closes what the engine opened.
func (e *Engine) Close() {
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
