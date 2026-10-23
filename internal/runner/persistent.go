package runner

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/triageagent-dev/tria-spot/internal/stream"
)

// Persistent runs the turns of a conversation in one `claude -p` process that
// stays alive between them. The supervisor writes each turn as a user message
// on the CLI's stdin (--input-format stream-json) and reads the stream until
// the turn's result; there is no process to start, no MCP connection to make
// and no transcript to load for the second turn on.
//
// It has the same Run as Runner, so the supervisor does not know which it
// has. A process is replaced when the conversation it holds is not the one
// asked for (a fresh one after a rotation, another session), when the
// arguments or the MCP file changed (a permission mode, a rotated core
// token), when it exited, or when it is old. A replacement resumes the
// session, so nothing but the start-up is lost.
//
// The wire format is the CLI's: one JSON object per line. A user message is
// {"type":"user","message":{"role":"user","content":...},"parent_tool_use_id":null,"uuid":...}
// and an interrupt is {"type":"control_request","request_id":...,"request":{"subtype":"interrupt"}}
// (the SDK types SDKUserMessage and SDKControlInterruptRequest,
// https://code.claude.com/docs/en/agent-sdk/typescript).
type Persistent struct {
	r *Runner

	mu     sync.Mutex // one turn at a time; guards p
	p      *proc
	maxAge time.Duration
}

// DefaultMaxProcessAge bounds how long one process lives; the next turn after
// it resumes the session in a new one.
const DefaultMaxProcessAge = 12 * time.Hour

// NewPersistent returns a Persistent runner.
func NewPersistent(o Options) *Persistent {
	return &Persistent{r: New(o), maxAge: DefaultMaxProcessAge}
}

// SetPermissionMode changes the mode later turns start in; a running process
// is replaced by one in that mode on the next turn.
func (p *Persistent) SetPermissionMode(m string) { p.r.SetPermissionMode(m) }

// proc is one running CLI process.
type proc struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	sig     string // what it was started with: arguments and MCP file
	args    []string
	started time.Time
	stderr  *tailBuffer
	done    chan struct{} // closed once the process has exited and its output is read

	mu      sync.Mutex
	waitErr error
	session string // the conversation it is in; empty until the first init of a fresh one
	turn    *turnState
}

// turnState collects one turn's lines.
type turnState struct {
	acc         *stream.Accumulator
	result      chan struct{} // closed on the turn's result message
	resultOnce  sync.Once
	authFailed  chan struct{} // closed when the CLI keeps retrying a 401
	authOnce    sync.Once
	authRetries int
}

func newTurn() *turnState {
	return &turnState{acc: stream.NewAccumulator(), result: make(chan struct{}), authFailed: make(chan struct{})}
}

func (p *proc) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// Run executes one turn: prompt goes to the process holding resume (or to a
// fresh conversation when resume is empty).
func (p *Persistent) Run(ctx context.Context, prompt, resume string) Result {
	start := time.Now()
	o := p.r.options()
	res := Result{}

	if err := writeMCP(o); err != nil {
		res.Err = err
		return res
	}
	for _, d := range []string{"home", "claude", "work"} {
		if err := os.MkdirAll(filepath.Join(o.StateDir, d), 0o700); err != nil {
			res.Err = err
			return res
		}
	}
	args := persistentArgs(o, resume)
	// Not the resume id: a process stays in its session, and the id it was
	// started with is not what a later turn passes.
	sig := signature(persistentArgs(o, ""), o.Paths.MCPConfig)

	p.mu.Lock()
	defer p.mu.Unlock()
	if reason := p.replaceReason(resume, sig); reason != "" {
		p.stop(o, reason)
	}
	if p.p == nil {
		np, err := p.start(o, args, sig, resume)
		if err != nil {
			res.Args = args
			res.Err = err
			return res
		}
		p.p = np
	}
	pr := p.p
	res.Args = pr.args

	turn := newTurn()
	pr.mu.Lock()
	pr.turn = turn
	pr.mu.Unlock()

	if err := pr.send(userMessage(prompt)); err != nil {
		// The process died between turns; the caller sees a failed turn and the
		// next one starts a new process.
		<-waitDone(pr, 2*time.Second)
		return p.finish(ctx, pr, turn, res, start, false, false)
	}

	cctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	var authFailed, timedOut bool
	select {
	case <-turn.result:
	case <-pr.done:
	case <-turn.authFailed:
		authFailed = true
		p.kill(pr)
	case <-cctx.Done():
		// The turn's own deadline, unless the parent ended: that is a shutdown.
		timedOut = ctx.Err() == nil
		p.interrupt(o, pr, turn)
	}
	return p.finish(ctx, pr, turn, res, start, authFailed, timedOut)
}

// finish turns the collected lines into a Result and forgets the turn.
func (p *Persistent) finish(ctx context.Context, pr *proc, turn *turnState, res Result, start time.Time, authFailed, timedOut bool) Result {
	pr.mu.Lock()
	pr.turn = nil
	sum := turn.acc.Summary()
	if sum.SessionID != "" {
		pr.session = sum.SessionID
	}
	waitErr := pr.waitErr
	pr.mu.Unlock()

	res.Summary = sum
	res.Stderr = strings.TrimSpace(pr.stderr.take())
	res.Duration = time.Since(start)
	res.AuthFailed = authFailed
	res.Canceled = ctx.Err() != nil
	res.TimedOut = timedOut
	res.CoreDown = sum.GotInit && !coreConnected(sum.MCPServers)
	if !pr.alive() {
		var ee *exec.ExitError
		if errors.As(waitErr, &ee) {
			res.ExitCode = ee.ExitCode()
		}
		if !sum.GotResult {
			res.Err = fmt.Errorf("claude exited: %w", orErr(waitErr))
		}
		p.p = nil
	}
	return res
}

func orErr(err error) error {
	if err == nil {
		return errors.New("process ended without a result")
	}
	return err
}

func (p *Persistent) replaceReason(resume, sig string) string {
	switch pr := p.p; {
	case pr == nil:
		return ""
	case !pr.alive():
		return "the process had exited"
	case pr.sig != sig:
		return "arguments or MCP configuration changed"
	case resume == "":
		return "a fresh conversation was asked for"
	case pr.currentSession() != resume:
		return "another session was asked for"
	case p.maxAge > 0 && time.Since(pr.started) > p.maxAge:
		return "the process is old"
	}
	return ""
}

func (p *proc) currentSession() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.session
}

// start launches a process. A resumed conversation takes the session id it is
// started with; a fresh one learns it from the first init.
func (p *Persistent) start(o Options, args []string, sig, resume string) (*proc, error) {
	cmd := exec.Command(o.Bin, args...)
	cmd.Dir = filepath.Join(o.StateDir, "work")
	cmd.Env = p.r.env(o)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	pr := &proc{cmd: cmd, sig: sig, args: args, started: time.Now(), stderr: &tailBuffer{max: 64 << 10},
		done: make(chan struct{}), session: resume}
	cmd.Stderr = pr.stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", o.Bin, err)
	}
	pr.stdin = stdin
	o.Logger.Info("claude process started", "resume", resume != "")
	go pr.read(stdout, o)
	return pr, nil
}

// read feeds the process's output to the current turn until it ends, then
// reaps the process.
func (p *proc) read(stdout io.Reader, o Options) {
	lr := stream.NewLineReader(stdout)
	for {
		raw, err := lr.Next()
		if len(raw) > 0 {
			if o.LogContent {
				o.Logger.Debug("claude", "line", truncate(string(raw), 600))
			}
			p.line(raw)
		}
		if err != nil {
			break
		}
	}
	werr := p.cmd.Wait()
	p.mu.Lock()
	p.waitErr = werr
	p.mu.Unlock()
	close(p.done)
}

func (p *proc) line(raw []byte) {
	p.mu.Lock()
	t := p.turn
	p.mu.Unlock()
	if t == nil {
		return // between turns: control receipts and lifecycle notes
	}
	if t.acc.Add(raw) {
		t.resultOnce.Do(func() { close(t.result) })
		return
	}
	if strings.Contains(string(raw), `"subtype":"api_retry"`) && strings.Contains(string(raw), `"authentication_failed"`) {
		if t.authRetries++; t.authRetries == maxAuthRetries {
			t.authOnce.Do(func() { close(t.authFailed) })
		}
	}
}

func (p *proc) send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = p.stdin.Write(append(b, '\n'))
	return err
}

func userMessage(prompt string) map[string]any {
	return map[string]any{
		"type":               "user",
		"message":            map[string]any{"role": "user", "content": prompt},
		"parent_tool_use_id": nil,
		"uuid":               newUUID(),
	}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// interrupt asks the CLI to abort the running turn and waits for the result
// it writes; a CLI that does not answer within the grace is killed.
func (p *Persistent) interrupt(o Options, pr *proc, turn *turnState) {
	_ = pr.send(map[string]any{
		"type":       "control_request",
		"request_id": "int-" + newUUID(),
		"request":    map[string]any{"subtype": "interrupt"},
	})
	t := time.NewTimer(o.Grace)
	defer t.Stop()
	select {
	case <-turn.result:
	case <-pr.done:
	case <-t.C:
		o.Logger.Warn("claude did not stop after an interrupt; killing it")
		p.kill(pr)
	}
}

// kill ends the process group and waits for it.
func (p *Persistent) kill(pr *proc) {
	_ = signalGroup(pr.cmd, syscall.SIGKILL)
	<-waitDone(pr, 10*time.Second)
}

func waitDone(pr *proc, d time.Duration) <-chan struct{} {
	c := make(chan struct{})
	go func() {
		select {
		case <-pr.done:
		case <-time.After(d):
		}
		close(c)
	}()
	return c
}

// stop ends the current process: stdin is closed so it can finish, then it
// is terminated after the grace.
func (p *Persistent) stop(o Options, why string) {
	pr := p.p
	p.p = nil
	if pr == nil {
		return
	}
	o.Logger.Info("claude process stopped", "why", why)
	if pr.alive() {
		_ = pr.stdin.Close()
		select {
		case <-pr.done:
			return
		case <-time.After(o.Grace):
		}
		_ = signalGroup(pr.cmd, syscall.SIGTERM)
		select {
		case <-pr.done:
			return
		case <-time.After(5 * time.Second):
		}
		_ = signalGroup(pr.cmd, syscall.SIGKILL)
		<-waitDone(pr, 5*time.Second)
	}
}

// Close stops the process. Call it after the last Run.
func (p *Persistent) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stop(p.r.options(), "shutdown")
	return nil
}

// persistentArgs is BuildArgs plus the streaming input.
func persistentArgs(o Options, resume string) []string {
	return append(BuildArgs(o, resume), "--input-format", "stream-json")
}

// signature identifies what a process was started with: its arguments and
// the MCP file it read at start (it carries the core token, which rotates).
func signature(args []string, mcpFile string) string {
	h := sha256.New()
	h.Write([]byte(strings.Join(args, "\x00")))
	b, _ := os.ReadFile(mcpFile)
	h.Write([]byte{0})
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	mu  sync.Mutex
	max int
	b   []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.b = append(t.b, p...)
	if len(t.b) > t.max {
		t.b = t.b[len(t.b)-t.max:]
	}
	return len(p), nil
}

// take returns what was written since the last take.
func (t *tailBuffer) take() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := string(t.b)
	t.b = nil
	return s
}
