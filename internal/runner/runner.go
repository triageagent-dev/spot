// Package runner runs one `claude -p` turn: it builds the argument list,
// feeds the prompt on stdin, keeps credentials out of the child's
// environment, enforces the wall-clock cap, and parses the stream-json
// output.
//
// Flags used, all from https://code.claude.com/docs/en/cli-reference:
// -p, --output-format stream-json, --verbose, --permission-mode,
// --permission-prompts, --allowedTools, --disallowedTools, --mcp-config,
// --strict-mcp-config, --settings, --append-system-prompt-file, --max-turns,
// --model, --resume. It never passes --bare: bare mode ignores profiles and
// federation variables (https://code.claude.com/docs/en/authentication).
package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/triageagent-dev/tria-spot/internal/render"
	"github.com/triageagent-dev/tria-spot/internal/stream"
)

// Options configure a Runner.
type Options struct {
	Bin            string
	Model          string
	MaxTurns       int
	Timeout        time.Duration
	StateDir       string
	Paths          render.Paths
	PermissionMode string
	AllowedTools   []string
	// Env is appended to the scrubbed parent environment.
	Env []string
	// Grace is how long the CLI gets after SIGTERM before it is killed.
	Grace  time.Duration
	Logger *slog.Logger
}

// Runner runs turns.
type Runner struct {
	mu sync.Mutex
	o  Options
}

// New returns a Runner.
func New(o Options) *Runner {
	if o.Grace == 0 {
		o.Grace = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Runner{o: o}
}

// SetPermissionMode changes the mode later turns start in.
func (r *Runner) SetPermissionMode(m string) {
	r.mu.Lock()
	r.o.PermissionMode = m
	r.mu.Unlock()
}

func (r *Runner) options() Options {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.o
}

// maxAuthRetries is how many 401 retries the CLI reports before the turn is
// stopped.
const maxAuthRetries = 4

// Result is the outcome of one invocation.
type Result struct {
	Summary  stream.Summary
	Args     []string
	ExitCode int
	TimedOut bool
	Canceled bool // the parent context ended (shutdown)
	// AuthFailed is set when the CLI kept retrying a 401: the key in the
	// Secret is wrong or revoked. The runner stops the turn instead of letting
	// the CLI back off for the whole turn timeout.
	AuthFailed bool
	Stderr     string
	Duration   time.Duration
	Err        error // failure to start or a non-zero exit
}

// DefaultAllowed is the --allowedTools list: reads, the notes directory, the
// core's MCP tools and the shell tools an investigation needs. kubectl is
// allowed as a whole because the ServiceAccount's RBAC, not a rule here, is
// what bounds it; the PreToolUse hook counts the writes.
func DefaultAllowed(stateDir string) []string {
	return []string{
		"Read", "Glob", "Grep",
		"Edit(/" + filepath.ToSlash(stateDir) + "/**)",
		"mcp__" + render.ServerName + "__*",
		"Bash(kubectl *)",
		"Bash(jq *)", "Bash(ls *)", "Bash(head *)", "Bash(tail *)", "Bash(grep *)",
		"Bash(wc *)", "Bash(sort *)", "Bash(uniq *)", "Bash(date *)", "Bash(echo *)",
		"Bash(mkdir *)",
	}
}

// BuildArgs returns the CLI arguments for a turn. resume is a session id or
// empty for a new conversation. The prompt is not here: it goes on stdin.
func BuildArgs(o Options, resume string) []string {
	a := []string{
		"-p",
		"--output-format", "stream-json",
		"--verbose",
		"--permission-mode", o.PermissionMode,
		"--permission-prompts", "none",
		"--mcp-config", o.Paths.MCPConfig,
		"--strict-mcp-config",
		"--settings", o.Paths.Settings,
		"--append-system-prompt-file", o.Paths.Instruction,
		"--max-turns", fmt.Sprint(o.MaxTurns),
		"--disallowedTools", "WebFetch,WebSearch",
	}
	if o.Model != "" {
		a = append(a, "--model", o.Model)
	}
	if resume != "" {
		a = append(a, "--resume", resume)
	}
	if len(o.AllowedTools) > 0 {
		a = append(a, "--allowedTools", strings.Join(o.AllowedTools, ","))
	}
	return a
}

// ScrubEnv drops from env every variable that looks like a credential or that
// selects a credential source, so the agent's shell never inherits one by
// accident and nothing shadows the configured auth mode, then appends extra.
func ScrubEnv(env []string, extra ...string) []string {
	var out []string
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		if secretish(name) {
			continue
		}
		out = append(out, kv)
	}
	return append(out, extra...)
}

func secretish(name string) bool {
	u := strings.ToUpper(name)
	// ANTHROPIC_* covers the key, the bearer token, the profile and every
	// federation variable; CLAUDE_CODE_USE_* would pick a cloud provider, which
	// outranks everything. Federation's own variables are set by env(), after
	// this scrub, from the rendered profile.
	if strings.HasPrefix(u, "ANTHROPIC_") || strings.HasPrefix(u, "CLAUDE_CODE_OAUTH") || strings.HasPrefix(u, "CLAUDE_CODE_USE_") {
		return true
	}
	for _, s := range []string{"KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"} {
		if strings.Contains(u, s) {
			return true
		}
	}
	return false
}

// Env is the environment the CLI runs in: the scrubbed parent environment
// plus a writable HOME and config directory under the state volume and a few
// switches that keep the CLI quiet and stateless where that matters.
func (r *Runner) Env() []string {
	return r.env(r.options())
}

func (r *Runner) env(o Options) []string {
	extra := []string{
		"HOME=" + filepath.Join(o.StateDir, "home"),
		"CLAUDE_CONFIG_DIR=" + filepath.Join(o.StateDir, "claude"),
		"DISABLE_AUTOUPDATER=1",
		"DISABLE_ERROR_REPORTING=1",
		"CLAUDE_CODE_DISABLE_AUTO_MEMORY=1",
	}
	// Federation: the profile rendered under the run dir. Only these two
	// ANTHROPIC_* variables reach the CLI; the ids and the token path are in the
	// profile file, not the environment.
	if o.Paths.AnthropicConfigDir != "" {
		extra = append(extra, "ANTHROPIC_CONFIG_DIR="+o.Paths.AnthropicConfigDir, "ANTHROPIC_PROFILE="+o.Paths.Profile)
	}
	return ScrubEnv(os.Environ(), append(extra, o.Env...)...)
}

// Run executes one turn with prompt on stdin, resuming resume if not empty.
func (r *Runner) Run(ctx context.Context, prompt, resume string) Result {
	start := time.Now()
	o := r.options()
	res := Result{Args: BuildArgs(o, resume)}
	for _, d := range []string{"home", "claude"} {
		if err := os.MkdirAll(filepath.Join(o.StateDir, d), 0o700); err != nil {
			res.Err = err
			return res
		}
	}

	cctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, o.Bin, res.Args...)
	cmd.Dir = o.StateDir
	cmd.Env = r.env(o)
	cmd.Stdin = strings.NewReader(prompt)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return signalGroup(cmd, syscall.SIGTERM) }
	cmd.WaitDelay = o.Grace
	var stderr bytes.Buffer
	cmd.Stderr = &limitedWriter{w: &stderr, n: 64 << 10}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		res.Err = err
		return res
	}
	if err := cmd.Start(); err != nil {
		res.Err = fmt.Errorf("start %s: %w", o.Bin, err)
		return res
	}
	authRetries := 0
	sum, perr := stream.Parse(stdout, func(l []byte) {
		o.Logger.Debug("claude", "line", truncate(string(l), 600))
		if bytes.Contains(l, []byte(`"subtype":"api_retry"`)) && bytes.Contains(l, []byte(`"authentication_failed"`)) {
			if authRetries++; authRetries == maxAuthRetries {
				res.AuthFailed = true
				cancel()
			}
		}
	})
	if perr != nil {
		o.Logger.Warn("reading claude output", "err", perr)
	}
	werr := cmd.Wait()
	res.Summary = sum
	res.Stderr = strings.TrimSpace(stderr.String())
	res.Duration = time.Since(start)
	res.TimedOut = errors.Is(cctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil && !res.AuthFailed
	if res.Stderr != "" {
		o.Logger.Debug("claude stderr", "stderr", truncate(res.Stderr, 1000))
	}
	res.Canceled = ctx.Err() != nil
	if werr != nil {
		var ee *exec.ExitError
		if errors.As(werr, &ee) {
			res.ExitCode = ee.ExitCode()
		}
		res.Err = fmt.Errorf("claude exited: %w", werr)
	}
	return res
}

func signalGroup(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, sig); err != nil {
		return cmd.Process.Signal(sig)
	}
	return nil
}

type limitedWriter struct {
	w io.Writer
	n int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.n <= 0 {
		return len(p), nil
	}
	q := p
	if len(q) > l.n {
		q = q[:l.n]
	}
	l.n -= len(q)
	_, _ = l.w.Write(q)
	return len(p), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
