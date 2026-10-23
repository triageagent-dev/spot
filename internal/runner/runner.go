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
	// DisallowedTools is the --disallowedTools list; empty means DefaultDisallowed.
	DisallowedTools []string
	// CoreMCPURL and CoreTokenFile re-render the MCP file before every turn,
	// so the CLI always carries the token the file holds now.
	CoreMCPURL    string
	CoreTokenFile string
	// EgoMCPURL and EgoTokenFile, when set, add ego's MCP server (the tools in
	// EgoTools) to that file.
	EgoMCPURL    string
	EgoTokenFile string
	// Connectors are more MCP servers, written to the same file without a
	// credential (the connector holds its own).
	Connectors []render.Server
	// Env is appended to the scrubbed parent environment.
	Env []string
	// Grace is how long the CLI gets after SIGTERM before it is killed.
	Grace  time.Duration
	Logger *slog.Logger
	// Telemetry, when set, turns on the CLI's OpenTelemetry traces. The
	// sender's token is not here: the rendered settings carry a helper that
	// reads it from a file.
	Telemetry *Telemetry
	// LogContent lets the raw CLI lines, which carry the model's words and tool
	// output, reach the debug log.
	LogContent bool
}

// Telemetry configures the CLI's OTLP trace export (https://code.claude.com/docs/en/monitoring-usage).
type Telemetry struct {
	Endpoint    string // OTLP/HTTP base URL, for example http://ego.<ns>.svc:4318
	Service     string // service.name, gen_ai.agent.name and ego.thread (the spot is one thread across rotations)
	Tenant      string // the tenant label ego files the spans under
	TenantAttr  string // the attribute ego reads the label from (ego's --usage-tenant-attr; triage.tenant on a core's ego)
	TenantName  string // ego.tenant.name: the text ego shows for the tenant
	ToolDetails bool   // tool parameters on spans; off, names and timings only
}

// resourceAttributes is the tenant label under the attribute ego reads it from
// (ego.tenant is not one ego reads on a core's ego, so a spot sent under it lands in
// the sender's default tenant), its display name, and the spot as one agent and
// one thread across context rotations.
func (t Telemetry) resourceAttributes() string {
	attr := t.TenantAttr
	if attr == "" {
		attr = "triage.tenant"
	}
	name := t.TenantName
	if name == "" {
		name = t.Tenant
	}
	return attr + "=" + t.Tenant + ",ego.tenant.name=" + name + ",gen_ai.agent.name=" + t.Service + ",ego.thread=" + t.Service
}

// Env returns the CLI's telemetry environment. None of it is secret.
func (t Telemetry) Env() []string {
	env := []string{
		"CLAUDE_CODE_ENABLE_TELEMETRY=1",
		"CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1", // traces are beta
		"OTEL_TRACES_EXPORTER=otlp",
		"OTEL_METRICS_EXPORTER=none",
		"OTEL_LOGS_EXPORTER=none", // events carry prompts; traces are enough
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + t.Endpoint,
		"OTEL_SERVICE_NAME=" + t.Service,
		"OTEL_RESOURCE_ATTRIBUTES=" + t.resourceAttributes(),
	}
	if t.ToolDetails {
		env = append(env, "OTEL_LOG_TOOL_DETAILS=1")
	}
	return env
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
	// CoreDown is set when the init message reports the core MCP server as
	// anything but connected: the agent ran without its tools, so the turn
	// counts as failed and the heartbeat is not refreshed.
	CoreDown bool
	Stderr   string
	Duration time.Duration
	Err      error // failure to start or a non-zero exit
}

// CoreTools is the core's tools the loop needs: the ones the system prompt
// names, the read-only situation and memory tools, and the two speech tools.
// Everything else the core offers goes through the classifier in auto mode
// and is refused in dontAsk; the tools in CoreToolsDenied never run.
var CoreTools = []string{
	"get_triage_instructions", "get_tenant_situation", "get_tenant_health", "get_tenant_goals",
	"list_open_issues", "list_issues", "get_issue_detail", "get_issue_graph", "get_recent_events",
	"search_memory", "search_corpus", "get_memory", "get_memory_context", "get_memory_items", "get_memory_cell",
	"fetch_memory", "get_world_context", "get_service_analytics", "get_service_prediction",
	"inspect_service", "list_services", "list_applications", "analyze_issue", "verify_hypothesis",
	"comment_on_issue", "upsert_memory_item", "note_observation",
	"say_in_workspace", "read_workspace_room", "read_workspace_file", "set_spot_status",
}

// EgoTools is the ego tools the spot may call: the ones that read sessions,
// token usage, quality and findings. Corrections, labels, role bindings and
// running the findings loop are not here.
var EgoTools = []string{
	"list_metrics", "list_sessions", "summarize_session", "explain_session", "list_runs", "get_run_results",
	"get_usage", "get_quality", "explain_regression", "get_findings", "list_corrections",
}

// EgoAllowed returns the --allowedTools rules for EgoTools.
func EgoAllowed() []string {
	out := make([]string, 0, len(EgoTools))
	for _, t := range EgoTools {
		out = append(out, "mcp__"+render.EgoServerName+"__"+t)
	}
	return out
}

// CoreToolsDenied are the core's tools the spot must never call: they close,
// forget or re-rate issues, change tenant settings, drive the core's own
// loops, or send test traffic. The prompt says so too, but a rule the CLI
// enforces is what holds under an injected instruction.
var CoreToolsDenied = []string{
	"resolve_issue", "forget_issue", "update_issue_severity", "link_issues",
	"set_tenant_goals", "run_rsi", "emit_event", "classify_event", "decide_event", "guardrail_event",
	"test_notify", "test_push", "approve_pattern", "link_pattern", "recognize_pattern",
	"expire_memory_item", "refute_memory_item", "supersede_memory_item", "consolidate_memory",
	"build_memory", "build_knowledge", "build_rca",
}

// DefaultAllowed is the --allowedTools list: reads, the notes directory and
// next.json, the core tools the loop uses and the shell tools an
// investigation needs. A cluster is reached through a connector (an MCP
// server that holds its own credential), never through a shell: kubectl is
// not here, see LocalKubectlRule. No tool that writes an arbitrary path is here:
// echo, mkdir, sort and uniq can write files, so the agent keeps its notes
// with Edit and Write.
func DefaultAllowed(stateDir string) []string {
	state := "/" + filepath.ToSlash(stateDir)
	out := []string{
		"Read", "Glob", "Grep",
		"Edit(" + state + "/notes/**)", "Write(" + state + "/notes/**)",
		"Edit(" + state + "/next.json)", "Write(" + state + "/next.json)",
	}
	for _, t := range CoreTools {
		out = append(out, "mcp__"+render.ServerName+"__"+t)
	}
	return append(out,
		"Bash(jq *)", "Bash(ls *)", "Bash(head *)", "Bash(tail *)", "Bash(grep *)",
		"Bash(wc *)", "Bash(date *)",
	)
}

// LocalKubectlRule is the one rule that lets the spot run kubectl in its own
// pod, bounded by the pod's ServiceAccount (--local-kubectl). Off by default:
// a spot works on a cluster through its connectors.
const LocalKubectlRule = "Bash(kubectl *)"

// DefaultDisallowed is the --disallowedTools list: the web, subagents (their
// usage is not in the result the ledger counts) and the core tools the spot
// must never call.
func DefaultDisallowed() []string { return Disallowed(false) }

// Disallowed is DefaultDisallowed, or with coreAll only the web and subagents:
// the operator has chosen to give the spot every core tool Tria has, so the
// core's own grant (the ServiceAccount's Role) is what bounds it.
func Disallowed(coreAll bool) []string {
	out := []string{"WebFetch", "WebSearch", "Task", "Agent"}
	if coreAll {
		return out
	}
	for _, t := range CoreToolsDenied {
		out = append(out, "mcp__"+render.ServerName+"__"+t)
	}
	return out
}

// CoreAllTool is the rule that allows every tool of the core's MCP server.
var CoreAllTool = "mcp__" + render.ServerName + "__*"

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
	}
	disallowed := o.DisallowedTools
	if len(disallowed) == 0 {
		disallowed = DefaultDisallowed()
	}
	a = append(a, "--disallowedTools", strings.Join(disallowed, ","))
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
	// The image carries the CLI, kubectl and jq as ko kodata. Put that
	// directory first on the CLI's PATH so the agent's shell finds them.
	if o.Telemetry != nil {
		extra = append(extra, o.Telemetry.Env()...)
	}
	if kd := os.Getenv("KO_DATA_PATH"); kd != "" {
		extra = append(extra, "PATH="+kd+string(os.PathListSeparator)+os.Getenv("PATH"))
	}
	// Federation: the profile rendered under the run dir. Only these two
	// ANTHROPIC_* variables reach the CLI; the ids and the token path are in the
	// profile file, not the environment.
	if o.Paths.AnthropicConfigDir != "" {
		extra = append(extra, "ANTHROPIC_CONFIG_DIR="+o.Paths.AnthropicConfigDir, "ANTHROPIC_PROFILE="+o.Paths.Profile)
	}
	return ScrubEnv(os.Environ(), append(extra, o.Env...)...)
}

// writeMCP renders the MCP file with the core token the file holds now.
func writeMCP(o Options) error {
	if o.CoreTokenFile == "" {
		return nil
	}
	var extra []render.Server
	if o.EgoMCPURL != "" {
		extra = append(extra, render.Server{Name: render.EgoServerName, URL: o.EgoMCPURL, TokenFile: o.EgoTokenFile})
	}
	extra = append(extra, o.Connectors...)
	return render.WriteMCP(o.Paths.MCPConfig, o.CoreMCPURL, o.CoreTokenFile, extra...)
}

// Run executes one turn with prompt on stdin, resuming resume if not empty.
func (r *Runner) Run(ctx context.Context, prompt, resume string) Result {
	start := time.Now()
	o := r.options()
	res := Result{Args: BuildArgs(o, resume)}
	for _, d := range []string{"home", "claude", "work"} {
		if err := os.MkdirAll(filepath.Join(o.StateDir, d), 0o700); err != nil {
			res.Err = err
			return res
		}
	}
	// The token the file holds now, every turn.
	if err := writeMCP(o); err != nil {
		res.Err = err
		return res
	}

	cctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, o.Bin, res.Args...)
	// An empty directory of its own: the CLI reads project settings and
	// instructions from its working directory, and the agent cannot write here.
	cmd.Dir = filepath.Join(o.StateDir, "work")
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
		// A raw stream-json line holds the model's words and tool output.
		if o.LogContent {
			o.Logger.Debug("claude", "line", truncate(string(l), 600))
		}
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
	if res.Stderr != "" && o.LogContent {
		o.Logger.Debug("claude stderr", "stderr", truncate(res.Stderr, 1000))
	}
	res.CoreDown = sum.GotInit && !coreConnected(sum.MCPServers)
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

// coreConnected reports whether the init message listed the core MCP server
// as connected.
func coreConnected(servers []stream.MCPServer) bool {
	for _, s := range servers {
		if s.Name == render.ServerName {
			return s.Status == "connected"
		}
	}
	return false
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
