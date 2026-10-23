// Command tria-spot is the supervisor and container entrypoint of the Tria
// spot (ADR-095): it wakes a Claude Code agent in turns, gives it the core's
// MCP tools and a kubectl bounded by RBAC, and keeps its schedule, budget and
// heartbeat. `tria-spot hook pretool` is the Claude Code PreToolUse hook.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/triageagent-dev/tria-spot/internal/config"
	"github.com/triageagent-dev/tria-spot/internal/corecli"
	"github.com/triageagent-dev/tria-spot/internal/hook"
	"github.com/triageagent-dev/tria-spot/internal/ledger"
	"github.com/triageagent-dev/tria-spot/internal/prompt"
	"github.com/triageagent-dev/tria-spot/internal/render"
	"github.com/triageagent-dev/tria-spot/internal/runner"
	"github.com/triageagent-dev/tria-spot/internal/supervisor"
	"github.com/triageagent-dev/tria-spot/internal/triggers"
)

// Stamped by the Makefile.
var (
	version   = "dev"
	gitCommit = "unknown"
	buildDate = "unknown"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, in io.Reader, out, errOut io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "hook":
			return hookCmd(args[1:], in, out, errOut)
		case "version", "--version", "-version":
			fmt.Fprintf(out, "tria-spot %s (%s, %s)\n", version, gitCommit, buildDate)
			return 0
		case "run":
			args = args[1:]
		}
	}
	cfg, err := config.Parse(args, errOut)
	if err != nil {
		if err != flag.ErrHelp {
			fmt.Fprintln(errOut, "tria-spot:", err)
			return 2
		}
		return 0
	}
	return supervise(cfg, errOut)
}

func hookCmd(args []string, in io.Reader, out, errOut io.Writer) int {
	if len(args) == 0 || args[0] != "pretool" {
		fmt.Fprintln(errOut, "usage: tria-spot hook pretool [--state-dir DIR] [--kubectl-writes-per-hour N]")
		return 2
	}
	fs := flag.NewFlagSet("hook pretool", flag.ContinueOnError)
	fs.SetOutput(errOut)
	state := fs.String("state-dir", config.DefaultStateDir, "state directory (audit log and write counter)")
	writesCap := fs.Int("kubectl-writes-per-hour", 20, "cap on mutating kubectl commands per hour; 0 = no cap")
	if err := fs.Parse(args[1:]); err != nil {
		// A broken hook invocation must not block the agent's tools.
		return 0
	}
	return hook.Run(in, out, errOut, hook.Options{StateDir: *state, MaxPerHour: *writesCap})
}

func supervise(cfg config.Config, errOut io.Writer) int {
	reapOrphans()
	log := slog.New(slog.NewTextHandler(errOut, &slog.HandlerOptions{Level: level(cfg.LogLevel)}))
	log = log.With("spot", cfg.Name, "tenant", cfg.Tenant)
	log.Info("starting", "version", version, "commit", gitCommit)

	led, err := ledger.New(cfg.StateDir)
	if err != nil {
		log.Error("state dir", "err", err)
		return 1
	}
	hookBin := cfg.HookBin
	if hookBin == "" {
		if hookBin, err = os.Executable(); err != nil {
			log.Error("cannot find own path for the hook", "err", err)
			return 1
		}
	}
	var fed *render.Federation
	if cfg.AnthropicAuth == config.AuthFederation {
		fed = &render.Federation{IdentityTokenFile: cfg.IdentityTokenFile, RuleID: cfg.FederationRuleID,
			OrganizationID: cfg.AnthropicOrgID, ServiceAccountID: cfg.AnthropicServiceAcctID, WorkspaceID: cfg.AnthropicWorkspaceID}
	}
	if fed != nil {
		// The kubelet projects the token; absent here only means it is not mounted yet.
		if _, err := os.Stat(fed.IdentityTokenFile); err != nil {
			log.Warn("identity token file is not readable yet; the first exchange will fail until it is", "err", err)
		}
		log.Info("anthropic auth: workload identity federation", "rule", fed.RuleID, "serviceAccount", fed.ServiceAccountID)
	}
	paths, err := render.Render(render.Input{
		RunDir: cfg.RunDir, StateDir: cfg.StateDir, CoreMCPURL: cfg.CoreMCPURL,
		CoreTokenFile: cfg.CoreTokenFile, AnthropicKeyFile: cfg.AnthropicKeyFile, Federation: fed,
		HookBin: hookBin, KubectlWritesHr: cfg.KubectlWritesHr, OtelTokenFile: cfg.OtelTokenFile,
		Instruction: prompt.Instruction(prompt.Params{
			Name: cfg.Name, Tenant: cfg.Tenant, StateDir: cfg.StateDir, MinWake: cfg.MinWake, MaxWake: cfg.MaxWake,
			Connectors: promptConnectors(cfg.Connectors),
		}),
	})
	if err != nil {
		log.Error("rendering config", "err", err)
		return 1
	}

	allowed := appendRules(runner.DefaultAllowed(cfg.StateDir), cfg.ExtraAllowed)
	if cfg.LocalKubectl {
		allowed = append(allowed, runner.LocalKubectlRule)
		log.Info("local kubectl is on; the pod's ServiceAccount is the bound")
	}
	if cfg.EgoMCPURL != "" {
		allowed = append(allowed, runner.EgoAllowed()...)
	}
	var servers []render.Server
	for _, c := range cfg.Connectors {
		allowed = append(allowed, c.AllowedRules()...)
		servers = append(servers, render.Server{Name: c.Name, URL: c.URL})
		log.Info("connector", "name", c.Name, "tools", len(c.Tools))
	}
	disallowed := appendRules(runner.Disallowed(cfg.CoreTools == "all"), cfg.ExtraDisallowed)
	if !cfg.LocalKubectl {
		disallowed = append(disallowed, runner.LocalKubectlRule)
	}
	if cfg.CoreTools == "all" {
		allowed = append(allowed, runner.CoreAllTool)
		log.Info("core tools: all of them; the core's Role is the bound")
	}
	var tel *runner.Telemetry
	if cfg.OtelEndpoint != "" {
		tel = &runner.Telemetry{Endpoint: cfg.OtelEndpoint, Service: cfg.Name, Tenant: cfg.OtelTenant, TenantAttr: cfg.OtelTenantAttr, TenantName: cfg.OtelTenantName, ToolDetails: cfg.OtelToolDetails}
		log.Info("telemetry: traces to an OTLP endpoint", "endpoint", cfg.OtelEndpoint, "toolDetails", cfg.OtelToolDetails)
	}
	ropts := runner.Options{
		Bin: resolveClaudeBin(cfg.ClaudeBin, os.Getenv("KO_DATA_PATH")), Model: cfg.Model, MaxTurns: cfg.MaxTurns, Timeout: cfg.TurnTimeout,
		StateDir: cfg.StateDir, Paths: paths, PermissionMode: cfg.PermissionMode,
		AllowedTools: allowed, DisallowedTools: disallowed, Logger: log, LogContent: cfg.LogContent, Telemetry: tel,
		CoreMCPURL: cfg.CoreMCPURL, CoreTokenFile: cfg.CoreTokenFile,
		EgoMCPURL: cfg.EgoMCPURL, EgoTokenFile: cfg.OtelTokenFile, Connectors: servers,
	}
	var rn supervisor.Turner = runner.New(ropts)
	if cfg.Persistent {
		pr := runner.NewPersistent(ropts)
		defer pr.Close()
		rn = pr
		log.Info("one claude process is kept alive between turns")
	}
	core := corecli.New(cfg.CoreMCPURL, cfg.CoreTokenFile, cfg.Tenant)
	defer core.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	sup := supervisor.New(cfg, led, core, rn, log)
	if cfg.CoreAPIURL != "" {
		tc, err := triggers.New(cfg.CoreAPIURL, cfg.Tenant, cfg.CoreTokenFile, log)
		if err != nil {
			log.Error("trigger stream", "err", err)
			return 1
		}
		go tc.Run(ctx)
		sup.SetTrigger(tc)
	}
	if err := sup.Run(ctx); err != nil {
		log.Error("supervisor", "err", err)
		return 1
	}
	log.Info("stopped")
	return 0
}

// appendRules adds the comma-separated extra rules to base.
func appendRules(base []string, extra string) []string {
	for _, r := range strings.Split(extra, ",") {
		if r = strings.TrimSpace(r); r != "" {
			base = append(base, r)
		}
	}
	return base
}

// reapOrphans makes the supervisor reap re-parented children when it is PID 1
// in the container: a kubectl or shell the CLI left behind after a kill would
// otherwise stay a zombie. Only children the runner is not waiting on are
// reaped here; exec.Cmd.Wait collects its own.
func reapOrphans() {
	if os.Getpid() != 1 {
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGCHLD)
	go func() {
		for range ch {
			for {
				var ws syscall.WaitStatus
				pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
				if pid <= 0 || err != nil {
					break
				}
			}
		}
	}()
}

// resolveClaudeBin returns the CLI to run. In the ko image the CLI lives in
// kodata, so a default "claude" becomes the kodata copy when it exists; any
// other value (an explicit --claude-bin) is used as given.
func resolveClaudeBin(bin, koData string) string {
	if bin != "claude" || koData == "" {
		return bin
	}
	if p := filepath.Join(koData, "claude"); fileExists(p) {
		return p
	}
	return bin
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.Mode().IsRegular()
}

func level(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}

func promptConnectors(cs []config.Connector) []prompt.Connector {
	out := make([]prompt.Connector, 0, len(cs))
	for _, c := range cs {
		out = append(out, prompt.Connector{Name: c.Name, Hint: c.Hint})
	}
	return out
}
