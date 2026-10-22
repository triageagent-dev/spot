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
		HookBin: hookBin, KubectlWritesHr: cfg.KubectlWritesHr,
		Instruction: prompt.Instruction(prompt.Params{
			Name: cfg.Name, Tenant: cfg.Tenant, StateDir: cfg.StateDir, MinWake: cfg.MinWake, MaxWake: cfg.MaxWake,
		}),
	})
	if err != nil {
		log.Error("rendering config", "err", err)
		return 1
	}

	allowed := runner.DefaultAllowed(cfg.StateDir)
	for _, r := range strings.Split(cfg.ExtraAllowed, ",") {
		if r = strings.TrimSpace(r); r != "" {
			allowed = append(allowed, r)
		}
	}
	rn := runner.New(runner.Options{
		Bin: cfg.ClaudeBin, Model: cfg.Model, MaxTurns: cfg.MaxTurns, Timeout: cfg.TurnTimeout,
		StateDir: cfg.StateDir, Paths: paths, PermissionMode: cfg.PermissionMode,
		AllowedTools: allowed, Logger: log,
	})
	core := corecli.New(cfg.CoreMCPURL, cfg.CoreTokenFile, cfg.Tenant)
	defer core.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := supervisor.New(cfg, led, core, rn, log).Run(ctx); err != nil {
		log.Error("supervisor", "err", err)
		return 1
	}
	log.Info("stopped")
	return 0
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
