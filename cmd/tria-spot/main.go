// Command tria-spot is the supervisor and container entrypoint of the Tria
// spot (the core's ADR-095, ADR-098): it wakes the spot's own agent loop in turns, gives it
// the core's MCP tools and its connectors, and keeps its schedule, budget and
// heartbeat.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/config"
	"github.com/triageagent-dev/spot/internal/corecli"
	"github.com/triageagent-dev/spot/internal/ledger"
	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/prompt"
	"github.com/triageagent-dev/spot/internal/supervisor"
	"github.com/triageagent-dev/spot/internal/telemetry"
	"github.com/triageagent-dev/spot/internal/tools"
	"github.com/triageagent-dev/spot/internal/triggers"
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
		case "version", "--version", "-version":
			_, _ = fmt.Fprintf(out, "tria-spot %s (%s, %s)\n", version, gitCommit, buildDate)
			return 0
		case "run":
			args = args[1:]
		}
	}
	cfg, err := config.Parse(args, errOut)
	if err != nil {
		if err != flag.ErrHelp {
			_, _ = fmt.Fprintln(errOut, "tria-spot:", err)
			return 2
		}
		return 0
	}
	return supervise(cfg, errOut)
}

func supervise(cfg config.Config, errOut io.Writer) int {
	log := slog.New(slog.NewTextHandler(errOut, &slog.HandlerOptions{Level: level(cfg.LogLevel)}))
	log = log.With("spot", cfg.Name, "tenant", cfg.Tenant)
	log.Info("starting", "version", version, "commit", gitCommit)
	cfg.Version = version

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	led, err := ledger.New(cfg.StateDir)
	if err != nil {
		log.Error("state dir", "err", err)
		return 1
	}

	model, err := newModel(ctx, cfg, log)
	if err != nil {
		log.Error("model client", "err", err)
		return 1
	}

	tel, err := telemetry.New(ctx, telemetry.Options{
		Endpoint: cfg.OtelEndpoint, TokenFile: cfg.OtelTokenFile, Name: cfg.Name,
		Tenant: cfg.OtelTenant, TenantAttr: cfg.OtelTenantAttr, TenantName: cfg.OtelTenantName,
		Content: cfg.OtelContent, Provider: cfg.LLMProvider,
	})
	if err != nil {
		log.Error("telemetry", "err", err)
		return 1
	}
	if tel != nil {
		log.Info("telemetry: traces to an OTLP endpoint", "endpoint", cfg.OtelEndpoint, "content", cfg.OtelContent)
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := tel.Shutdown(sctx); err != nil {
				log.Warn("flushing spans at shutdown", "err", err)
			}
		}()
	}

	var sup *supervisor.Supervisor // set below; the registry asks it for the rooms's watermark
	registry := tools.New(tools.Options{
		RoomSince: func(room string) int64 {
			if sup == nil {
				return 0
			}
			return sup.RoomSince(room)
		},
		Core: coreServer(cfg), Others: otherServers(cfg),
		CallTimeout: cfg.ToolTimeout, ResultMax: cfg.ToolResultMax, SpotName: cfg.Name, RecallFirst: cfg.RecallFirst, NotesOnly: cfg.NotesOnly, PostGate: cfg.PostGate, FenceResults: cfg.FenceResults, Logger: log,
	})
	for _, c := range cfg.Connectors {
		log.Info("connector", "name", c.Name, "tools", len(c.Tools))
	}
	if cfg.CoreTools == "all" {
		log.Info("core tools: all of them; the core's Role is the bound")
	}

	core := corecli.New(cfg.CoreMCPURL, cfg.CoreTokenFile, cfg.Tenant)
	defer core.Close()

	template, err := loadInstruction(ctx, core, cfg.InstructionCell, log)
	if err != nil {
		log.Error("system prompt", "cell", cfg.InstructionCell, "err", err)
		return 1
	}

	ag := agent.New(agent.Options{
		LLM: model, Tools: registry, Model: cfg.Model, Effort: cfg.Effort,
		System: []string{prompt.Instruction(template, prompt.Params{
			Name: cfg.Name, Tenant: cfg.Tenant, MinWake: cfg.MinWake, MaxWake: cfg.MaxWake,
			Connectors: promptConnectors(cfg.Connectors),
		})},
		MaxSteps: cfg.MaxSteps, MaxToolCalls: cfg.MaxToolCalls, MaxTurnTokens: cfg.MaxTurnTokens, Timeout: cfg.TurnTimeout, Telemetry: tel, Logger: log, LogContent: cfg.LogContent,
	})

	sup = supervisor.New(cfg, led, core, ag, log)
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

// newModel builds the model client for the configured provider and auth mode.
// It fails at once on a key file it cannot read or a mode with missing ids.
func newModel(ctx context.Context, cfg config.Config, log *slog.Logger) (llm.Client, error) {
	log.Info("model", "provider", cfg.LLMProvider, "id", cfg.Model, "effort", cfg.Effort, "maxSteps", cfg.MaxSteps, "maxToolCalls", cfg.MaxToolCalls, "maxTurnTokens", cfg.MaxTurnTokens)
	if cfg.LLMProvider == config.ProviderGemini {
		o := llm.GeminiOptions{Auth: cfg.LLMAuth, Model: cfg.Model, KeyFile: cfg.GeminiKeyFile}
		if cfg.LLMAuth == config.AuthVertex {
			o.VertexProject, o.VertexLocation = cfg.VertexProject, cfg.VertexRegion
			log.Info("model auth: Gemini on Google Vertex AI with application default credentials", "project", cfg.VertexProject, "location", cfg.VertexRegion)
		} else {
			log.Info("model auth: Gemini API key from a file")
		}
		o.Cache = true
		return llm.NewGemini(ctx, o)
	}
	o := llm.Options{Auth: cfg.LLMAuth, Model: cfg.Model, KeyFile: cfg.AnthropicKeyFile}
	switch cfg.LLMAuth {
	case config.AuthFederation:
		o.Federation = llm.Federation{
			RuleID: cfg.FederationRuleID, OrganizationID: cfg.AnthropicOrgID, ServiceAccountID: cfg.AnthropicServiceAcctID,
			WorkspaceID: cfg.AnthropicWorkspaceID, IdentityTokenFile: cfg.IdentityTokenFile,
		}
		// The kubelet projects the token; absent here only means it is not mounted yet.
		if _, err := os.Stat(cfg.IdentityTokenFile); err != nil {
			log.Warn("identity token file is not readable yet; the first exchange will fail until it is", "err", err)
		}
		log.Info("model auth: Anthropic workload identity federation", "rule", cfg.FederationRuleID, "serviceAccount", cfg.AnthropicServiceAcctID)
	case config.AuthVertex:
		o.VertexProject, o.VertexRegion = cfg.VertexProject, cfg.VertexRegion
		log.Info("model auth: Google Vertex AI with application default credentials", "project", cfg.VertexProject, "region", cfg.VertexRegion)
	default:
		log.Info("model auth: API key from a file")
	}
	return llm.NewAnthropic(ctx, o)
}

// coreServer is the core's MCP server as the tool layer sees it: its tools
// allowed by name and the destructive ones denied by name, or with
// --core-tools all every tool it offers.
func coreServer(cfg config.Config) tools.Server {
	s := tools.Server{Name: tools.CoreServer, URL: cfg.CoreMCPURL, TokenFile: cfg.CoreTokenFile}
	if cfg.CoreTools == "all" {
		s.AllowAll = true
		return s
	}
	s.Allow, s.Deny = tools.CoreTools, tools.CoreToolsDenied
	return s
}

// otherServers are ego, when configured, and the connectors. A connector holds
// its own credentials, so the spot sends it none.
func otherServers(cfg config.Config) []tools.Server {
	var out []tools.Server
	if cfg.EgoMCPURL != "" {
		out = append(out, tools.Server{Name: tools.EgoServer, URL: cfg.EgoMCPURL, TokenFile: cfg.OtelTokenFile, Allow: tools.EgoTools})
	}
	for _, c := range cfg.Connectors {
		out = append(out, tools.Server{Name: c.Name, URL: c.URL, Allow: c.Tools})
	}
	return out
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
		out = append(out, prompt.Connector{Name: c.Name, Hint: c.Hint, Tools: c.Tools})
	}
	return out
}

// instructionWait bounds how long the spot waits for the core to hand over its
// system prompt before it gives up and lets the pod restart.
const (
	instructionWait  = 2 * time.Minute
	instructionRetry = 10 * time.Second
)

// loadInstruction reads the system prompt, a memory cell of the core, once at
// start. The text is not in the image: without the cell the spot does not run.
func loadInstruction(ctx context.Context, core *corecli.Client, id string, log *slog.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, instructionWait)
	defer cancel()
	for {
		text, err := core.MemoryCellContent(ctx, id)
		if err == nil {
			sum := sha256.Sum256([]byte(text))
			log.Info("system prompt read from the core", "cell", id, "bytes", len(text), "sha256", hex.EncodeToString(sum[:6]))
			return text, nil
		}
		log.Warn("system prompt not read yet", "cell", id, "err", err)
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(instructionRetry):
		}
	}
}
