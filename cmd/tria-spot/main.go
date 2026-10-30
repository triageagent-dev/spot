// Command tria-spot is the supervisor and container entrypoint of the Tria
// spot (the core's ADR-095, ADR-098): it wakes the spot's own agent loop in turns, gives it
// the core's MCP tools and its connectors, and keeps its schedule, budget and
// heartbeat.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/config"
	"github.com/triageagent-dev/spot/internal/corecli"
	"github.com/triageagent-dev/spot/internal/identity"
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
	newRegistry := func() *tools.Registry {
		return tools.New(tools.Options{
			RoomSince: func(room string) int64 {
				if sup == nil {
					return 0
				}
				return sup.RoomSince(room)
			},
			Core: coreServer(cfg), Others: otherServers(cfg),
			CallTimeout: cfg.ToolTimeout, ResultMax: cfg.ToolResultMax, SpotName: cfg.Name, RecallFirst: cfg.RecallFirst, NotesOnly: cfg.NotesOnly, PostGate: cfg.PostGate, FenceResults: cfg.FenceResults, Tasks: cfg.MaxTasks > 0, Logger: log,
		})
	}
	for _, c := range cfg.Connectors {
		log.Info("connector", "name", c.Name, "tools", len(c.Tools))
	}
	if cfg.CoreTools == "all" {
		log.Info("core tools: all of them; the core's Role is the bound")
	}

	// The core's link opens before anything is read from the core: with a
	// certificate identity the access token comes over it, and the spot joins (and
	// waits for a person to approve it) first. A deployment holds a stream; an
	// actor of Agent Substrate holds requests and answers the core on its port.
	var stream coreLink
	if cfg.CoreAPIURL != "" {
		tc, err := newLink(cfg, log)
		if err != nil {
			log.Error("core link", "err", err)
			return 1
		}
		if l, ok := tc.(*triggers.Link); ok {
			// The server listens at once and the link starts with the core's first
			// request, which the process then waits for.
			shutdown, err := serve(cfg.Listen, l.Handler(), log)
			if err != nil {
				log.Error("listen", "addr", cfg.Listen, "err", err)
				return 1
			}
			defer shutdown()
			l.Serve(ctx)
			log.Info("waiting for the core's first request")
		} else {
			go tc.Run(ctx)
		}
		stream = tc
		if cfg.CertificateIdentity() {
			log.Info("identity: certificate; waiting for the core to accept the spot", "stateDir", cfg.IdentityDir)
			if err := tc.WaitReady(ctx); err != nil {
				return 0
			}
			log.Info("identity: the core accepted the spot")
		}
	}

	core := corecli.New(cfg.CoreMCPURL, cfg.CoreTokenFile, cfg.Tenant)
	core.SetLogger(log)
	defer core.Close()

	template, err := loadInstruction(ctx, core, cfg.InstructionCell, log)
	if err != nil {
		log.Error("system prompt", "cell", cfg.InstructionCell, "err", err)
		return 1
	}
	system := []string{prompt.Instruction(template, prompt.Params{
		Name: cfg.Name, Tenant: cfg.Tenant, MinWake: cfg.MinWake, MaxWake: cfg.MaxWake,
		Connectors: promptConnectors(cfg.Connectors),
	})}

	// newAgent builds a long-lane agent: its own tool state (the gates and the
	// connections are per turn), the same model client and the usual caps.
	newAgent := func() *agent.Agent {
		return agent.New(agent.Options{
			LLM: model, Tools: newRegistry(), Model: cfg.Model, Effort: cfg.Effort, System: system,
			MaxSteps: cfg.MaxSteps, MaxToolCalls: cfg.MaxToolCalls, MaxTurnTokens: cfg.MaxTurnTokens, Timeout: cfg.TurnTimeout, Telemetry: tel, Logger: log, LogContent: cfg.LogContent,
		})
	}

	sup = supervisor.New(cfg, led, core, newAgent(), log)
	if cfg.MaxTasks > 0 {
		sup.SetTaskTurner(func() supervisor.Turner { return newAgent() })
	}
	if cfg.FastLane {
		// The short lane: its own tool state (the gates and the connections are
		// per turn), the same model client, and tighter caps.
		fast := agent.New(agent.Options{
			LLM: model, Tools: newRegistry(), Model: cfg.Model, Effort: cfg.Effort, System: system,
			MaxSteps: min(cfg.MaxSteps, cfg.FastMaxToolCalls+4), MaxToolCalls: cfg.FastMaxToolCalls, MaxTurnTokens: cfg.FastMaxTurnTokens,
			Timeout: cfg.TurnTimeout, Telemetry: tel, Logger: log, LogContent: cfg.LogContent,
		})
		sup.SetFastTurner(fast)
		log.Info("short lane on", "maxToolCalls", cfg.FastMaxToolCalls, "maxTurnTokens", cfg.FastMaxTurnTokens)
	}
	if tel != nil && cfg.Substrate() {
		sup.SetFlush(func() {
			fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := tel.Flush(fctx); err != nil {
				log.Warn("flushing spans", "err", err)
			}
		})
	}
	if stream != nil {
		sup.SetTrigger(stream)
		stream.SetControl(sup.Control)
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
	case config.AuthInjected:
		o.BaseURL = cfg.LLMBaseURL
		log.Info("model auth: a placeholder key; the egress puts the real one on the request", "baseURL", cfg.LLMBaseURL)
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

// coreLink is the core's channel to the supervisor: the websocket stream of a
// deployment, or the HTTP link of an actor.
type coreLink interface {
	supervisor.Trigger
	SetControl(triggers.ControlFunc)
	Run(context.Context)
	WaitReady(context.Context) error
}

// newLink builds the client of the core's channel: the HTTP link for a substrate
// actor; otherwise the trigger stream, with the bearer token of
// --core-token-file, or, with --core-ca-fingerprint, by certificate.
func newLink(cfg config.Config, log *slog.Logger) (coreLink, error) {
	if !cfg.CertificateIdentity() {
		return triggers.New(cfg.CoreAPIURL, cfg.Tenant, cfg.CoreTokenFile, log)
	}
	cert := triggers.Cert{
		Name: cfg.Name, Workspace: cfg.Tenant, Version: cfg.Version, TokenFile: cfg.CoreTokenFile, EnrollKeyFile: cfg.EnrollKeyFile,
	}
	open := func() (*identity.Identity, error) {
		return identity.Load(cfg.IdentityDir, cfg.Name, cfg.Tenant, cfg.CoreCAFingerprint)
	}
	if cfg.Substrate() {
		// The key is made when the core first asks, not when the process starts: an
		// actor is booted to be snapshotted too, and a key made then would be in
		// every actor made from the snapshot.
		cert.Open = open
		return triggers.NewLink(cfg.CoreAPIURL, cert, log)
	}
	id, err := open()
	if err != nil {
		return nil, err
	}
	cert.ID = id
	return triggers.NewCert(cfg.CoreAPIURL, cert, log)
}

// serve answers the core's requests and the wake-up probe on addr until the
// returned function is called. It listens before it returns, so a port in use
// fails the start instead of the probe.
func serve(addr string, h http.Handler, log *slog.Logger) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
		}
	}()
	log.Info("listening for the core", "addr", ln.Addr().String())
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}
