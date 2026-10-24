// Package config holds the spot's flags. Credentials are never flag values:
// --core-token-file, --anthropic-key-file and --otel-token-file are paths.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// Config is the supervisor's complete configuration.
type Config struct {
	Tenant string
	Name   string
	// ChatName is the name people see and write to in the messenger, when it
	// is not Name: the core names a spot after its workspace's label. Empty =
	// Name.
	ChatName   string
	CoreMCPURL string
	// CoreAPIURL is the core's HTTP API, for its trigger stream (a websocket
	// the core writes to when something changed). Empty = no stream: the
	// supervisor polls every PollInterval.
	CoreAPIURL    string
	CoreTokenFile string
	// ConnectorsFile is a JSON array of Connector: more MCP servers, with the
	// tools the spot may call on each. Connectors is what it holds, loaded by Parse.
	ConnectorsFile string
	Connectors     []Connector
	// LLMProvider is the model provider: "anthropic" or "gemini". Both sit
	// behind llm.Client.
	LLMProvider string
	// LLMAuth is how the spot authenticates to the model API: "key" (an API key
	// read from a file), "federation" (Anthropic Workload Identity Federation:
	// a projected identity token is exchanged for a short-lived access token;
	// Anthropic only) or "vertex" (Claude or Gemini on Google Vertex AI through
	// the pod's Workload Identity). The ids are not secrets.
	LLMAuth                string
	AnthropicKeyFile       string
	GeminiKeyFile          string
	IdentityTokenFile      string
	FederationRuleID       string
	AnthropicOrgID         string
	AnthropicServiceAcctID string
	AnthropicWorkspaceID   string
	VertexProject          string
	// VertexRegion is the Vertex AI region or global, for either provider.
	VertexRegion string
	// Model is the model id (empty = the provider's default, applied by Parse);
	// Effort is low, medium, high, xhigh or max.
	Model  string
	Effort string
	// StateDir holds the supervisor's own files (usage, cursors, status). The
	// spot's memory is the core's; losing this directory is harmless.
	StateDir       string
	MinWake        time.Duration
	MaxWake        time.Duration
	MaxSteps       int // model requests per wake
	TurnTimeout    time.Duration
	ToolTimeout    time.Duration // one tool call
	ToolResultMax  int           // bytes of one tool result the model sees
	DailyTokenCeil int64
	PollInterval   time.Duration
	EdgeCooldown   time.Duration
	// ChatBatch is how long a room message that does not name the spot may
	// wait before it wakes a turn on its own; a message that names it wakes
	// after EdgeCooldown. MaxEdgeWakesHr caps early wakes per hour; past it,
	// edges are held for the next scheduled wake.
	ChatBatch      time.Duration
	MaxEdgeWakesHr int
	LogLevel       string
	LogContent     bool // allow message, issue and model text in debug logs

	// Telemetry: the spot's own OpenTelemetry spans go to an OTLP/HTTP
	// endpoint (ego). The sender token is a file, never an environment value.
	OtelEndpoint   string
	OtelTokenFile  string
	OtelContent    bool   // put the turn's messages, tool arguments and results on the spans (the receiver evaluates from them)
	OtelTenant     string // the tenant label of the spans (default: the spot's name, so each spot is its own tenant in ego)
	OtelTenantAttr string // the attribute ego reads the label from (ego's --usage-tenant-attr)
	OtelTenantName string // the display name ego shows for that tenant (default: the label)
	// EgoMCPURL is ego's MCP endpoint; the spot reads its sessions, usage and
	// findings there with the same sender token (--otel-token-file).
	EgoMCPURL string
	// CoreTools is "curated" (the tools the loop names, the destructive ones
	// refused) or "all" (every core tool, bounded by the ServiceAccount's Role).
	CoreTools string

	HeartbeatID string
}

// Auth modes.
const (
	AuthKey        = "key"
	AuthFederation = "federation"
	AuthVertex     = "vertex"
)

// Model providers.
const (
	ProviderAnthropic = "anthropic"
	ProviderGemini    = "gemini"
)

// Defaults.
const (
	DefaultStateDir       = "/state"
	DefaultAnthropicModel = "claude-opus-5-5"
	DefaultGeminiModel    = "gemini-3-flash-preview"
	DefaultEffort         = "medium"
)

// Parse reads args (without the program name). Output of flag errors goes to
// out.
func Parse(args []string, out io.Writer) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("tria-spot", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&c.Tenant, "tenant", "", "tenant this spot works for (required)")
	fs.StringVar(&c.Name, "name", "", "spot name; default <tenant>-spot")
	fs.StringVar(&c.ChatName, "chat-name", "", "the name the messenger shows for the spot, when it differs from --name (the core names a spot after its workspace's label); the spot answers to both; default --name")
	fs.StringVar(&c.CoreMCPURL, "core-mcp-url", "", "streamable-HTTP URL of the core's /mcp (required)")
	fs.StringVar(&c.CoreAPIURL, "core-api-url", "", "the core's HTTP API (for example http://<service>.<namespace>.svc:8080); with it the core tells the spot when something changed, over a websocket, and --poll-interval is only the fallback while that stream is down")
	fs.StringVar(&c.CoreTokenFile, "core-token-file", "", "file holding the core bearer token (required)")
	fs.StringVar(&c.ConnectorsFile, "connectors-file", "", "JSON file listing connectors, more MCP servers beside the core: [{name, url, tools, hint}]; the spot may call only the named tools and holds no credential for them; empty = none")
	fs.StringVar(&c.LLMProvider, "llm-provider", ProviderAnthropic, "the model provider: anthropic or gemini")
	fs.StringVar(&c.LLMAuth, "llm-auth", AuthKey, "how the spot authenticates to the model API: key, federation (anthropic only) or vertex")
	fs.StringVar(&c.AnthropicKeyFile, "anthropic-key-file", "", "file holding the Anthropic API key (required with --llm-provider=anthropic --llm-auth=key, rejected otherwise)")
	fs.StringVar(&c.GeminiKeyFile, "gemini-key-file", "", "file holding the Gemini API key, re-read on every request (required with --llm-provider=gemini --llm-auth=key, rejected otherwise)")
	fs.StringVar(&c.IdentityTokenFile, "anthropic-identity-token-file", "", "federation: file holding the projected identity token (JWT), re-read on every exchange")
	fs.StringVar(&c.FederationRuleID, "anthropic-federation-rule-id", "", "federation: the federation rule id (fdrl_...)")
	fs.StringVar(&c.AnthropicOrgID, "anthropic-org-id", "", "federation: the Anthropic organization id")
	fs.StringVar(&c.AnthropicServiceAcctID, "anthropic-service-account-id", "", "federation: the Anthropic service account id (svac_...)")
	fs.StringVar(&c.AnthropicWorkspaceID, "anthropic-workspace-id", "", "federation: workspace id (wrkspc_...); optional when the rule spans one workspace")
	fs.StringVar(&c.VertexProject, "vertex-project", "", "vertex: the Google Cloud project that serves the model (required with --llm-auth=vertex)")
	fs.StringVar(&c.VertexRegion, "vertex-region", "global", "vertex: the region, or global (the Vertex AI location, for either provider)")
	fs.StringVar(&c.StateDir, "state-dir", DefaultStateDir, "directory for the supervisor's own files: token usage, room cursors, status; losing it is harmless")
	fs.StringVar(&c.Model, "model", "", "the model id (on Vertex AI too, with no prefix); default claude-opus-5-5 for anthropic, gemini-3-flash-preview for gemini")
	fs.StringVar(&c.Effort, "effort", DefaultEffort, "how much effort the model puts into a step: low, medium, high, xhigh or max")
	fs.DurationVar(&c.MinWake, "min-wake", 2*time.Minute, "floor for the agent's own wake-up schedule")
	fs.DurationVar(&c.MaxWake, "max-wake", 2*time.Hour, "ceiling for the agent's own wake-up schedule")
	fs.IntVar(&c.MaxSteps, "max-steps", 40, "cap on model requests per wake")
	fs.DurationVar(&c.TurnTimeout, "turn-timeout", 20*time.Minute, "wall-clock cap per wake")
	fs.DurationVar(&c.ToolTimeout, "tool-timeout", 60*time.Second, "cap on one tool call; past it the model gets an error result")
	fs.IntVar(&c.ToolResultMax, "tool-result-max-bytes", 32768, "cap on the bytes of one tool result the model sees; a longer one is cut and says so")
	fs.Int64Var(&c.DailyTokenCeil, "daily-token-ceiling", 2_000_000, "tokens per UTC day after which no wake starts; 0 disables the ceiling")
	fs.DurationVar(&c.PollInterval, "poll-interval", 30*time.Second, "how often the core is polled for edges between turns; with --core-api-url only while the trigger stream is down")
	fs.DurationVar(&c.EdgeCooldown, "edge-cooldown", time.Minute, "minimum gap between the end of a turn and an edge wake")
	fs.DurationVar(&c.ChatBatch, "chat-batch", 10*time.Minute, "how long a room message that does not name the spot waits before it wakes a turn; 0 wakes at once")
	fs.IntVar(&c.MaxEdgeWakesHr, "max-edge-wakes-per-hour", 12, "cap on early wakes (edges, messages) per hour; past it edges wait for the scheduled wake; 0 = no cap")
	fs.StringVar(&c.LogLevel, "log-level", "info", "debug, info, warn or error")
	fs.StringVar(&c.OtelEndpoint, "otel-endpoint", "", "OTLP/HTTP endpoint for the spot's traces (for example http://ego.<ns>.svc:4318); empty turns telemetry off")
	fs.StringVar(&c.OtelTokenFile, "otel-token-file", "", "file holding the sender's bearer token for --otel-endpoint")
	fs.StringVar(&c.CoreTools, "core-tools", "curated", "curated: the core tools the loop names, destructive ones refused by name; all: every core tool the core offers, bounded by the core Role")
	fs.StringVar(&c.EgoMCPURL, "ego-mcp-url", "", "ego's MCP endpoint (for example http://ego.<ns>.svc:8001/mcp), read with the --otel-token-file token; empty = no ego tools")
	fs.StringVar(&c.OtelTenant, "otel-tenant", "", "the tenant label the spot's traces carry in ego (letters, digits, dots, dashes, underscores); default the spot's name, so each spot is its own tenant")
	fs.StringVar(&c.OtelTenantAttr, "otel-tenant-attr", "triage.tenant", "the span attribute ego reads the tenant label from (ego's --usage-tenant-attr)")
	fs.StringVar(&c.OtelTenantName, "otel-tenant-name", "", "the display name ego shows for the tenant (ego.tenant.name, at most 80 characters); default the label")
	fs.BoolVar(&c.OtelContent, "otel-content", true, "put the turn's content on the trace spans: the prompts, the answers, the tool arguments and results, which the receiver needs to evaluate a turn; --otel-content=false leaves names, timings and token counts only")
	fs.BoolVar(&c.LogContent, "log-content", false, "let debug logs carry message, issue, tool input and model text; off, no log line carries any")
	fs.StringVar(&c.HeartbeatID, "heartbeat-id", "", "memory cell id of the heartbeat; default system2-heartbeat-<tenant>")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() > 0 {
		return c, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	c.applyDefaults()
	var cerr error
	if c.Connectors, cerr = LoadConnectors(c.ConnectorsFile); cerr != nil {
		return c, errors.Join(cerr, c.Validate())
	}
	return c, c.Validate()
}

// otelLabel is what ego accepts as a tenant label and what an attribute name
// may be made of.
var otelLabel = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.Model) == "" {
		switch c.LLMProvider {
		case ProviderAnthropic:
			c.Model = DefaultAnthropicModel
		case ProviderGemini:
			c.Model = DefaultGeminiModel
		}
	}
	if c.Name == "" && c.Tenant != "" {
		c.Name = c.Tenant + "-spot"
	}
	if c.HeartbeatID == "" && c.Tenant != "" {
		c.HeartbeatID = "system2-heartbeat-" + c.Tenant
	}
	if c.OtelTenant == "" {
		c.OtelTenant = c.Name
	}
	if c.OtelTenantName == "" {
		c.OtelTenantName = c.OtelTenant
	}
}

// Validate checks the values that have no safe default.
func (c *Config) Validate() error {
	var errs []error
	req := func(v, flagName string) {
		if strings.TrimSpace(v) == "" {
			errs = append(errs, fmt.Errorf("--%s is required", flagName))
		}
	}
	req(c.Tenant, "tenant")
	if c.OtelEndpoint != "" && !strings.HasPrefix(c.OtelEndpoint, "http://") && !strings.HasPrefix(c.OtelEndpoint, "https://") {
		errs = append(errs, errors.New("--otel-endpoint must be an http:// or https:// URL"))
	}
	if c.CoreTools != "curated" && c.CoreTools != "all" {
		errs = append(errs, errors.New("--core-tools is curated or all"))
	}
	if c.EgoMCPURL != "" && c.OtelTokenFile == "" {
		errs = append(errs, errors.New("--ego-mcp-url needs --otel-token-file, the sender token ego checks"))
	}
	if c.EgoMCPURL != "" && !strings.HasPrefix(c.EgoMCPURL, "http://") && !strings.HasPrefix(c.EgoMCPURL, "https://") {
		errs = append(errs, errors.New("--ego-mcp-url must be an http:// or https:// URL"))
	}
	if c.OtelEndpoint == "" && c.OtelTokenFile != "" {
		errs = append(errs, errors.New("--otel-token-file needs --otel-endpoint"))
	}
	if c.CoreAPIURL != "" && !strings.HasPrefix(c.CoreAPIURL, "http://") && !strings.HasPrefix(c.CoreAPIURL, "https://") {
		errs = append(errs, errors.New("--core-api-url must be an http:// or https:// URL"))
	}
	req(c.CoreMCPURL, "core-mcp-url")
	req(c.CoreTokenFile, "core-token-file")
	federation := map[string]string{
		"anthropic-identity-token-file": c.IdentityTokenFile, "anthropic-federation-rule-id": c.FederationRuleID,
		"anthropic-org-id": c.AnthropicOrgID, "anthropic-service-account-id": c.AnthropicServiceAcctID,
		"anthropic-workspace-id": c.AnthropicWorkspaceID,
	}
	only := func(mode string, flags map[string]string) {
		for flagName, v := range flags {
			if v != "" {
				errs = append(errs, fmt.Errorf("--%s only applies with --llm-auth=%s", flagName, mode))
			}
		}
	}
	switch c.LLMProvider {
	case ProviderAnthropic:
		if c.GeminiKeyFile != "" {
			errs = append(errs, errors.New("--gemini-key-file only applies with --llm-provider=gemini"))
		}
		switch c.LLMAuth {
		case AuthKey:
			req(c.AnthropicKeyFile, "anthropic-key-file")
			only(AuthFederation, federation)
			only(AuthVertex, map[string]string{"vertex-project": c.VertexProject})
		case AuthFederation:
			if c.AnthropicKeyFile != "" {
				errs = append(errs, errors.New("--anthropic-key-file must not be given with --llm-auth=federation: the key would shadow federation"))
			}
			only(AuthVertex, map[string]string{"vertex-project": c.VertexProject})
			req(c.IdentityTokenFile, "anthropic-identity-token-file")
			req(c.FederationRuleID, "anthropic-federation-rule-id")
			req(c.AnthropicOrgID, "anthropic-org-id")
			req(c.AnthropicServiceAcctID, "anthropic-service-account-id")
			for flagName, v := range map[string]string{
				"anthropic-federation-rule-id": c.FederationRuleID, "anthropic-org-id": c.AnthropicOrgID,
				"anthropic-service-account-id": c.AnthropicServiceAcctID, "anthropic-workspace-id": c.AnthropicWorkspaceID,
			} {
				if strings.ContainsAny(v, " \t\n/\\") {
					errs = append(errs, fmt.Errorf("--%s must not contain spaces or slashes", flagName))
				}
			}
		case AuthVertex:
			if c.AnthropicKeyFile != "" {
				errs = append(errs, errors.New("--anthropic-key-file only applies with --llm-auth=key"))
			}
			only(AuthFederation, federation)
			req(c.VertexProject, "vertex-project")
			req(c.VertexRegion, "vertex-region")
		default:
			errs = append(errs, fmt.Errorf("--llm-auth %q is not one of key, federation, vertex", c.LLMAuth))
		}
	case ProviderGemini:
		anthropic := map[string]string{"anthropic-key-file": c.AnthropicKeyFile}
		for k, v := range federation {
			anthropic[k] = v
		}
		for flagName, v := range anthropic {
			if v != "" {
				errs = append(errs, fmt.Errorf("--%s only applies with --llm-provider=anthropic", flagName))
			}
		}
		switch c.LLMAuth {
		case AuthKey:
			req(c.GeminiKeyFile, "gemini-key-file")
			only(AuthVertex, map[string]string{"vertex-project": c.VertexProject})
		case AuthVertex:
			if c.GeminiKeyFile != "" {
				errs = append(errs, errors.New("--gemini-key-file only applies with --llm-auth=key"))
			}
			req(c.VertexProject, "vertex-project")
			req(c.VertexRegion, "vertex-region")
		case AuthFederation:
			errs = append(errs, errors.New("--llm-auth=federation is an Anthropic auth mode: use key or vertex with --llm-provider=gemini"))
		default:
			errs = append(errs, fmt.Errorf("--llm-auth %q is not one of key, vertex", c.LLMAuth))
		}
	default:
		errs = append(errs, fmt.Errorf("--llm-provider %q is not one of anthropic, gemini", c.LLMProvider))
	}
	req(c.Model, "model")
	switch c.Effort {
	case "low", "medium", "high", "xhigh", "max":
	default:
		errs = append(errs, fmt.Errorf("--effort %q is not one of low, medium, high, xhigh, max", c.Effort))
	}
	if c.OtelEndpoint != "" {
		// ego keys a tenant by sender/label and uses the label in grants, which
		// split on spaces, commas and bars; a name it cannot take it drops silently.
		if !otelLabel.MatchString(c.OtelTenant) {
			errs = append(errs, fmt.Errorf("--otel-tenant %q must be letters, digits, dots, dashes or underscores", c.OtelTenant))
		}
		if !otelLabel.MatchString(c.OtelTenantAttr) {
			errs = append(errs, fmt.Errorf("--otel-tenant-attr %q is not an attribute name", c.OtelTenantAttr))
		}
		if n := c.OtelTenantName; len([]rune(n)) > 80 || strings.TrimSpace(n) != n || strings.ContainsFunc(n, unicode.IsControl) || strings.ContainsAny(n, ",=") {
			errs = append(errs, fmt.Errorf("--otel-tenant-name must be at most 80 characters, trimmed, with no control characters, commas or equals signs"))
		}
	}
	req(c.StateDir, "state-dir")
	if c.MinWake <= 0 {
		errs = append(errs, errors.New("--min-wake must be positive"))
	}
	if c.MaxWake < c.MinWake {
		errs = append(errs, errors.New("--max-wake must be at least --min-wake"))
	}
	if c.MaxSteps < 1 {
		errs = append(errs, errors.New("--max-steps must be at least 1"))
	}
	if c.ToolTimeout <= 0 {
		errs = append(errs, errors.New("--tool-timeout must be positive"))
	}
	if c.ToolResultMax < 1 {
		errs = append(errs, errors.New("--tool-result-max-bytes must be at least 1"))
	}
	if c.TurnTimeout <= 0 {
		errs = append(errs, errors.New("--turn-timeout must be positive"))
	}
	if c.PollInterval <= 0 {
		errs = append(errs, errors.New("--poll-interval must be positive"))
	}
	if c.ChatBatch < 0 || c.MaxEdgeWakesHr < 0 {
		errs = append(errs, errors.New("--chat-batch and --max-edge-wakes-per-hour cannot be negative"))
	}
	if c.DailyTokenCeil < 0 {
		errs = append(errs, errors.New("--daily-token-ceiling must not be negative"))
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("--log-level %q is not one of debug, info, warn, error", c.LogLevel))
	}
	if strings.ContainsAny(c.Tenant, " /\\\n\t") {
		errs = append(errs, errors.New("--tenant must not contain spaces or slashes"))
	}
	return errors.Join(errs...)
}
