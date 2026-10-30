package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/param"
	"github.com/anthropics/anthropic-sdk-go/vertex"
)

// Auth modes.
const (
	// AuthKey is a static API key read from a file.
	AuthKey = "key"
	// AuthFederation is Anthropic Workload Identity Federation: a projected
	// ServiceAccount token (a file) is exchanged for a short-lived access token.
	AuthFederation = "federation"
	// AuthVertex is Claude on Google Vertex AI through Application Default
	// Credentials, the pod's Workload Identity.
	AuthVertex = "vertex"
	// AuthInjected sends a placeholder API key and holds no credential: something
	// between the spot and the API (the egress gateway of a substrate actor, the
	// core's ADR-107) replaces the header with the real key. The header must be
	// there for it to be replaced.
	AuthInjected = "injected"
)

// DefaultMaxTokens is the output cap of one request when the request names none.
const DefaultMaxTokens = 16000

// Federation holds the ids of Workload Identity Federation. None is a secret;
// the identity token is a file.
type Federation struct {
	RuleID            string
	OrganizationID    string
	ServiceAccountID  string
	WorkspaceID       string // optional when the rule spans one workspace
	IdentityTokenFile string
}

// Options configure NewAnthropic.
// anthropicAPIURL is the API host used when none is given, so no
// environment variable can choose it.
const anthropicAPIURL = "https://api.anthropic.com"

// injectedKey is what the spot sends as its API key with AuthInjected. It is not a
// credential: the egress that injects the real one replaces the header only when
// the request carries it.
const injectedKey = "replaced-by-egress"

type Options struct {
	Auth  string
	Model string

	// KeyFile holds the API key (AuthKey). It is read again on every request, so
	// a rotated Secret is picked up without a restart.
	KeyFile string

	Federation Federation

	VertexProject string
	VertexRegion  string

	// BaseURL and HTTPClient replace the API host and transport, for tests.
	BaseURL    string
	HTTPClient option.HTTPClient
}

// Anthropic is a Client over the official SDK.
type Anthropic struct {
	client  anthropic.Client
	model   string
	keyFile string
}

// federationVars are the process environment variables the SDK reads to find
// Workload Identity Federation. None of them is a secret: the ids are not, and
// the token is a file path.
var federationVars = []string{
	"ANTHROPIC_FEDERATION_RULE_ID", "ANTHROPIC_ORGANIZATION_ID", "ANTHROPIC_SERVICE_ACCOUNT_ID",
	"ANTHROPIC_IDENTITY_TOKEN_FILE", "ANTHROPIC_WORKSPACE_ID",
}

// shadowVars are the variables that outrank federation in the SDK's credential
// order and would silently replace it: a static key, a bearer token, a literal
// identity token and a named profile.
var shadowVars = []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_IDENTITY_TOKEN", "ANTHROPIC_PROFILE"}

// NewAnthropic builds the client for the auth mode and fails at once on a mode
// or a file it cannot use.
func NewAnthropic(ctx context.Context, o Options) (*Anthropic, error) {
	if strings.TrimSpace(o.Model) == "" {
		return nil, errors.New("llm: no model")
	}
	a := &Anthropic{model: o.Model}
	var opts []option.RequestOption
	switch o.Auth {
	case AuthKey:
		key, err := readKey(o.KeyFile)
		if err != nil {
			return nil, err
		}
		a.keyFile = o.KeyFile
		// No environment: a stray ANTHROPIC_* variable must not pick the
		// credential or the host.
		opts = append(opts, option.WithoutEnvironmentDefaults(), option.WithAPIKey(key))
	case AuthFederation:
		if err := setFederationEnv(o.Federation); err != nil {
			return nil, err
		}
		// Built with no key: the SDK finds federation in the variables above.
		// The host is pinned too, so a stray ANTHROPIC_BASE_URL cannot send
		// the federation exchange and the requests elsewhere.
		if o.BaseURL == "" {
			opts = append(opts, option.WithBaseURL(anthropicAPIURL))
		}
	case AuthVertex:
		if o.VertexProject == "" {
			return nil, errors.New("llm: vertex needs a project")
		}
		region := o.VertexRegion
		if region == "" {
			region = "global"
		}
		opts = append(opts, vertex.WithGoogleAuth(ctx, region, o.VertexProject))
	case AuthInjected:
		// A placeholder, never a real key, and nothing from the environment: a
		// stray ANTHROPIC_API_KEY or ANTHROPIC_BASE_URL must neither be sent nor
		// choose the host. The host is pinned unless a base URL is given.
		opts = append(opts, option.WithoutEnvironmentDefaults(), option.WithAPIKey(injectedKey))
		if o.BaseURL == "" {
			opts = append(opts, option.WithBaseURL(anthropicAPIURL))
		}
	default:
		return nil, fmt.Errorf("llm: auth %q is not one of key, federation, vertex, injected", o.Auth)
	}
	if o.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(o.BaseURL))
	}
	if o.HTTPClient != nil {
		opts = append(opts, option.WithHTTPClient(o.HTTPClient))
	}
	a.client = anthropic.NewClient(opts...)
	return a, nil
}

// setFederationEnv makes the process find Workload Identity Federation. The
// repository keeps credentials out of the environment: what is set here is the
// ids and the path of the token file, which are not credentials, and only for
// this process; it starts no child. The variables that outrank federation in
// the SDK (ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN, ANTHROPIC_IDENTITY_TOKEN,
// ANTHROPIC_PROFILE) are removed, because any of them would silently shadow it.
func setFederationEnv(f Federation) error {
	for name, v := range map[string]string{
		"rule id": f.RuleID, "organization id": f.OrganizationID,
		"service account id": f.ServiceAccountID, "identity token file": f.IdentityTokenFile,
	} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("llm: federation needs the %s", name)
		}
	}
	for _, name := range shadowVars {
		if err := os.Unsetenv(name); err != nil {
			return err
		}
	}
	set := map[string]string{
		"ANTHROPIC_FEDERATION_RULE_ID":  f.RuleID,
		"ANTHROPIC_ORGANIZATION_ID":     f.OrganizationID,
		"ANTHROPIC_SERVICE_ACCOUNT_ID":  f.ServiceAccountID,
		"ANTHROPIC_IDENTITY_TOKEN_FILE": f.IdentityTokenFile,
		"ANTHROPIC_WORKSPACE_ID":        f.WorkspaceID,
	}
	for _, name := range federationVars {
		var err error
		if v := set[name]; v != "" {
			err = os.Setenv(name, v)
		} else {
			err = os.Unsetenv(name)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func readKey(path string) (string, error) {
	if path == "" {
		return "", errors.New("llm: no API key file")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("llm: API key file: %w", err)
	}
	key := strings.TrimSpace(string(b))
	if key == "" {
		return "", errors.New("llm: API key file is empty")
	}
	return key, nil
}

// Model is the model id requests are sent for.
func (a *Anthropic) Model() string { return a.model }

// Complete implements Client. The SDK retries a 429 and a 5xx twice by itself
// and applies its own timeout; the caller's context bounds the whole turn.
func (a *Anthropic) Complete(ctx context.Context, req Request) (Response, error) {
	var opts []option.RequestOption
	if a.keyFile != "" {
		key, err := readKey(a.keyFile)
		if err != nil {
			return Response{}, err
		}
		opts = append(opts, option.WithAPIKey(key))
	}
	msg, err := a.client.Messages.New(ctx, buildParams(a.model, req), opts...)
	if err != nil {
		return Response{}, wrapError(err)
	}
	return fromMessage(msg), nil
}

// buildParams turns a Request into the SDK's parameters.
//
// The cache works on a prefix, rendered as tools, then system, then messages.
// The tools are sorted by name and the system prompt carries a one-hour
// breakpoint on its last block, so the same prefix is read again by wakes up to
// an hour apart. The last block of the last message gets a short breakpoint, so
// the steps of one turn read the conversation so far instead of paying for it
// again. Thinking is not sent: the model thinks adaptively on its own, and
// "disabled" or a budget is rejected. The tool choice is left at auto.
func buildParams(model string, req Request) anthropic.MessageNewParams {
	p := anthropic.MessageNewParams{Model: anthropic.Model(model), MaxTokens: int64(req.MaxTokens)}
	if req.MaxTokens <= 0 {
		p.MaxTokens = DefaultMaxTokens
	}
	if req.Effort != "" {
		p.OutputConfig = anthropic.OutputConfigParam{Effort: anthropic.OutputConfigEffort(req.Effort)}
	}
	for i, s := range req.System {
		b := anthropic.TextBlockParam{Text: s}
		if i == len(req.System)-1 {
			b.CacheControl = anthropic.CacheControlEphemeralParam{TTL: anthropic.CacheControlEphemeralTTLTTL1h}
		}
		p.System = append(p.System, b)
	}
	tools := append([]ToolDef(nil), req.Tools...)
	sort.SliceStable(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
	for _, t := range tools {
		tp := anthropic.ToolParam{Name: t.Name}
		if t.Description != "" {
			tp.Description = anthropic.String(t.Description)
		}
		schema := json.RawMessage(`{"type":"object"}`)
		if json.Valid(t.InputSchema) {
			schema = t.InputSchema
		}
		tp.InputSchema = param.Override[anthropic.ToolInputSchemaParam](schema)
		p.Tools = append(p.Tools, anthropic.ToolUnionParam{OfTool: &tp})
	}
	for _, m := range req.Messages {
		p.Messages = append(p.Messages, toMessageParam(m))
	}
	if n := len(p.Messages); n > 0 {
		if blocks := p.Messages[n-1].Content; len(blocks) > 0 {
			if cc := blocks[len(blocks)-1].GetCacheControl(); cc != nil {
				*cc = anthropic.NewCacheControlEphemeralParam()
			}
		}
	}
	return p
}

func toMessageParam(m Message) anthropic.MessageParam {
	blocks := make([]anthropic.ContentBlockParamUnion, 0, len(m.Content))
	for _, b := range m.Content {
		switch b.Kind {
		case KindText:
			blocks = append(blocks, anthropic.NewTextBlock(b.Text))
		case KindToolUse:
			var in any = json.RawMessage(`{}`)
			if len(b.Input) > 0 && json.Valid(b.Input) {
				in = b.Input
			}
			blocks = append(blocks, anthropic.NewToolUseBlock(b.ID, in, b.Name))
		case KindToolResult:
			blocks = append(blocks, anthropic.NewToolResultBlock(b.ToolUseID, b.Content, b.IsError))
		case KindThinking:
			blocks = append(blocks, anthropic.NewThinkingBlock(b.Signature, b.Thinking))
		case KindRedactedThinking:
			blocks = append(blocks, anthropic.NewRedactedThinkingBlock(b.Data))
		}
	}
	if m.Role == RoleAssistant {
		return anthropic.NewAssistantMessage(blocks...)
	}
	return anthropic.NewUserMessage(blocks...)
}

// fromMessage turns the SDK's answer into a Response. Blocks the loop has no use
// for (server tools are not requested) are dropped.
func fromMessage(m *anthropic.Message) Response {
	r := Response{
		Model:      string(m.Model),
		StopReason: stopReason(m.StopReason),
		Usage: Usage{
			Input: m.Usage.InputTokens, Output: m.Usage.OutputTokens,
			CacheRead: m.Usage.CacheReadInputTokens, CacheWrite: m.Usage.CacheCreationInputTokens,
		},
	}
	if r.StopReason == StopRefusal {
		r.RefusalCategory = string(m.StopDetails.Category)
	}
	for _, c := range m.Content {
		switch v := c.AsAny().(type) {
		case anthropic.TextBlock:
			r.Content = append(r.Content, Text(v.Text))
		case anthropic.ToolUseBlock:
			in := json.RawMessage(v.JSON.Input.Raw())
			if len(in) == 0 {
				in = json.RawMessage(`{}`)
			}
			r.Content = append(r.Content, Block{Kind: KindToolUse, ID: v.ID, Name: v.Name, Input: in})
		case anthropic.ThinkingBlock:
			r.Content = append(r.Content, Block{Kind: KindThinking, Thinking: v.Thinking, Signature: v.Signature})
		case anthropic.RedactedThinkingBlock:
			r.Content = append(r.Content, Block{Kind: KindRedactedThinking, Data: v.Data})
		}
	}
	return r
}

func stopReason(s anthropic.StopReason) StopReason {
	switch s {
	case anthropic.StopReasonEndTurn:
		return StopEndTurn
	case anthropic.StopReasonToolUse:
		return StopToolUse
	case anthropic.StopReasonMaxTokens:
		return StopMaxTokens
	case anthropic.StopReasonRefusal:
		return StopRefusal
	case anthropic.StopReasonPauseTurn:
		return StopPauseTurn
	}
	return StopOther
}

// wrapError turns the SDK's error into an APIError the loop can classify; any
// other error (a transport failure, a cancelled context) is returned as it is.
func wrapError(err error) error {
	var ae *anthropic.Error
	if !errors.As(err, &ae) {
		return err
	}
	msg := ae.Error()
	if i := strings.Index(msg, "\n"); i > 0 {
		msg = msg[:i]
	}
	return &APIError{Status: ae.StatusCode, Type: string(ae.Type()), Message: msg, RequestID: ae.RequestID}
}
