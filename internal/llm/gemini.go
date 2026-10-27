package llm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/genai"
)

// GeminiOptions configure NewGemini.
type GeminiOptions struct {
	// Cache puts the system instruction and the tools in an explicit Gemini
	// cache (see Gemini). Off, the API's implicit caching is all there is.
	Cache bool

	// Auth is AuthKey (the Gemini API with a key from a file) or AuthVertex
	// (Gemini on Vertex AI through Application Default Credentials). Federation
	// is an Anthropic auth mode.
	Auth  string
	Model string

	// KeyFile holds the Gemini API key (AuthKey). It is read again on every
	// request, so a rotated Secret is picked up without a restart.
	KeyFile string

	// VertexProject and VertexLocation select the Vertex AI endpoint
	// (AuthVertex); an empty location is global.
	VertexProject  string
	VertexLocation string

	// BaseURL and HTTPClient replace the API host and transport, for tests.
	BaseURL    string
	HTTPClient *http.Client
}

// Gemini is a Client over the Google GenAI SDK.
//
// Caching: the system instruction and the tool list (sorted) are the stable
// prefix of every request. Gemini caches a prefix implicitly, but only when it
// hits, and a miss bills the whole prefix again at every step of a wake. So the
// prefix is also put in an explicit cache (CachedContent, one hour, remade when
// the prefix changes or is about to expire) and requests name it instead of
// carrying the prefix. If the cache cannot be made or used (a model that does
// not support it, a prefix under the minimum size) the request goes without it
// and the cache is not tried again for a while.
type Gemini struct {
	cache   geminiCache
	cacheOn bool
	// vertex is built once (it holds the ADC); in key mode it is nil and a
	// client is built per request, because the SDK fixes the key at build time.
	vertex  *genai.Client
	model   string
	keyFile string
	baseURL string
	httpc   *http.Client
	calls   atomic.Int64
}

// Default hosts, passed to the SDK explicitly so that a stray
// GOOGLE_GEMINI_BASE_URL or GOOGLE_VERTEX_BASE_URL in the environment cannot
// redirect a request that carries a credential.
const geminiAPIHost = "https://generativelanguage.googleapis.com/"

func vertexHost(location string) string {
	switch location {
	case "global":
		return "https://aiplatform.googleapis.com/"
	case "us", "eu":
		return "https://aiplatform." + location + ".rep.googleapis.com/"
	}
	return "https://" + location + "-aiplatform.googleapis.com/"
}

// geminiAttempts is the number of tries the SDK makes of one request (the first
// included) on 408, 429 and 5xx. The SDK retries nothing unless asked; this
// matches the Anthropic client's two retries. There is no retry layer here.
const geminiAttempts = 3

// NewGemini builds the client for the auth mode and fails at once on a mode or
// a file it cannot use. In key mode nothing is read from the environment: the
// key is passed explicitly and an empty key file is an error, so the SDK never
// falls back to GOOGLE_API_KEY or GEMINI_API_KEY.
func NewGemini(ctx context.Context, o GeminiOptions) (*Gemini, error) {
	if strings.TrimSpace(o.Model) == "" {
		return nil, errors.New("llm: no model")
	}
	g := &Gemini{model: o.Model, baseURL: o.BaseURL, httpc: o.HTTPClient, cacheOn: o.Cache}
	switch o.Auth {
	case AuthKey:
		if _, err := readKey(o.KeyFile); err != nil {
			return nil, err
		}
		g.keyFile = o.KeyFile
		if g.baseURL == "" {
			g.baseURL = geminiAPIHost
		}
	case AuthVertex:
		if o.VertexProject == "" {
			return nil, errors.New("llm: vertex needs a project")
		}
		loc := o.VertexLocation
		if loc == "" {
			loc = "global"
		}
		base := o.BaseURL
		if base == "" {
			base = vertexHost(loc)
		}
		c, err := genai.NewClient(ctx, &genai.ClientConfig{
			Backend: genai.BackendVertexAI, Project: o.VertexProject, Location: loc,
			HTTPClient:  o.HTTPClient,
			HTTPOptions: genai.HTTPOptions{BaseURL: base, RetryOptions: geminiRetry()},
		})
		if err != nil {
			return nil, fmt.Errorf("llm: gemini on vertex: %w", err)
		}
		g.vertex = c
	case AuthFederation:
		return nil, errors.New("llm: federation is an Anthropic auth mode")
	default:
		return nil, fmt.Errorf("llm: auth %q is not one of key, vertex", o.Auth)
	}
	return g, nil
}

func geminiRetry() *genai.HTTPRetryOptions {
	n := int32(geminiAttempts)
	return &genai.HTTPRetryOptions{Attempts: &n}
}

// Model is the model id requests are sent for.
func (g *Gemini) Model() string { return g.model }

// client returns the SDK client for one request.
func (g *Gemini) client(ctx context.Context) (*genai.Client, error) {
	if g.vertex != nil {
		return g.vertex, nil
	}
	key, err := readKey(g.keyFile)
	if err != nil {
		return nil, err
	}
	return genai.NewClient(ctx, &genai.ClientConfig{
		Backend: genai.BackendGeminiAPI, APIKey: key, HTTPClient: g.httpc,
		HTTPOptions: genai.HTTPOptions{BaseURL: g.baseURL, RetryOptions: geminiRetry()},
	})
}

// Complete implements Client.
func (g *Gemini) Complete(ctx context.Context, req Request) (Response, error) {
	contents, cfg, err := buildGeminiParams(g.model, req)
	if err != nil {
		return Response{}, err
	}
	c, err := g.client(ctx)
	if err != nil {
		return Response{}, err
	}
	full := *cfg
	cached := g.cacheOn && g.cache.use(ctx, c, g.model, cfg)
	resp, err := c.Models.GenerateContent(ctx, g.model, contents, cfg)
	if err != nil && cached {
		// The cache may have been dropped by the API: go on without it.
		g.cache.drop()
		resp, err = c.Models.GenerateContent(ctx, g.model, contents, &full)
	}
	if err != nil {
		return Response{}, wrapGeminiError(err)
	}
	return fromGeminiResponse(resp, g.model, func() string { return fmt.Sprintf("call_%d", g.calls.Add(1)) })
}

// generatedID matches the ids this client makes up for a function call the API
// gave none; they are not sent back, since the API never issued them.
func generatedID(id string) bool {
	rest, ok := strings.CutPrefix(id, "call_")
	if !ok || rest == "" {
		return false
	}
	for _, r := range rest {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// geminiThinking maps the effort to the thinking configuration. Gemini 3 takes
// a level, the earlier thinking models a token budget (-1 = dynamic). An empty
// or unknown effort leaves the model's default. Thoughts are not returned
// (IncludeThoughts stays false); a thought signature still is, on function
// calls.
func geminiThinking(model, effort string) *genai.ThinkingConfig {
	if strings.HasPrefix(model, "gemini-3") {
		var l genai.ThinkingLevel
		switch effort {
		case "low":
			l = genai.ThinkingLevelLow
		case "medium":
			l = genai.ThinkingLevelMedium
		case "high", "xhigh", "max":
			l = genai.ThinkingLevelHigh
		default:
			return nil
		}
		return &genai.ThinkingConfig{ThinkingLevel: l}
	}
	var b int32
	switch effort {
	case "low":
		b = 1024
	case "medium":
		b = 4096
	case "high":
		b = 8192
	case "xhigh", "max":
		b = -1
	default:
		return nil
	}
	return &genai.ThinkingConfig{ThinkingBudget: &b}
}

// buildGeminiParams turns a Request into the SDK's contents and configuration.
// The tools are sorted by name and carry their JSON schema unchanged; the tool
// choice is left at auto.
func buildGeminiParams(model string, req Request) ([]*genai.Content, *genai.GenerateContentConfig, error) {
	cfg := &genai.GenerateContentConfig{
		MaxOutputTokens: int32(req.MaxTokens),
		ThinkingConfig:  geminiThinking(model, req.Effort),
	}
	if req.MaxTokens <= 0 {
		cfg.MaxOutputTokens = DefaultMaxTokens
	}
	if len(req.System) > 0 {
		sys := &genai.Content{}
		for _, s := range req.System {
			sys.Parts = append(sys.Parts, &genai.Part{Text: s})
		}
		cfg.SystemInstruction = sys
	}
	if len(req.Tools) > 0 {
		tools := append([]ToolDef(nil), req.Tools...)
		sort.SliceStable(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
		tool := &genai.Tool{}
		for _, t := range tools {
			fd := &genai.FunctionDeclaration{Name: t.Name, Description: t.Description}
			if len(t.InputSchema) > 0 && json.Valid(t.InputSchema) {
				fd.ParametersJsonSchema = t.InputSchema
			}
			tool.FunctionDeclarations = append(tool.FunctionDeclarations, fd)
		}
		cfg.Tools = []*genai.Tool{tool}
	}

	names := map[string]string{} // tool use id -> tool name, from the assistant messages so far
	var contents []*genai.Content
	for _, m := range req.Messages {
		role := string(genai.RoleUser)
		if m.Role == RoleAssistant {
			role = string(genai.RoleModel)
		}
		c := &genai.Content{Role: role}
		for _, b := range m.Content {
			switch b.Kind {
			case KindText:
				if b.Text == "" && b.Signature == "" {
					continue
				}
				c.Parts = append(c.Parts, &genai.Part{Text: b.Text, ThoughtSignature: decodeSignature(b.Signature)})
			case KindThinking:
				if b.Thinking == "" && b.Signature == "" {
					continue
				}
				c.Parts = append(c.Parts, &genai.Part{Text: b.Thinking, Thought: true, ThoughtSignature: decodeSignature(b.Signature)})
			case KindToolUse:
				names[b.ID] = b.Name
				fc := &genai.FunctionCall{Name: b.Name, Args: decodeArgs(b.Input)}
				if !generatedID(b.ID) {
					fc.ID = b.ID
				}
				c.Parts = append(c.Parts, &genai.Part{FunctionCall: fc, ThoughtSignature: decodeSignature(b.Signature)})
			case KindToolResult:
				name, ok := names[b.ToolUseID]
				if !ok {
					return nil, nil, fmt.Errorf("llm: tool result for tool use %q, which no earlier assistant message made", b.ToolUseID)
				}
				key := "result"
				if b.IsError {
					key = "error"
				}
				fr := &genai.FunctionResponse{Name: name, Response: map[string]any{key: b.Content}}
				if !generatedID(b.ToolUseID) {
					fr.ID = b.ToolUseID
				}
				c.Parts = append(c.Parts, &genai.Part{FunctionResponse: fr})
			}
		}
		if len(c.Parts) > 0 {
			contents = append(contents, c)
		}
	}
	return contents, cfg, nil
}

func decodeArgs(in json.RawMessage) map[string]any {
	args := map[string]any{}
	if len(in) == 0 {
		return args
	}
	dec := json.NewDecoder(bytes.NewReader(in))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil || m == nil {
		return args
	}
	return m
}

func encodeSignature(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

func decodeSignature(s string) []byte {
	if s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// fromGeminiResponse turns the SDK's answer into a Response. nextID makes the id
// of a function call the API gave none.
func fromGeminiResponse(r *genai.GenerateContentResponse, model string, nextID func() string) (Response, error) {
	out := Response{Model: model}
	if r.ModelVersion != "" {
		out.Model = r.ModelVersion
	}
	if u := r.UsageMetadata; u != nil {
		out.Usage = Usage{
			Input:     int64(u.PromptTokenCount) - int64(u.CachedContentTokenCount),
			CacheRead: int64(u.CachedContentTokenCount),
			Output:    int64(u.CandidatesTokenCount) + int64(u.ThoughtsTokenCount),
		}
	}
	if len(r.Candidates) == 0 || r.Candidates[0] == nil {
		if pf := r.PromptFeedback; pf != nil && pf.BlockReason != "" && pf.BlockReason != genai.BlockedReasonUnspecified {
			out.StopReason, out.RefusalCategory = StopRefusal, string(pf.BlockReason)
			return out, nil
		}
		return Response{}, errors.New("llm: the model returned no candidate")
	}
	cand := r.Candidates[0]
	calls := false
	if cand.Content != nil {
		for _, p := range cand.Content.Parts {
			if p == nil {
				continue
			}
			sig := encodeSignature(p.ThoughtSignature)
			switch {
			case p.FunctionCall != nil:
				calls = true
				id := p.FunctionCall.ID
				if id == "" {
					id = nextID()
				}
				in, err := json.Marshal(p.FunctionCall.Args)
				if err != nil || len(p.FunctionCall.Args) == 0 {
					in = []byte(`{}`)
				}
				out.Content = append(out.Content, Block{Kind: KindToolUse, ID: id, Name: p.FunctionCall.Name, Input: in, Signature: sig})
			case p.Thought:
				out.Content = append(out.Content, Block{Kind: KindThinking, Thinking: p.Text, Signature: sig})
			case p.Text != "" || sig != "":
				b := Text(p.Text)
				b.Signature = sig
				out.Content = append(out.Content, b)
			}
		}
	}
	switch {
	case calls:
		out.StopReason = StopToolUse
	case cand.FinishReason == genai.FinishReasonStop:
		out.StopReason = StopEndTurn
	case cand.FinishReason == genai.FinishReasonMaxTokens:
		out.StopReason = StopMaxTokens
	case refusalFinish(cand.FinishReason):
		out.StopReason, out.RefusalCategory = StopRefusal, string(cand.FinishReason)
	default:
		out.StopReason = StopOther
	}
	return out, nil
}

func refusalFinish(f genai.FinishReason) bool {
	switch f {
	case genai.FinishReasonSafety, genai.FinishReasonRecitation, genai.FinishReasonBlocklist,
		genai.FinishReasonProhibitedContent, genai.FinishReasonSPII:
		return true
	}
	return false
}

// wrapGeminiError turns the SDK's error into an APIError the loop can classify;
// any other error (a transport failure, a cancelled context) is returned as it
// is.
func wrapGeminiError(err error) error {
	var ae genai.APIError
	var pe *genai.APIError
	switch {
	case errors.As(err, &ae):
	case errors.As(err, &pe) && pe != nil:
		ae = *pe
	default:
		return err
	}
	msg := ae.Message
	if i := strings.Index(msg, "\n"); i > 0 {
		msg = msg[:i]
	}
	if len(msg) > 500 {
		msg = msg[:500]
	}
	return &APIError{Status: ae.Code, Type: ae.Status, Message: msg}
}

// geminiCacheTTL is how long an explicit cache lives; it is remade a few
// minutes before it ends.
const (
	geminiCacheTTL     = time.Hour
	geminiCacheRenew   = 5 * time.Minute
	geminiCacheBackoff = 15 * time.Minute
)

// geminiCache is the explicit cache of the request prefix.
type geminiCache struct {
	mu      sync.Mutex
	name    string
	key     string
	expires time.Time
	off     time.Time // do not try to make one before this
}

// use puts the prefix in the cache and takes it out of cfg, and reports
// whether it did. On any failure cfg is left as it is.
func (gc *geminiCache) use(ctx context.Context, c *genai.Client, model string, cfg *genai.GenerateContentConfig) bool {
	if cfg.SystemInstruction == nil && len(cfg.Tools) == 0 {
		return false
	}
	key := cachePrefixKey(model, cfg)
	gc.mu.Lock()
	defer gc.mu.Unlock()
	now := time.Now()
	if gc.name == "" || gc.key != key || now.After(gc.expires.Add(-geminiCacheRenew)) {
		if now.Before(gc.off) {
			return false
		}
		made, err := c.Caches.Create(ctx, model, &genai.CreateCachedContentConfig{
			TTL: geminiCacheTTL, DisplayName: "tria-spot-prefix",
			SystemInstruction: cfg.SystemInstruction, Tools: cfg.Tools,
		})
		if err != nil || made == nil || made.Name == "" {
			gc.name, gc.off = "", now.Add(geminiCacheBackoff)
			return false
		}
		gc.name, gc.key, gc.expires = made.Name, key, now.Add(geminiCacheTTL)
	}
	cfg.CachedContent = gc.name
	cfg.SystemInstruction, cfg.Tools = nil, nil
	return true
}

// drop forgets the cache after the API refused it.
func (gc *geminiCache) drop() {
	gc.mu.Lock()
	gc.name, gc.off = "", time.Now().Add(geminiCacheBackoff)
	gc.mu.Unlock()
}

// cachePrefixKey identifies a prefix: the model, the system instruction and
// the tools as they will be sent.
func cachePrefixKey(model string, cfg *genai.GenerateContentConfig) string {
	h := sha256.New()
	h.Write([]byte(model))
	b, _ := json.Marshal(struct {
		S *genai.Content
		T []*genai.Tool
	}{cfg.SystemInstruction, cfg.Tools})
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}
