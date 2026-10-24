// Package telemetry sends the spot's own OpenTelemetry spans to an OTLP/HTTP
// endpoint (ego). One root span covers a turn, with one span per model request
// and one per tool call, named and attributed after the OpenTelemetry GenAI
// conventions. Spans carry names, timings and token counts and, with Content
// (the default), the turn's messages, tool arguments and tool results: that is
// what the receiver builds a conversation turn from, and without it nothing
// can be evaluated.
//
// Every method is safe on a nil receiver, so a turn runs the same with
// telemetry off.
package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/triageagent-dev/tria-spot/internal/llm"
)

// Options configure a Provider.
type Options struct {
	// Endpoint is the OTLP/HTTP base URL (for example http://ego.<ns>.svc:4318);
	// empty = telemetry off.
	Endpoint string
	// TokenFile holds the sender's bearer token, read on every export.
	TokenFile string
	// Name is the spot's name: service.name, gen_ai.agent.name and ego.thread,
	// so every wake is a segment of one thread.
	Name string
	// Tenant is the label ego files the spans under, found by ego under
	// TenantAttr; TenantName is the text ego shows for it.
	Tenant, TenantAttr, TenantName string
	// Content puts the turn's content on the spans: the messages of each model
	// request and its answer, the system prompt, and the arguments and result
	// of each tool call. The receiver needs it to evaluate a turn. Off, the
	// spans carry names, timings and token counts only.
	Content bool
	// Provider is the model provider, the value of gen_ai.provider.name on the
	// chat spans: "anthropic" (also the default when empty) or "gemini".
	Provider string

	// exporter replaces the OTLP exporter, for tests.
	exporter sdktrace.SpanExporter
}

// Provider owns the tracer provider of the process.
type Provider struct {
	tp     *sdktrace.TracerProvider
	tracer trace.Tracer
	o      Options
}

// New returns a Provider that exports in batches, or nil when telemetry is off
// (an empty Endpoint). Flush ends each turn's batch and Shutdown ends the
// process's.
func New(ctx context.Context, o Options) (*Provider, error) {
	exp := o.exporter
	if exp == nil {
		if o.Endpoint == "" {
			return nil, nil
		}
		full, err := tracesURL(o.Endpoint)
		if err != nil {
			return nil, err
		}
		client := &http.Client{Timeout: 15 * time.Second, Transport: &bearer{file: o.TokenFile, next: http.DefaultTransport}}
		exp, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(full), otlptracehttp.WithHTTPClient(client))
		if err != nil {
			return nil, fmt.Errorf("telemetry: %w", err)
		}
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(resource.NewSchemaless(resourceAttributes(o)...)),
	)
	return &Provider{tp: tp, tracer: tp.Tracer("tria-spot"), o: o}, nil
}

// resourceAttributes are the ones the receiver reads: the spot as a service,
// an agent and one thread, and its tenant label and display name.
func resourceAttributes(o Options) []attribute.KeyValue {
	attr := o.TenantAttr
	if attr == "" {
		attr = "ego.tenant"
	}
	tenant := o.Tenant
	if tenant == "" {
		tenant = o.Name
	}
	name := o.TenantName
	if name == "" {
		name = tenant
	}
	return []attribute.KeyValue{
		attribute.String("service.name", o.Name),
		attribute.String("gen_ai.agent.name", o.Name),
		attribute.String("ego.thread", o.Name),
		attribute.String(attr, tenant),
		attribute.String("ego.tenant.name", name),
	}
}

func tracesURL(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("telemetry: %q is not an http:// or https:// URL", endpoint)
	}
	if !strings.HasSuffix(u.Path, "/v1/traces") {
		u.Path = strings.TrimRight(u.Path, "/") + "/v1/traces"
	}
	return u.String(), nil
}

// bearer adds the sender's token, read from its file at every request.
type bearer struct {
	file string
	next http.RoundTripper
}

func (b *bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.file != "" {
		if raw, err := os.ReadFile(b.file); err == nil {
			if tok := strings.TrimSpace(string(raw)); tok != "" {
				r = r.Clone(r.Context())
				r.Header.Set("Authorization", "Bearer "+tok)
			}
		}
	}
	return b.next.RoundTrip(r)
}

// Flush exports what the batch holds, for at most the context's time.
func (p *Provider) Flush(ctx context.Context) error {
	if p == nil {
		return nil
	}
	return p.tp.ForceFlush(ctx)
}

// Shutdown flushes and stops the exporter.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	return p.tp.Shutdown(ctx)
}

// Turn is the root span of one wake.
type Turn struct {
	p    *Provider
	ctx  context.Context
	span trace.Span
	id   string
	// chats counts the model requests started, so the first one can carry the
	// system prompt.
	chats int
}

// StartTurn opens the root span "invoke_agent <name>" with a fresh conversation
// id, the wake reason and, with Content, the wake prompt as the first user
// message (the receiver's fallback when a chat span has none).
func (p *Provider) StartTurn(ctx context.Context, reason, prompt string) (context.Context, *Turn) {
	if p == nil {
		return ctx, nil
	}
	id := uuid.NewString()
	ctx, span := p.tracer.Start(ctx, "invoke_agent "+p.o.Name, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "invoke_agent"),
		attribute.String("gen_ai.conversation.id", id),
		attribute.String("gen_ai.agent.name", p.o.Name),
		attribute.String("tria.wake.reason", reason),
	))
	if p.o.Content {
		span.SetAttributes(attribute.String("gen_ai.input.messages", messagesJSON("user", prompt, "")))
	}
	return ctx, &Turn{p: p, ctx: ctx, span: span, id: id}
}

// ID is the turn's conversation id; empty with telemetry off.
func (t *Turn) ID() string {
	if t == nil {
		return ""
	}
	return t.id
}

// End closes the root span. text is the final answer (kept only with
// Content); err marks the turn failed.
func (t *Turn) End(text string, err error) {
	if t == nil {
		return
	}
	if t.p.o.Content && text != "" {
		t.span.SetAttributes(attribute.String("gen_ai.output.messages", messagesJSON("assistant", capString(text, maxAttr-512), "stop")))
	}
	if err != nil {
		t.span.SetStatus(codes.Error, err.Error())
	}
	t.span.End()
}

// messagesJSON is a one-message gen_ai message array.
func messagesJSON(role, text, finish string) string {
	type part struct {
		Type    string `json:"type"`
		Content string `json:"content"`
	}
	m := map[string]any{"role": role, "parts": []part{{Type: "text", Content: text}}}
	if finish != "" {
		m["finish_reason"] = finish
	}
	b, _ := json.Marshal([]any{m})
	return string(b)
}

// maxAttr is the longest attribute value the spans carry. A longer one is cut.
const maxAttr = 64 << 10

func (p *Provider) provider() string {
	if p.o.Provider == "" {
		return "anthropic"
	}
	return p.o.Provider
}

// Chat is the span of one model request.
type Chat struct {
	t     *Turn
	span  trace.Span
	model string
}

// Chat starts the span of a model request under the root. With Content it
// carries the request's messages as gen_ai.input.messages and, on the first
// request of the turn, the system prompt as gen_ai.system_instructions.
func (t *Turn) Chat(model string, req llm.Request) *Chat {
	if t == nil {
		return nil
	}
	_, span := t.p.tracer.Start(t.ctx, "chat "+model, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "chat"),
		attribute.String("gen_ai.provider.name", t.p.provider()),
		attribute.String("gen_ai.request.model", model),
	))
	if t.p.o.Content {
		span.SetAttributes(attribute.String("gen_ai.input.messages", inputMessagesJSON(req.Messages)))
		if t.chats == 0 && len(req.System) > 0 {
			span.SetAttributes(attribute.String("gen_ai.system_instructions", capString(strings.Join(req.System, "\n\n"), maxAttr)))
		}
	}
	t.chats++
	return &Chat{t: t, span: span, model: model}
}

// End closes the span with what the model answered. err, if not nil, marks it
// failed and the other values are ignored.
func (c *Chat) End(r llm.Response, err error) {
	if c == nil {
		return
	}
	if err != nil {
		c.span.SetStatus(codes.Error, err.Error())
		c.span.End()
		return
	}
	model := r.Model
	if model == "" {
		model = c.model
	}
	c.span.SetAttributes(
		attribute.String("gen_ai.response.model", model),
		// The receiver reads input_tokens as the whole prompt of the call and
		// adds input and output for a session's total, and reads the two cache
		// attributes separately as the split for pricing. The API's own
		// input_tokens leaves the cached parts out, so they are added here.
		attribute.Int64("gen_ai.usage.input_tokens", r.Usage.Input+r.Usage.CacheRead+r.Usage.CacheWrite),
		attribute.Int64("gen_ai.usage.output_tokens", r.Usage.Output),
		attribute.Int64("gen_ai.usage.cache_read.input_tokens", r.Usage.CacheRead),
		attribute.Int64("gen_ai.usage.cache_creation.input_tokens", r.Usage.CacheWrite),
		attribute.StringSlice("gen_ai.response.finish_reasons", []string{string(r.StopReason)}),
	)
	if c.t.p.o.Content {
		if m, ok := genAIMessage(llm.Message{Role: llm.RoleAssistant, Content: r.Content}); ok {
			m["finish_reason"] = string(r.StopReason)
			c.span.SetAttributes(attribute.String("gen_ai.output.messages", capMessages([]map[string]any{m})))
		}
	}
	c.span.End()
}

// genAIMessage is a message in the GenAI convention's shape: a role and parts
// (text, tool_call, tool_call_response). Thinking blocks are left out. ok is
// false when nothing is left.
func genAIMessage(m llm.Message) (map[string]any, bool) {
	var parts []map[string]any
	for _, b := range m.Content {
		switch b.Kind {
		case llm.KindText:
			parts = append(parts, map[string]any{"type": "text", "content": b.Text})
		case llm.KindToolUse:
			args := json.RawMessage(`{}`)
			if len(b.Input) > 0 && json.Valid(b.Input) {
				args = b.Input
			}
			parts = append(parts, map[string]any{"type": "tool_call", "id": b.ID, "name": b.Name, "arguments": args})
		case llm.KindToolResult:
			parts = append(parts, map[string]any{"type": "tool_call_response", "id": b.ToolUseID, "response": b.Content})
		}
	}
	if len(parts) == 0 {
		return nil, false
	}
	return map[string]any{"role": string(m.Role), "parts": parts}, true
}

func inputMessagesJSON(msgs []llm.Message) string {
	var out []map[string]any
	for _, m := range msgs {
		if g, ok := genAIMessage(m); ok {
			out = append(out, g)
		}
	}
	return capMessages(out)
}

// capMessages marshals the messages and, if the array is longer than maxAttr,
// drops the oldest messages first and puts one user message at the front that
// says how many were left out, so the last message always survives. If the last
// message alone is too long, its strings are cut in the middle.
func capMessages(msgs []map[string]any) string {
	enc := func(ms []map[string]any, omitted int) string {
		all := ms
		if omitted > 0 {
			note := map[string]any{"role": "user", "parts": []map[string]any{
				{"type": "text", "content": fmt.Sprintf("[%d earlier messages omitted]", omitted)}}}
			all = append([]map[string]any{note}, ms...)
		}
		b, _ := json.Marshal(all)
		return string(b)
	}
	omitted := 0
	for {
		out := enc(msgs[omitted:], omitted)
		if len(out) <= maxAttr || len(msgs)-omitted <= 1 {
			if len(out) <= maxAttr {
				return out
			}
			break
		}
		// Sizes are rechecked after each drop; messages are few.
		omitted++
	}
	last := shrinkMessage(msgs[len(msgs)-1], maxAttr/4)
	out := enc([]map[string]any{last}, len(msgs)-1)
	for budget := maxAttr / 8; len(out) > maxAttr && budget > 64; budget /= 2 {
		last = shrinkMessage(last, budget)
		out = enc([]map[string]any{last}, len(msgs)-1)
	}
	return out
}

// shrinkMessage cuts the long strings and argument objects of a message to
// about limit bytes each.
func shrinkMessage(m map[string]any, limit int) map[string]any {
	parts, _ := m["parts"].([]map[string]any)
	np := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		q := map[string]any{}
		for k, v := range p {
			switch x := v.(type) {
			case string:
				q[k] = cutMiddle(x, limit)
			case json.RawMessage:
				if len(x) > limit {
					q[k] = map[string]any{"truncated": cutMiddle(string(x), limit)}
				} else {
					q[k] = x
				}
			default:
				q[k] = v
			}
		}
		np = append(np, q)
	}
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	out["parts"] = np
	return out
}

// capString cuts a string longer than max bytes in the middle and marks it.
func capString(s string, max int) string { return cutMiddle(s, max) }

// cutMiddle keeps the start and the end of s, at character boundaries, and
// puts a marker with the number of bytes cut between them; the result is at
// most max bytes.
func cutMiddle(s string, max int) string {
	if len(s) <= max {
		return s
	}
	marker := func(n int) string { return fmt.Sprintf("\n[... %d bytes cut ...]\n", n) }
	keep := max - len(marker(len(s)))
	if keep < 2 {
		keep = 2
	}
	head, tail := keep/2, keep-keep/2
	for head > 0 && !utf8.RuneStart(s[head]) {
		head--
	}
	t := len(s) - tail
	for t < len(s) && !utf8.RuneStart(s[t]) {
		t++
	}
	return s[:head] + marker(t-head) + s[t:]
}

// Tool is the span of one tool call.
type Tool struct {
	t    *Turn
	span trace.Span
}

// Tool starts the span of a tool call under the root. args is the call's JSON
// input, kept as a string only with Content.
func (t *Turn) Tool(name, callID string, args json.RawMessage) *Tool {
	if t == nil {
		return nil
	}
	_, span := t.p.tracer.Start(t.ctx, "execute_tool "+name, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "execute_tool"),
		attribute.String("gen_ai.tool.name", name),
		attribute.String("gen_ai.tool.call.id", callID),
	))
	if t.p.o.Content {
		span.SetAttributes(attribute.String("gen_ai.tool.call.arguments", capArguments(args)))
	}
	return &Tool{t: t, span: span}
}

// End closes the span; isError marks it failed. With Content the result is
// kept as the JSON string {"result": <text>, "isError": <bool>}.
func (tl *Tool) End(result string, isError bool) {
	if tl == nil {
		return
	}
	if tl.t.p.o.Content {
		tl.span.SetAttributes(attribute.String("gen_ai.tool.call.result", resultJSON(result, isError)))
	}
	if isError {
		tl.span.SetStatus(codes.Error, "tool error")
	}
	tl.span.End()
}

// capArguments is a call's input as the string the receiver parses as a JSON
// object. One longer than maxAttr is replaced by an object that holds its cut
// text, so the string stays valid JSON.
func capArguments(args json.RawMessage) string {
	if len(args) == 0 || !json.Valid(args) {
		return "{}"
	}
	if len(args) <= maxAttr {
		return string(args)
	}
	return jsonObjectWithin(map[string]any{}, "truncated", string(args))
}

// resultJSON is {"result": text, "isError": isError}, with the text cut in the
// middle until the whole fits in maxAttr.
func resultJSON(text string, isError bool) string {
	return jsonObjectWithin(map[string]any{"isError": isError}, "result", text)
}

// jsonObjectWithin marshals base plus key=text, cutting text until the result
// is at most maxAttr bytes (escaping can make it longer than the text).
func jsonObjectWithin(base map[string]any, key, text string) string {
	for limit := maxAttr; ; limit /= 2 {
		base[key] = cutMiddle(text, limit)
		b, _ := json.Marshal(base)
		if len(b) <= maxAttr || limit < 64 {
			return string(b)
		}
	}
}
