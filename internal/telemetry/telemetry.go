// Package telemetry sends the spot's own OpenTelemetry spans to an OTLP/HTTP
// endpoint (ego). One root span covers a turn, with one span per model request
// and one per tool call, named and attributed after the OpenTelemetry GenAI
// conventions. Spans carry names, timings and token counts; the prompt, the
// answer and tool parameters and results are added only when asked for.
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
	// ToolDetails adds the arguments and result of each tool call to its span.
	ToolDetails bool
	// LogContent adds the first prompt and the final text to the root span.
	LogContent bool

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
}

// StartTurn opens the root span "invoke_agent <name>" with a fresh conversation
// id, the wake reason and, with LogContent, the first user message.
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
	if p.o.LogContent {
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
// LogContent); err marks the turn failed.
func (t *Turn) End(text string, err error) {
	if t == nil {
		return
	}
	if t.p.o.LogContent && text != "" {
		t.span.SetAttributes(attribute.String("gen_ai.output.messages", messagesJSON("assistant", text, "stop")))
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

// Chat is the span of one model request.
type Chat struct {
	t     *Turn
	span  trace.Span
	model string
}

// Chat starts the span of a model request under the root.
func (t *Turn) Chat(model string) *Chat {
	if t == nil {
		return nil
	}
	_, span := t.p.tracer.Start(t.ctx, "chat "+model, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "chat"),
		attribute.String("gen_ai.provider.name", "anthropic"),
		attribute.String("gen_ai.request.model", model),
	))
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
		attribute.Int64("gen_ai.usage.input_tokens", r.Usage.Input),
		attribute.Int64("gen_ai.usage.output_tokens", r.Usage.Output),
		attribute.Int64("gen_ai.usage.cache_read.input_tokens", r.Usage.CacheRead),
		attribute.Int64("gen_ai.usage.cache_creation.input_tokens", r.Usage.CacheWrite),
		attribute.StringSlice("gen_ai.response.finish_reasons", []string{string(r.StopReason)}),
	)
	c.span.End()
}

// Tool is the span of one tool call.
type Tool struct {
	t    *Turn
	span trace.Span
}

// Tool starts the span of a tool call under the root. args is the call's JSON
// input, kept only with ToolDetails.
func (t *Turn) Tool(name, callID string, args json.RawMessage) *Tool {
	if t == nil {
		return nil
	}
	_, span := t.p.tracer.Start(t.ctx, "execute_tool "+name, trace.WithAttributes(
		attribute.String("gen_ai.operation.name", "execute_tool"),
		attribute.String("gen_ai.tool.name", name),
		attribute.String("gen_ai.tool.call.id", callID),
	))
	if t.p.o.ToolDetails && len(args) > 0 {
		span.SetAttributes(attribute.String("gen_ai.tool.call.arguments", string(args)))
	}
	return &Tool{t: t, span: span}
}

// End closes the span; isError marks it failed. result is kept only with
// ToolDetails.
func (tl *Tool) End(result string, isError bool) {
	if tl == nil {
		return
	}
	if tl.t.p.o.ToolDetails {
		tl.span.SetAttributes(attribute.String("gen_ai.tool.call.result", result))
	}
	if isError {
		tl.span.SetStatus(codes.Error, "tool error")
	}
	tl.span.End()
}
