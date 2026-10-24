package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/triageagent-dev/tria-spot/internal/llm"
)

func attrs(kv []attribute.KeyValue) map[string]attribute.Value {
	m := map[string]attribute.Value{}
	for _, a := range kv {
		m[string(a.Key)] = a.Value
	}
	return m
}

func run(t *testing.T, o Options) tracetest.SpanStubs {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	o.exporter = exp
	p, err := New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	_, turn := p.StartTurn(context.Background(), "edge", "the user prompt")
	c := turn.Chat("claude-x")
	c.End(llm.Response{Model: "claude-x-2", StopReason: llm.StopToolUse, Usage: llm.Usage{Input: 10, Output: 5, CacheRead: 100, CacheWrite: 7}}, nil)
	tl := turn.Tool("core__list_open_issues", "toolu_1", json.RawMessage(`{"limit":5}`))
	tl.End("the result", false)
	bad := turn.Tool("core__get_pods", "toolu_2", nil)
	bad.End("boom", true)
	turn.End("final text", nil)
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	return exp.GetSpans()
}

func byName(spans tracetest.SpanStubs, prefix string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for _, s := range spans {
		if strings.HasPrefix(s.Name, prefix) {
			out = append(out, s)
		}
	}
	return out
}

func TestTurnSpansAndResource(t *testing.T) {
	spans := run(t, Options{Endpoint: "http://x", Name: "example-spot", Tenant: "example", TenantAttr: "triage.tenant"})
	if len(spans) != 4 {
		t.Fatalf("spans: %d", len(spans))
	}
	root := byName(spans, "invoke_agent ")[0]
	if root.Name != "invoke_agent example-spot" {
		t.Errorf("root: %s", root.Name)
	}
	ra := attrs(root.Attributes)
	if ra["gen_ai.operation.name"].AsString() != "invoke_agent" || ra["tria.wake.reason"].AsString() != "edge" || ra["gen_ai.agent.name"].AsString() != "example-spot" {
		t.Errorf("root attributes: %v", ra)
	}
	if id := ra["gen_ai.conversation.id"].AsString(); len(id) != 36 {
		t.Errorf("conversation id: %q", id)
	}
	if _, has := ra["gen_ai.input.messages"]; has {
		t.Error("the prompt must not be on the span without LogContent")
	}
	if _, has := ra["gen_ai.output.messages"]; has {
		t.Error("the answer must not be on the span without LogContent")
	}
	res := attrs(root.Resource.Attributes())
	for k, want := range map[string]string{
		"service.name": "example-spot", "gen_ai.agent.name": "example-spot", "ego.thread": "example-spot",
		"triage.tenant": "example", "ego.tenant.name": "example",
	} {
		if res[k].AsString() != want {
			t.Errorf("resource %s = %q, want %q (%v)", k, res[k].AsString(), want, res)
		}
	}

	chat := byName(spans, "chat ")[0]
	if chat.Name != "chat claude-x" || chat.Parent.SpanID() != root.SpanContext.SpanID() {
		t.Errorf("chat span: %s parent %v", chat.Name, chat.Parent)
	}
	ca := attrs(chat.Attributes)
	if ca["gen_ai.operation.name"].AsString() != "chat" || ca["gen_ai.provider.name"].AsString() != "anthropic" ||
		ca["gen_ai.request.model"].AsString() != "claude-x" || ca["gen_ai.response.model"].AsString() != "claude-x-2" ||
		ca["gen_ai.usage.input_tokens"].AsInt64() != 10 || ca["gen_ai.usage.output_tokens"].AsInt64() != 5 ||
		ca["gen_ai.usage.cache_read.input_tokens"].AsInt64() != 100 || ca["gen_ai.usage.cache_creation.input_tokens"].AsInt64() != 7 ||
		strings.Join(ca["gen_ai.response.finish_reasons"].AsStringSlice(), ",") != "tool_use" {
		t.Errorf("chat attributes: %v", ca)
	}

	tools := byName(spans, "execute_tool ")
	if len(tools) != 2 {
		t.Fatalf("tool spans: %d", len(tools))
	}
	for _, s := range tools {
		if s.Parent.SpanID() != root.SpanContext.SpanID() {
			t.Errorf("%s must be under the root", s.Name)
		}
		a := attrs(s.Attributes)
		if a["gen_ai.operation.name"].AsString() != "execute_tool" || a["gen_ai.tool.name"].AsString() != strings.TrimPrefix(s.Name, "execute_tool ") || a["gen_ai.tool.call.id"].AsString() == "" {
			t.Errorf("tool attributes: %v", a)
		}
		if _, has := a["gen_ai.tool.call.arguments"]; has {
			t.Error("tool arguments need ToolDetails")
		}
		if _, has := a["gen_ai.tool.call.result"]; has {
			t.Error("tool results need ToolDetails")
		}
	}
	if tools[0].Status.Code == codes.Error || tools[1].Status.Code != codes.Error {
		t.Errorf("only the failing tool is an error: %v %v", tools[0].Status, tools[1].Status)
	}
}

func TestDefaultsOfTheResource(t *testing.T) {
	spans := run(t, Options{Endpoint: "http://x", Name: "example-spot"})
	res := attrs(spans[0].Resource.Attributes())
	if res["ego.tenant"].AsString() != "example-spot" || res["ego.tenant.name"].AsString() != "example-spot" {
		t.Errorf("resource: %v", res)
	}
	spans = run(t, Options{Endpoint: "http://x", Name: "example-spot", Tenant: "t1", TenantName: "Example workspace"})
	res = attrs(spans[0].Resource.Attributes())
	if res["ego.tenant"].AsString() != "t1" || res["ego.tenant.name"].AsString() != "Example workspace" {
		t.Errorf("resource: %v", res)
	}
}

func TestContentOnlyWhenAsked(t *testing.T) {
	spans := run(t, Options{Endpoint: "http://x", Name: "n", LogContent: true, ToolDetails: true})
	ra := attrs(byName(spans, "invoke_agent ")[0].Attributes)
	var in []map[string]any
	if err := json.Unmarshal([]byte(ra["gen_ai.input.messages"].AsString()), &in); err != nil || len(in) != 1 || in[0]["role"] != "user" ||
		!strings.Contains(ra["gen_ai.input.messages"].AsString(), "the user prompt") {
		t.Errorf("input messages: %v %v", ra["gen_ai.input.messages"].AsString(), err)
	}
	if !strings.Contains(ra["gen_ai.output.messages"].AsString(), "final text") {
		t.Errorf("output messages: %v", ra["gen_ai.output.messages"].AsString())
	}
	ta := attrs(byName(spans, "execute_tool ")[0].Attributes)
	if ta["gen_ai.tool.call.arguments"].AsString() != `{"limit":5}` || ta["gen_ai.tool.call.result"].AsString() != "the result" {
		t.Errorf("tool details: %v", ta)
	}
}

func TestOffIsNil(t *testing.T) {
	p, err := New(context.Background(), Options{Name: "n"})
	if err != nil || p != nil {
		t.Fatalf("%v %v", p, err)
	}
	ctx, turn := p.StartTurn(context.Background(), "tick", "x")
	if ctx == nil || turn != nil || turn.ID() != "" {
		t.Fatal("a disabled turn is nil")
	}
	turn.Chat("m").End(llm.Response{}, nil)
	turn.Tool("t", "id", nil).End("r", true)
	turn.End("t", nil)
	if p.Flush(ctx) != nil || p.Shutdown(ctx) != nil {
		t.Error("nil provider flush")
	}
}

func TestExportGoesToTheEndpointWithTheTokenFromTheFile(t *testing.T) {
	var path, auth string
	got := make(chan struct{}, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, auth = r.URL.Path, r.Header.Get("Authorization")
		_, _ = io.Copy(io.Discard, r.Body)
		got <- struct{}{}
	}))
	defer srv.Close()
	tok := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tok, []byte("sender-token\n"), 0o600)
	p, err := New(context.Background(), Options{Endpoint: srv.URL, TokenFile: tok, Name: "n"})
	if err != nil || p == nil {
		t.Fatalf("%v %v", p, err)
	}
	_, turn := p.StartTurn(context.Background(), "tick", "x")
	turn.End("", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	default:
		t.Fatal("nothing was exported")
	}
	if path != "/v1/traces" || auth != "Bearer sender-token" {
		t.Errorf("path %q auth %q", path, auth)
	}
}

func TestTracesURL(t *testing.T) {
	for in, want := range map[string]string{
		"http://ego.ns.svc:4318":  "http://ego.ns.svc:4318/v1/traces",
		"http://ego.ns.svc:4318/": "http://ego.ns.svc:4318/v1/traces",
		"https://h/otlp":          "https://h/otlp/v1/traces",
		"http://h:4318/v1/traces": "http://h:4318/v1/traces",
	} {
		if got, err := tracesURL(in); err != nil || got != want {
			t.Errorf("%s -> %s, %v (want %s)", in, got, err, want)
		}
	}
	if _, err := tracesURL("ego:4318"); err == nil {
		t.Error("a URL without a scheme is an error")
	}
}

func TestProviderNameFollowsTheOption(t *testing.T) {
	spans := run(t, Options{Endpoint: "http://x", Name: "example-spot", Tenant: "example", TenantAttr: "triage.tenant", Provider: "gemini"})
	if got := attrs(byName(spans, "chat ")[0].Attributes)["gen_ai.provider.name"].AsString(); got != "gemini" {
		t.Errorf("provider = %q", got)
	}
}
