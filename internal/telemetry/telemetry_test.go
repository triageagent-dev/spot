package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/triageagent-dev/spot/internal/llm"
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
	c := turn.Chat("claude-x", llm.Request{System: []string{"sys"}, Messages: []llm.Message{llm.UserText("the user prompt")}})
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
		ca["gen_ai.usage.input_tokens"].AsInt64() != 117 || ca["gen_ai.usage.output_tokens"].AsInt64() != 5 ||
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
			t.Error("tool arguments need Content")
		}
		if _, has := a["gen_ai.tool.call.result"]; has {
			t.Error("tool results need Content")
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

// twoStepTurn runs a turn of two model requests and one tool call, the way the
// agent does, and returns the spans.
func twoStepTurn(t *testing.T, o Options) tracetest.SpanStubs {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	o.exporter = exp
	p, err := New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	_, turn := p.StartTurn(context.Background(), "human", "Wake reason: human")
	sys := []string{"You are the spot.", "Be brief."}
	m1 := []llm.Message{llm.UserText("Wake reason: human\nsomeone asks about checkout")}
	c1 := turn.Chat("claude-x", llm.Request{System: sys, Messages: m1})
	call := llm.Block{Kind: llm.KindToolUse, ID: "toolu_1", Name: "core__list_open_issues", Input: json.RawMessage(`{"limit":5}`)}
	asst := llm.Message{Role: llm.RoleAssistant, Content: []llm.Block{
		{Kind: llm.KindThinking, Thinking: "hm", Signature: "sig"}, llm.Text("let me look"), call}}
	c1.End(llm.Response{Model: "claude-x", StopReason: llm.StopToolUse, Content: asst.Content, Usage: llm.Usage{Input: 10, Output: 5}}, nil)
	tl := turn.Tool("core__list_open_issues", "toolu_1", call.Input)
	tl.End("two issues", false)
	m2 := append(append([]llm.Message(nil), m1...), asst, llm.Message{Role: llm.RoleUser, Content: []llm.Block{llm.ToolResult("toolu_1", "two issues", false)}})
	c2 := turn.Chat("claude-x", llm.Request{System: sys, Messages: m2})
	c2.End(llm.Response{Model: "claude-x", StopReason: llm.StopEndTurn, Content: []llm.Block{llm.Text("Checkout is fine.")}, Usage: llm.Usage{Input: 20, Output: 6}}, nil)
	turn.End("Checkout is fine.", nil)
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	return exp.GetSpans()
}

type gaMessage struct {
	Role  string `json:"role"`
	Parts []struct {
		Type      string          `json:"type"`
		Content   string          `json:"content"`
		ID        string          `json:"id"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
		Response  string          `json:"response"`
	} `json:"parts"`
	FinishReason string `json:"finish_reason"`
}

func decode(t *testing.T, s string) []gaMessage {
	t.Helper()
	var out []gaMessage
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		t.Fatalf("not a message array: %v\n%s", err, s)
	}
	return out
}

func TestContentForTheJudge(t *testing.T) {
	spans := twoStepTurn(t, Options{Endpoint: "http://x", Name: "n", Content: true})
	chats := byName(spans, "chat ")
	root := attrs(byName(spans, "invoke_agent ")[0].Attributes)

	// The first chat span: the system prompt and the one user message ego reads.
	c1 := attrs(chats[0].Attributes)
	if got := c1["gen_ai.system_instructions"].AsString(); got != "You are the spot.\n\nBe brief." {
		t.Errorf("system instructions: %q", got)
	}
	in1 := decode(t, c1["gen_ai.input.messages"].AsString())
	if len(in1) != 1 || in1[0].Role != "user" || in1[0].Parts[0].Type != "text" || !strings.Contains(in1[0].Parts[0].Content, "someone asks about checkout") {
		t.Errorf("first input: %+v", in1)
	}
	t.Logf("gen_ai.input.messages of the second chat span:\n%s", attrs(chats[1].Attributes)["gen_ai.input.messages"].AsString())

	// The second: the whole conversation so far, thinking left out.
	c2 := attrs(chats[1].Attributes)
	if _, has := c2["gen_ai.system_instructions"]; has {
		t.Error("only the first chat span carries the system prompt")
	}
	in2 := decode(t, c2["gen_ai.input.messages"].AsString())
	if len(in2) != 3 || in2[1].Role != "assistant" || len(in2[1].Parts) != 2 || in2[1].Parts[0].Content != "let me look" {
		t.Fatalf("second input: %+v", in2)
	}
	if p := in2[1].Parts[1]; p.Type != "tool_call" || p.ID != "toolu_1" || p.Name != "core__list_open_issues" || string(p.Arguments) != `{"limit":5}` {
		t.Errorf("tool_call part: %+v", p)
	}
	if p := in2[2].Parts[0]; in2[2].Role != "user" || p.Type != "tool_call_response" || p.ID != "toolu_1" || p.Response != "two issues" {
		t.Errorf("tool_call_response part: %+v", p)
	}
	if strings.Contains(c2["gen_ai.input.messages"].AsString(), "thinking") || strings.Contains(c2["gen_ai.input.messages"].AsString(), "sig") {
		t.Error("thinking blocks must be left out")
	}

	// The answers: one assistant message built from the response blocks.
	out1 := decode(t, c1["gen_ai.output.messages"].AsString())
	if len(out1) != 1 || out1[0].Role != "assistant" || len(out1[0].Parts) != 2 || out1[0].Parts[1].Type != "tool_call" || out1[0].FinishReason != "tool_use" {
		t.Errorf("first output: %+v", out1)
	}
	out2 := decode(t, c2["gen_ai.output.messages"].AsString())
	if len(out2) != 1 || out2[0].Parts[0].Content != "Checkout is fine." {
		t.Errorf("last output: %+v", out2)
	}

	// The root keeps the fallback.
	if r := decode(t, root["gen_ai.input.messages"].AsString()); len(r) != 1 || r[0].Role != "user" || r[0].Parts[0].Content != "Wake reason: human" {
		t.Errorf("root input: %+v", r)
	}
	if r := decode(t, root["gen_ai.output.messages"].AsString()); len(r) != 1 || r[0].Role != "assistant" || r[0].Parts[0].Content != "Checkout is fine." {
		t.Errorf("root output: %+v", r)
	}

	// The tool span: arguments as a JSON object string, the result as JSON.
	ta := attrs(byName(spans, "execute_tool ")[0].Attributes)
	if ta["gen_ai.tool.call.arguments"].AsString() != `{"limit":5}` {
		t.Errorf("arguments: %q", ta["gen_ai.tool.call.arguments"].AsString())
	}
	var res struct {
		Result  string `json:"result"`
		IsError bool   `json:"isError"`
	}
	if err := json.Unmarshal([]byte(ta["gen_ai.tool.call.result"].AsString()), &res); err != nil || res.Result != "two issues" || res.IsError {
		t.Errorf("result: %q %v", ta["gen_ai.tool.call.result"].AsString(), err)
	}
}

func TestToolErrorResultIsFlagged(t *testing.T) {
	spans := run(t, Options{Endpoint: "http://x", Name: "n", Content: true})
	var found bool
	for _, s := range byName(spans, "execute_tool core__get_pods") {
		a := attrs(s.Attributes)
		var res struct {
			Result  string `json:"result"`
			IsError bool   `json:"isError"`
		}
		_ = json.Unmarshal([]byte(a["gen_ai.tool.call.result"].AsString()), &res)
		found = res.IsError && res.Result == "boom"
		if a["gen_ai.tool.call.arguments"].AsString() != "{}" {
			t.Errorf("a call with no input still has a JSON object: %q", a["gen_ai.tool.call.arguments"].AsString())
		}
	}
	if !found {
		t.Error("the error result must say isError")
	}
}

func TestNoContentAttributesWhenContentIsOff(t *testing.T) {
	spans := twoStepTurn(t, Options{Endpoint: "http://x", Name: "n"})
	for _, s := range spans {
		for k := range attrs(s.Attributes) {
			if strings.Contains(k, "messages") || strings.Contains(k, "system_instructions") || strings.Contains(k, "tool.call.arguments") || strings.Contains(k, "tool.call.result") {
				t.Errorf("%s carries %s with content off", s.Name, k)
			}
		}
	}
}

func TestOversizedMessagesDropTheOldestAndKeepTheLast(t *testing.T) {
	var msgs []llm.Message
	big := strings.Repeat("x", 20<<10)
	for i := 0; i < 8; i++ {
		msgs = append(msgs, llm.Message{Role: llm.RoleUser, Content: []llm.Block{llm.Text(fmt.Sprintf("message %d %s", i, big))}})
	}
	msgs = append(msgs, llm.UserText("the newest question"))
	out := inputMessagesJSON(msgs)
	if len(out) > maxAttr {
		t.Fatalf("%d bytes, cap %d", len(out), maxAttr)
	}
	got := decode(t, out)
	first := got[0]
	if first.Role != "user" || first.Parts[0].Type != "text" || !strings.HasSuffix(first.Parts[0].Content, " earlier messages omitted]") || !strings.HasPrefix(first.Parts[0].Content, "[") {
		t.Fatalf("the first message must say what was dropped: %+v", first)
	}
	last := got[len(got)-1]
	if last.Parts[0].Content != "the newest question" {
		t.Errorf("the last message must survive: %+v", last)
	}
	if strings.Contains(out, "message 0 ") {
		t.Error("the oldest messages go first")
	}
	var n int
	if _, err := fmt.Sscanf(first.Parts[0].Content, "[%d earlier messages omitted]", &n); err != nil || n != 9-(len(got)-1) {
		t.Errorf("omitted count %d for %d kept: %q", n, len(got)-1, first.Parts[0].Content)
	}

	// Nothing to drop: untouched, no note.
	small := inputMessagesJSON(msgs[8:])
	if strings.Contains(small, "omitted") {
		t.Errorf("small input must be untouched: %s", small)
	}
}

func TestOneHugeMessageIsCutInTheMiddle(t *testing.T) {
	huge := "START" + strings.Repeat("é", 100<<10) + "END"
	out := inputMessagesJSON([]llm.Message{llm.UserText(huge)})
	if len(out) > maxAttr {
		t.Fatalf("%d bytes", len(out))
	}
	got := decode(t, out)
	c := got[len(got)-1].Parts[0].Content
	if !strings.HasPrefix(c, "START") || !strings.HasSuffix(c, "END") || !strings.Contains(c, "bytes cut") {
		t.Errorf("head and tail must survive with a marker: %.60s ... %.40s", c, c[len(c)-40:])
	}
	if !utf8.ValidString(c) {
		t.Error("a cut must not split a character")
	}
}

func TestLongStringsAreCutInTheMiddleAndMarked(t *testing.T) {
	s := strings.Repeat("a", 100<<10)
	c := cutMiddle(s, 1000)
	if len(c) > 1000 || !strings.Contains(c, "bytes cut") || len(cutMiddle("short", 1000)) != 5 {
		t.Errorf("cutMiddle: %d %q", len(c), c[400:560])
	}

	// A huge tool result: still valid JSON with isError, within the cap.
	r := resultJSON(strings.Repeat("\n\"", 80<<10), true)
	var v struct {
		Result  string `json:"result"`
		IsError bool   `json:"isError"`
	}
	if len(r) > maxAttr || json.Unmarshal([]byte(r), &v) != nil || !v.IsError || !strings.Contains(v.Result, "bytes cut") {
		t.Errorf("result: %d bytes, %v", len(r), v.IsError)
	}
	// Huge arguments stay a JSON object.
	a := capArguments(json.RawMessage(`{"q":"` + strings.Repeat("z", 100<<10) + `"}`))
	var obj map[string]any
	if len(a) > maxAttr || json.Unmarshal([]byte(a), &obj) != nil {
		t.Errorf("arguments: %d bytes", len(a))
	}
	// And the root's final text.
	root := messagesJSON("assistant", capString(s, maxAttr-512), "stop")
	if len(root) > maxAttr {
		t.Errorf("root output: %d", len(root))
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
	turn.Chat("m", llm.Request{}).End(llm.Response{}, nil)
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
