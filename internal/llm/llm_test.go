package llm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func marshal(t *testing.T, req Request) map[string]any {
	t.Helper()
	b, err := json.Marshal(buildParams("test-model", req))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSystemCacheBreakpointIsOnTheLastBlockOnly(t *testing.T) {
	m := marshal(t, Request{System: []string{"one", "two", "three"}, Messages: []Message{UserText("hi")}})
	sys := m["system"].([]any)
	if len(sys) != 3 {
		t.Fatalf("system blocks: %v", sys)
	}
	for i, s := range sys {
		cc, has := s.(map[string]any)["cache_control"].(map[string]any)
		if i < 2 && has {
			t.Errorf("block %d must carry no cache_control: %v", i, s)
		}
		if i == 2 && (!has || cc["type"] != "ephemeral" || cc["ttl"] != "1h") {
			t.Errorf("the last system block needs a one-hour breakpoint: %v", s)
		}
	}
}

func TestToolsAreSortedAndSchemasPassThrough(t *testing.T) {
	schema := `{"type":"object","properties":{"id":{"type":"string"},"n":{"type":["integer","null"]}},"required":["id"],"additionalProperties":false,"$defs":{"x":{"type":"string"}}}`
	m := marshal(t, Request{
		Messages: []Message{UserText("hi")},
		Tools: []ToolDef{
			{Name: "core__zeta", Description: "z", InputSchema: json.RawMessage(schema)},
			{Name: "core__alpha", InputSchema: json.RawMessage(`{"type":"object"}`)},
			{Name: "a__mid"},
		},
	})
	tools := m["tools"].([]any)
	var names []string
	for _, tl := range tools {
		names = append(names, tl.(map[string]any)["name"].(string))
	}
	if strings.Join(names, ",") != "a__mid,core__alpha,core__zeta" {
		t.Fatalf("order: %v", names)
	}
	got := tools[2].(map[string]any)["input_schema"]
	var want any
	_ = json.Unmarshal([]byte(schema), &want)
	gb, _ := json.Marshal(got)
	wb, _ := json.Marshal(want)
	if string(gb) != string(wb) {
		t.Errorf("schema changed:\n got %s\nwant %s", gb, wb)
	}
	if tools[0].(map[string]any)["input_schema"].(map[string]any)["type"] != "object" {
		t.Errorf("a tool with no schema still needs an object schema: %v", tools[0])
	}
	if _, has := tools[1].(map[string]any)["description"]; has {
		t.Errorf("an empty description must be left out")
	}
	// The same tools in another order render the same bytes.
	a, _ := json.Marshal(buildParams("m", Request{Tools: []ToolDef{{Name: "b"}, {Name: "a"}}}))
	b, _ := json.Marshal(buildParams("m", Request{Tools: []ToolDef{{Name: "a"}, {Name: "b"}}}))
	if string(a) != string(b) {
		t.Errorf("tool order leaks into the request")
	}
}

func TestEffortThinkingAndToolChoice(t *testing.T) {
	m := marshal(t, Request{Messages: []Message{UserText("hi")}, Effort: "high"})
	oc, _ := m["output_config"].(map[string]any)
	if oc["effort"] != "high" {
		t.Errorf("output_config: %v", m["output_config"])
	}
	for _, k := range []string{"thinking", "tool_choice", "temperature"} {
		if _, has := m[k]; has {
			t.Errorf("%s must not be sent: %v", k, m[k])
		}
	}
	if m["max_tokens"].(float64) != DefaultMaxTokens || m["model"] != "test-model" {
		t.Errorf("max_tokens/model: %v %v", m["max_tokens"], m["model"])
	}
	m = marshal(t, Request{Messages: []Message{UserText("hi")}, MaxTokens: 123})
	if m["max_tokens"].(float64) != 123 {
		t.Errorf("max_tokens: %v", m["max_tokens"])
	}
	if _, has := m["output_config"]; has {
		t.Errorf("no effort, no output_config: %v", m["output_config"])
	}
}

func TestToolResultsShareOneUserMessageAndThinkingIsKept(t *testing.T) {
	m := marshal(t, Request{Messages: []Message{
		UserText("go"),
		{Role: RoleAssistant, Content: []Block{
			{Kind: KindThinking, Thinking: "hm", Signature: "sig"},
			{Kind: KindRedactedThinking, Data: "opaque"},
			{Kind: KindToolUse, ID: "t1", Name: "a", Input: json.RawMessage(`{"x":1}`)},
			{Kind: KindToolUse, ID: "t2", Name: "b"},
		}},
		{Role: RoleUser, Content: []Block{ToolResult("t1", "one", false), ToolResult("t2", "boom", true), Text("also")}},
	}})
	msgs := m["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("messages: %v", msgs)
	}
	as := msgs[1].(map[string]any)["content"].([]any)
	if as[0].(map[string]any)["type"] != "thinking" || as[0].(map[string]any)["signature"] != "sig" || as[1].(map[string]any)["type"] != "redacted_thinking" {
		t.Errorf("thinking blocks must go back as received: %v", as)
	}
	if in := as[2].(map[string]any)["input"].(map[string]any); in["x"] != float64(1) {
		t.Errorf("tool_use input: %v", as[2])
	}
	if in, ok := as[3].(map[string]any)["input"].(map[string]any); !ok || len(in) != 0 {
		t.Errorf("a tool_use with no input needs {}: %v", as[3])
	}
	us := msgs[2].(map[string]any)["content"].([]any)
	if len(us) != 3 || us[0].(map[string]any)["tool_use_id"] != "t1" || us[1].(map[string]any)["is_error"] != true || us[2].(map[string]any)["type"] != "text" {
		t.Errorf("one user message must carry every tool_result: %v", us)
	}
}

func TestLastMessageBlockGetsAShortBreakpointAndNothingElseDoes(t *testing.T) {
	m := marshal(t, Request{System: []string{"s"}, Messages: []Message{
		UserText("first"),
		{Role: RoleAssistant, Content: []Block{{Kind: KindToolUse, ID: "t1", Name: "a"}}},
		{Role: RoleUser, Content: []Block{ToolResult("t1", "r1", false)}},
	}})
	msgs := m["messages"].([]any)
	for i, msg := range msgs {
		for _, b := range msg.(map[string]any)["content"].([]any) {
			cc, has := b.(map[string]any)["cache_control"].(map[string]any)
			if i < len(msgs)-1 && has {
				t.Errorf("message %d carries a breakpoint: %v", i, b)
			}
			if i == len(msgs)-1 && (!has || cc["ttl"] != nil) {
				t.Errorf("the last block needs the default-TTL breakpoint: %v", b)
			}
		}
	}
}

func TestErrorMapping(t *testing.T) {
	for status, auth := range map[int]bool{401: true, 403: true, 429: false, 500: false, 400: false} {
		status := status
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"type":"error","error":{"type":"authentication_error","message":"nope"}}`)
		}))
		a := testClient(t, srv.URL)
		_, err := a.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}})
		srv.Close()
		var ae *APIError
		if !errors.As(err, &ae) || ae.Status != status {
			t.Fatalf("status %d: %v", status, err)
		}
		if IsAuth(err) != auth {
			t.Errorf("status %d: IsAuth = %v, want %v", status, IsAuth(err), auth)
		}
	}
	if IsAuth(errors.New("x")) || IsAuth(nil) {
		t.Error("only an APIError is an auth failure")
	}
}

func testClient(t *testing.T, base string) *Anthropic {
	t.Helper()
	key := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(key, []byte("sk-test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := NewAnthropic(context.Background(), Options{Auth: AuthKey, Model: "test-model", KeyFile: key, BaseURL: base, HTTPClient: noRetryClient{}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// noRetryClient is the default client; the SDK retries 429 and 5xx twice with
// a back-off that would slow the test, so the server answers them at once and
// the test accepts the retries.
type noRetryClient struct{}

func (noRetryClient) Do(r *http.Request) (*http.Response, error) { return http.DefaultClient.Do(r) }

func TestCompleteReadsTheKeyFileEachTimeAndParsesTheAnswer(t *testing.T) {
	var gotKey, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"test-model","stop_reason":"tool_use",
"content":[{"type":"thinking","thinking":"hm","signature":"sig"},{"type":"text","text":"looking"},{"type":"tool_use","id":"toolu_1","name":"core__a","input":{"q":"x"}}],
"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":100,"cache_creation_input_tokens":7}}`)
	}))
	defer srv.Close()
	a := testClient(t, srv.URL)
	r, err := a.Complete(context.Background(), Request{System: []string{"sys"}, Messages: []Message{UserText("hi")}, Tools: []ToolDef{{Name: "core__a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if gotKey != "sk-test" || !strings.Contains(gotBody, `"cache_control"`) {
		t.Errorf("key %q body %s", gotKey, gotBody)
	}
	if r.StopReason != StopToolUse || r.Model != "test-model" || r.Usage != (Usage{Input: 10, Output: 5, CacheRead: 100, CacheWrite: 7}) {
		t.Errorf("response: %+v", r)
	}
	if len(r.Content) != 3 || r.Content[0].Kind != KindThinking || r.Content[0].Signature != "sig" ||
		r.Content[2].Kind != KindToolUse || r.Content[2].Name != "core__a" || string(r.Content[2].Input) != `{"q":"x"}` {
		t.Errorf("content: %+v", r.Content)
	}
	if r.Usage.Billable() != 22 || r.Usage.Context() != 117 {
		t.Errorf("billable %d context %d", r.Usage.Billable(), r.Usage.Context())
	}

	// A rotated key is used by the next request.
	if err := os.WriteFile(a.keyFile, []byte("sk-rotated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}}); err != nil {
		t.Fatal(err)
	}
	if gotKey != "sk-rotated" {
		t.Errorf("key after rotation: %q", gotKey)
	}
}

func TestRefusalCarriesItsCategory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"id":"m","type":"message","role":"assistant","model":"test-model","stop_reason":"refusal","stop_details":{"type":"refusal","category":"cyber","explanation":"x"},"content":[],"usage":{"input_tokens":1,"output_tokens":0}}`)
	}))
	defer srv.Close()
	r, err := testClient(t, srv.URL).Complete(context.Background(), Request{Messages: []Message{UserText("hi")}})
	if err != nil {
		t.Fatal(err)
	}
	if r.StopReason != StopRefusal || r.RefusalCategory != "cyber" {
		t.Errorf("%+v", r)
	}
}

func TestNewAnthropicFailsFastOnABadModeOrFile(t *testing.T) {
	ctx := context.Background()
	empty := filepath.Join(t.TempDir(), "empty")
	_ = os.WriteFile(empty, nil, 0o600)
	for name, o := range map[string]Options{
		"unknown mode":       {Auth: "oauth", Model: "m"},
		"no model":           {Auth: AuthKey, KeyFile: empty},
		"missing key file":   {Auth: AuthKey, Model: "m", KeyFile: "/nonexistent/key"},
		"empty key file":     {Auth: AuthKey, Model: "m", KeyFile: empty},
		"federation no ids":  {Auth: AuthFederation, Model: "m"},
		"vertex no project":  {Auth: AuthVertex, Model: "m"},
		"federation no file": {Auth: AuthFederation, Model: "m", Federation: Federation{RuleID: "r", OrganizationID: "o", ServiceAccountID: "s"}},
	} {
		if _, err := NewAnthropic(ctx, o); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestFederationSetsItsIdsAndClearsWhatWouldShadowIt(t *testing.T) {
	for _, v := range shadowVars {
		t.Setenv(v, "shadow")
	}
	for _, v := range federationVars {
		t.Setenv(v, "")
	}
	tok := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tok, []byte("jwt"), 0o600)
	_, err := NewAnthropic(context.Background(), Options{Auth: AuthFederation, Model: "m", Federation: Federation{
		RuleID: "fdrl_x", OrganizationID: "org", ServiceAccountID: "svac_x", IdentityTokenFile: tok}})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range shadowVars {
		if os.Getenv(v) != "" {
			t.Errorf("%s still set", v)
		}
	}
	if os.Getenv("ANTHROPIC_FEDERATION_RULE_ID") != "fdrl_x" || os.Getenv("ANTHROPIC_IDENTITY_TOKEN_FILE") != tok || os.Getenv("ANTHROPIC_WORKSPACE_ID") != "" {
		t.Errorf("federation env: %v", os.Environ())
	}
}

func TestFakeRecordsAndScripts(t *testing.T) {
	f := NewFake(ToolUse(Usage{Input: 1}, Call{Name: "a", Input: `{"k":1}`}), EndTurn("done", Usage{Output: 2}))
	ctx := context.Background()
	r, err := f.Complete(ctx, Request{Messages: []Message{UserText("hi")}})
	if err != nil || r.StopReason != StopToolUse || r.Content[0].Name != "a" || r.Content[0].ID == "" {
		t.Fatalf("%+v %v", r, err)
	}
	if r, _ = f.Complete(ctx, Request{}); r.StopReason != StopEndTurn || r.Usage.Output != 2 {
		t.Fatalf("%+v", r)
	}
	if _, err := f.Complete(ctx, Request{}); err == nil {
		t.Error("an exhausted script must fail")
	}
	if len(f.Requests()) != 3 || f.Left() != 0 {
		t.Errorf("seen %d left %d", len(f.Requests()), f.Left())
	}
}
