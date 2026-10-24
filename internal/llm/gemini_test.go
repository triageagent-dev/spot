package llm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"google.golang.org/genai"
)

// geminiServer is a fake Gemini API: it records each request and answers with
// the next canned reply.
type geminiServer struct {
	*httptest.Server
	mu      sync.Mutex
	bodies  []map[string]any
	headers []http.Header
	urls    []string
	reply   func(n int) (int, string)
}

func newGeminiServer(t *testing.T, reply func(n int) (int, string)) *geminiServer {
	t.Helper()
	s := &geminiServer{reply: reply}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		s.mu.Lock()
		s.bodies = append(s.bodies, m)
		s.headers = append(s.headers, r.Header.Clone())
		s.urls = append(s.urls, r.URL.String())
		n := len(s.bodies)
		s.mu.Unlock()
		code, body := s.reply(n)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(s.Close)
	return s
}

const okReply = `{"candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":2},"modelVersion":"gemini-test-001"}`

func keyFile(t *testing.T, key string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(p, []byte(key+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func newTestGemini(t *testing.T, s *geminiServer, model, kf string) *Gemini {
	t.Helper()
	g, err := NewGemini(context.Background(), GeminiOptions{
		Auth: AuthKey, Model: model, KeyFile: kf, BaseURL: s.URL, HTTPClient: s.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func sub(t *testing.T, m map[string]any, path ...string) any {
	t.Helper()
	var cur any = m
	for _, p := range path {
		switch v := cur.(type) {
		case map[string]any:
			cur = v[p]
		case []any:
			var i int
			for _, c := range p {
				i = i*10 + int(c-'0')
			}
			if i >= len(v) {
				t.Fatalf("path %v: index %d out of range", path, i)
			}
			cur = v[i]
		default:
			t.Fatalf("path %v: %q reached %T", path, p, cur)
		}
	}
	return cur
}

func TestGeminiRequestShape(t *testing.T) {
	s := newGeminiServer(t, func(int) (int, string) { return 200, okReply })
	g := newTestGemini(t, s, "gemini-2.5-flash", keyFile(t, "test-key-1"))
	schema := `{"type":"object","properties":{"id":{"type":"string"},"n":{"type":["integer","null"]}},"required":["id"],"additionalProperties":false,"$defs":{"x":{"type":"string"}}}`
	sig := base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 250})
	resp, err := g.Complete(context.Background(), Request{
		System:    []string{"first", "second"},
		MaxTokens: 777,
		Effort:    "high",
		Tools: []ToolDef{
			{Name: "core__zeta", Description: "z", InputSchema: json.RawMessage(schema)},
			{Name: "core__alpha", Description: "a", InputSchema: json.RawMessage(`{"type":"object"}`)},
		},
		Messages: []Message{
			UserText("hello"),
			{Role: RoleAssistant, Content: []Block{
				{Kind: KindThinking, Thinking: "hmm", Signature: sig},
				{Kind: KindToolUse, ID: "id-1", Name: "core__zeta", Input: json.RawMessage(`{"id":"a","n":12345678901234567}`), Signature: sig},
				{Kind: KindToolUse, ID: "call_7", Name: "core__alpha", Input: json.RawMessage(`{}`)},
			}},
			{Role: RoleUser, Content: []Block{
				ToolResult("id-1", "all fine", false),
				ToolResult("call_7", "boom", true),
				Text("also: a person wrote"),
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "gemini-test-001" || resp.StopReason != StopEndTurn {
		t.Errorf("response: %+v", resp)
	}
	body, hdr, url := s.bodies[0], s.headers[0], s.urls[0]

	if got := hdr.Get("x-goog-api-key"); got != "test-key-1" {
		t.Errorf("x-goog-api-key = %q", got)
	}
	if strings.Contains(url, "key=") || strings.Contains(url, "test-key-1") {
		t.Errorf("the key is in the URL: %s", url)
	}
	if !strings.Contains(url, "/models/gemini-2.5-flash:generateContent") {
		t.Errorf("url = %s", url)
	}

	sys := sub(t, body, "systemInstruction", "parts").([]any)
	if len(sys) != 2 || sub(t, body, "systemInstruction", "parts", "0", "text") != "first" || sub(t, body, "systemInstruction", "parts", "1", "text") != "second" {
		t.Errorf("system instruction: %v", sys)
	}

	tools := sub(t, body, "tools").([]any)
	if len(tools) != 1 {
		t.Fatalf("want one tool, got %d", len(tools))
	}
	decls := sub(t, body, "tools", "0", "functionDeclarations").([]any)
	if len(decls) != 2 || decls[0].(map[string]any)["name"] != "core__alpha" || decls[1].(map[string]any)["name"] != "core__zeta" {
		t.Fatalf("declarations: %v", decls)
	}
	if decls[1].(map[string]any)["description"] != "z" {
		t.Errorf("description: %v", decls[1])
	}
	var want any
	_ = json.Unmarshal([]byte(schema), &want)
	if !jsonEqual(decls[1].(map[string]any)["parametersJsonSchema"], want) {
		t.Errorf("schema not passed through:\n got %v\nwant %v", decls[1].(map[string]any)["parametersJsonSchema"], want)
	}
	if _, has := body["toolConfig"]; has {
		t.Errorf("tool choice must stay automatic: %v", body["toolConfig"])
	}

	gc := sub(t, body, "generationConfig").(map[string]any)
	if gc["maxOutputTokens"] != float64(777) {
		t.Errorf("maxOutputTokens = %v", gc["maxOutputTokens"])
	}
	if sub(t, gc, "thinkingConfig", "thinkingBudget") != float64(8192) {
		t.Errorf("thinkingConfig = %v", gc["thinkingConfig"])
	}
	if inc, _ := sub(t, gc, "thinkingConfig").(map[string]any)["includeThoughts"].(bool); inc {
		t.Errorf("thoughts must not be requested")
	}

	contents := sub(t, body, "contents").([]any)
	if len(contents) != 3 {
		t.Fatalf("contents: %v", contents)
	}
	if sub(t, body, "contents", "0", "role") != "user" || sub(t, body, "contents", "0", "parts", "0", "text") != "hello" {
		t.Errorf("first content: %v", contents[0])
	}
	if sub(t, body, "contents", "1", "role") != "model" {
		t.Errorf("assistant role: %v", contents[1])
	}
	parts := sub(t, body, "contents", "1", "parts").([]any)
	if len(parts) != 3 {
		t.Fatalf("model parts: %v", parts)
	}
	th := parts[0].(map[string]any)
	if th["thought"] != true || th["text"] != "hmm" || th["thoughtSignature"] != sig {
		t.Errorf("thought part: %v", th)
	}
	fc := parts[1].(map[string]any)
	if sub(t, fc, "functionCall", "name") != "core__zeta" || sub(t, fc, "functionCall", "id") != "id-1" || fc["thoughtSignature"] != sig {
		t.Errorf("function call part: %v", fc)
	}
	if sub(t, fc, "functionCall", "args", "id") != "a" {
		t.Errorf("args: %v", fc)
	}
	if n, ok := sub(t, fc, "functionCall", "args", "n").(float64); !ok || n != 12345678901234567 {
		t.Errorf("big number: %v", sub(t, fc, "functionCall", "args", "n"))
	}
	gen := parts[2].(map[string]any)
	if _, has := gen["thoughtSignature"]; has {
		t.Errorf("no signature, none sent: %v", gen)
	}
	if _, has := sub(t, gen, "functionCall").(map[string]any)["id"]; has {
		t.Errorf("a generated id must not be sent back: %v", gen)
	}

	if sub(t, body, "contents", "2", "role") != "user" {
		t.Errorf("tool result role: %v", contents[2])
	}
	r1 := sub(t, body, "contents", "2", "parts", "0", "functionResponse").(map[string]any)
	if r1["name"] != "core__zeta" || r1["id"] != "id-1" || sub(t, r1, "response", "result") != "all fine" {
		t.Errorf("function response 1: %v", r1)
	}
	r2 := sub(t, body, "contents", "2", "parts", "1", "functionResponse").(map[string]any)
	if r2["name"] != "core__alpha" || sub(t, r2, "response", "error") != "boom" {
		t.Errorf("function response 2: %v", r2)
	}
	if _, has := r2["id"]; has {
		t.Errorf("a generated id must not be sent back: %v", r2)
	}
	if sub(t, body, "contents", "2", "parts", "2", "text") != "also: a person wrote" {
		t.Errorf("trailing text: %v", contents[2])
	}
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func TestGeminiDefaultMaxTokensAndNoOptionalFields(t *testing.T) {
	s := newGeminiServer(t, func(int) (int, string) { return 200, okReply })
	g := newTestGemini(t, s, "gemini-2.5-flash", keyFile(t, "k"))
	if _, err := g.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}}); err != nil {
		t.Fatal(err)
	}
	body := s.bodies[0]
	if sub(t, body, "generationConfig", "maxOutputTokens") != float64(DefaultMaxTokens) {
		t.Errorf("generationConfig: %v", body["generationConfig"])
	}
	for _, k := range []string{"tools", "systemInstruction", "toolConfig"} {
		if _, has := body[k]; has {
			t.Errorf("%s must be absent: %v", k, body[k])
		}
	}
	if _, has := body["generationConfig"].(map[string]any)["thinkingConfig"]; has {
		t.Errorf("no effort, no thinking config")
	}
}

func TestGeminiThinkingTable(t *testing.T) {
	i := func(n int32) *int32 { return &n }
	cases := []struct {
		model, effort string
		level         genai.ThinkingLevel
		budget        *int32
		none          bool
	}{
		{"gemini-3-pro-preview", "low", genai.ThinkingLevelLow, nil, false},
		{"gemini-3.1-flash", "medium", genai.ThinkingLevelMedium, nil, false},
		{"gemini-3-flash", "high", genai.ThinkingLevelHigh, nil, false},
		{"gemini-3-flash", "xhigh", genai.ThinkingLevelHigh, nil, false},
		{"gemini-3-flash", "max", genai.ThinkingLevelHigh, nil, false},
		{"gemini-3-flash", "", "", nil, true},
		{"gemini-2.5-flash", "low", "", i(1024), false},
		{"gemini-2.5-flash", "medium", "", i(4096), false},
		{"gemini-2.5-pro", "high", "", i(8192), false},
		{"gemini-2.5-pro", "xhigh", "", i(-1), false},
		{"gemini-2.5-pro", "max", "", i(-1), false},
		{"gemini-2.5-pro", "", "", nil, true},
		{"gemini-2.5-pro", "unknown", "", nil, true},
	}
	for _, c := range cases {
		got := geminiThinking(c.model, c.effort)
		if c.none {
			if got != nil {
				t.Errorf("%s/%q: want no config, got %+v", c.model, c.effort, got)
			}
			continue
		}
		if got == nil {
			t.Errorf("%s/%q: no config", c.model, c.effort)
			continue
		}
		if got.ThinkingLevel != c.level {
			t.Errorf("%s/%q: level %q, want %q", c.model, c.effort, got.ThinkingLevel, c.level)
		}
		if (got.ThinkingBudget == nil) != (c.budget == nil) || (c.budget != nil && *got.ThinkingBudget != *c.budget) {
			t.Errorf("%s/%q: budget %v, want %v", c.model, c.effort, got.ThinkingBudget, c.budget)
		}
		if got.IncludeThoughts {
			t.Errorf("%s/%q: thoughts must not be included", c.model, c.effort)
		}
	}
}

func TestGeminiThinkingLevelReachesTheWire(t *testing.T) {
	s := newGeminiServer(t, func(int) (int, string) { return 200, okReply })
	g := newTestGemini(t, s, "gemini-3-pro-preview", keyFile(t, "k"))
	if _, err := g.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}, Effort: "medium"}); err != nil {
		t.Fatal(err)
	}
	if sub(t, s.bodies[0], "generationConfig", "thinkingConfig", "thinkingLevel") != "MEDIUM" {
		t.Errorf("generationConfig: %v", s.bodies[0]["generationConfig"])
	}
	if _, has := sub(t, s.bodies[0], "generationConfig", "thinkingConfig").(map[string]any)["thinkingBudget"]; has {
		t.Errorf("level and budget are exclusive")
	}
}

func TestGeminiResponseMapping(t *testing.T) {
	sigA := base64.StdEncoding.EncodeToString([]byte("sig-a"))
	s := newGeminiServer(t, func(int) (int, string) {
		return 200, `{"candidates":[{"content":{"role":"model","parts":[
			{"text":"thinking out loud","thought":true,"thoughtSignature":"` + sigA + `"},
			{"text":"let me look"},
			{"functionCall":{"name":"core__alpha","args":{"q":"x","n":3}},"thoughtSignature":"` + sigA + `"},
			{"functionCall":{"id":"api-id","name":"core__zeta","args":{}}}
		]},"finishReason":"STOP"}],
		"usageMetadata":{"promptTokenCount":1000,"cachedContentTokenCount":400,"candidatesTokenCount":50,"thoughtsTokenCount":25,"totalTokenCount":1075}}`
	})
	g := newTestGemini(t, s, "gemini-2.5-flash", keyFile(t, "k"))
	r, err := g.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}})
	if err != nil {
		t.Fatal(err)
	}
	if r.StopReason != StopToolUse {
		t.Errorf("stop = %q", r.StopReason)
	}
	if r.Model != "gemini-2.5-flash" {
		t.Errorf("no modelVersion: the request model, got %q", r.Model)
	}
	if want := (Usage{Input: 600, CacheRead: 400, Output: 75}); r.Usage != want {
		t.Errorf("usage = %+v, want %+v", r.Usage, want)
	}
	if len(r.Content) != 4 {
		t.Fatalf("content: %+v", r.Content)
	}
	if b := r.Content[0]; b.Kind != KindThinking || b.Thinking != "thinking out loud" || b.Signature != sigA {
		t.Errorf("thinking: %+v", b)
	}
	if b := r.Content[1]; b.Kind != KindText || b.Text != "let me look" {
		t.Errorf("text: %+v", b)
	}
	b := r.Content[2]
	if b.Kind != KindToolUse || b.Name != "core__alpha" || b.ID != "call_1" || b.Signature != sigA {
		t.Errorf("tool use without an id: %+v", b)
	}
	if !jsonEqual(json.RawMessage(b.Input), map[string]any{"q": "x", "n": 3}) {
		t.Errorf("input = %s", b.Input)
	}
	if b := r.Content[3]; b.ID != "api-id" || string(b.Input) != `{}` {
		t.Errorf("tool use with an id: %+v", b)
	}

	// A second response gets a new generated id.
	r2, err := g.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Content[2].ID != "call_2" {
		t.Errorf("second generated id = %q", r2.Content[2].ID)
	}
}

func TestGeminiFunctionCallSignatureRoundTrips(t *testing.T) {
	sig := base64.StdEncoding.EncodeToString([]byte{9, 8, 7})
	n := 0
	s := newGeminiServer(t, func(int) (int, string) {
		n++
		if n == 1 {
			return 200, `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"core__alpha","args":{"q":"x"}},"thoughtSignature":"` + sig + `"}]},"finishReason":"STOP"}]}`
		}
		return 200, okReply
	})
	g := newTestGemini(t, s, "gemini-3-pro-preview", keyFile(t, "k"))
	first, err := g.Complete(context.Background(), Request{Messages: []Message{UserText("go")}})
	if err != nil {
		t.Fatal(err)
	}
	use := first.Content[0]
	msgs := []Message{UserText("go"), {Role: RoleAssistant, Content: first.Content}, {Role: RoleUser, Content: []Block{ToolResult(use.ID, "ok", false)}}}
	if _, err := g.Complete(context.Background(), Request{Messages: msgs}); err != nil {
		t.Fatal(err)
	}
	part := sub(t, s.bodies[1], "contents", "1", "parts", "0").(map[string]any)
	if part["thoughtSignature"] != sig {
		t.Errorf("the signature must come back with its function call: %v", part)
	}
	if sub(t, s.bodies[1], "contents", "2", "parts", "0", "functionResponse", "name") != "core__alpha" {
		t.Errorf("function response name: %v", s.bodies[1]["contents"])
	}
}

func TestGeminiToolResultWithoutAToolUseIsAnError(t *testing.T) {
	_, _, err := buildGeminiParams("m", Request{Messages: []Message{
		UserText("hi"), {Role: RoleUser, Content: []Block{ToolResult("nope", "x", false)}},
	}})
	if err == nil {
		t.Fatal("want an error")
	}
}

func geminiResp(finish genai.FinishReason, parts ...*genai.Part) *genai.GenerateContentResponse {
	return &genai.GenerateContentResponse{Candidates: []*genai.Candidate{{
		Content: &genai.Content{Role: "model", Parts: parts}, FinishReason: finish,
	}}}
}

func TestGeminiStopReasons(t *testing.T) {
	next := func() string { return "call_x" }
	text := &genai.Part{Text: "hi"}
	call := &genai.Part{FunctionCall: &genai.FunctionCall{Name: "t"}}
	cases := []struct {
		name     string
		resp     *genai.GenerateContentResponse
		stop     StopReason
		category string
	}{
		{"stop", geminiResp(genai.FinishReasonStop, text), StopEndTurn, ""},
		{"max tokens", geminiResp(genai.FinishReasonMaxTokens, text), StopMaxTokens, ""},
		{"function call", geminiResp(genai.FinishReasonStop, call), StopToolUse, ""},
		{"function call, other finish", geminiResp(genai.FinishReasonOther, call), StopToolUse, ""},
		{"safety", geminiResp(genai.FinishReasonSafety), StopRefusal, "SAFETY"},
		{"recitation", geminiResp(genai.FinishReasonRecitation), StopRefusal, "RECITATION"},
		{"blocklist", geminiResp(genai.FinishReasonBlocklist), StopRefusal, "BLOCKLIST"},
		{"prohibited", geminiResp(genai.FinishReasonProhibitedContent), StopRefusal, "PROHIBITED_CONTENT"},
		{"spii", geminiResp(genai.FinishReasonSPII), StopRefusal, "SPII"},
		{"malformed call", geminiResp(genai.FinishReasonMalformedFunctionCall), StopOther, ""},
		{"language", geminiResp(genai.FinishReasonLanguage), StopOther, ""},
		{"unspecified", geminiResp(""), StopOther, ""},
		{"prompt blocked", &genai.GenerateContentResponse{PromptFeedback: &genai.GenerateContentResponsePromptFeedback{BlockReason: genai.BlockedReasonSafety}}, StopRefusal, "SAFETY"},
	}
	for _, c := range cases {
		r, err := fromGeminiResponse(c.resp, "m", next)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if r.StopReason != c.stop || r.RefusalCategory != c.category {
			t.Errorf("%s: stop %q category %q, want %q %q", c.name, r.StopReason, r.RefusalCategory, c.stop, c.category)
		}
	}
	if _, err := fromGeminiResponse(&genai.GenerateContentResponse{}, "m", next); err == nil {
		t.Error("no candidate and no block reason must be an error")
	}
}

func TestGeminiAPIErrorBecomesAPIError(t *testing.T) {
	for _, code := range []int{401, 403, 400} {
		s := newGeminiServer(t, func(int) (int, string) {
			return code, `{"error":{"code":` + itoa(code) + `,"message":"API key not valid.\nsecond line","status":"PERMISSION_DENIED"}}`
		})
		g := newTestGemini(t, s, "gemini-2.5-flash", keyFile(t, "k"))
		_, err := g.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}})
		ae, ok := err.(*APIError)
		if !ok {
			t.Fatalf("%d: %T %v", code, err, err)
		}
		if ae.Status != code || ae.Type != "PERMISSION_DENIED" || ae.Message != "API key not valid." {
			t.Errorf("%d: %+v", code, ae)
		}
		if IsAuth(err) != (code == 401 || code == 403) {
			t.Errorf("%d: IsAuth = %v", code, IsAuth(err))
		}
		if len(s.bodies) != 1 {
			t.Errorf("%d: a client error must not be retried, %d requests", code, len(s.bodies))
		}
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func TestGeminiRetriesAServerError(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the SDK's backoff")
	}
	s := newGeminiServer(t, func(n int) (int, string) {
		if n == 1 {
			return 503, `{"error":{"code":503,"message":"overloaded","status":"UNAVAILABLE"}}`
		}
		return 200, okReply
	})
	g := newTestGemini(t, s, "gemini-2.5-flash", keyFile(t, "k"))
	if _, err := g.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}}); err != nil {
		t.Fatal(err)
	}
	if len(s.bodies) != 2 {
		t.Errorf("requests = %d, want 2 (the SDK's own retry)", len(s.bodies))
	}
}

func TestGeminiKeyIsReadOnEveryRequest(t *testing.T) {
	s := newGeminiServer(t, func(int) (int, string) { return 200, okReply })
	kf := keyFile(t, "first-key")
	g := newTestGemini(t, s, "gemini-2.5-flash", kf)
	req := Request{Messages: []Message{UserText("hi")}}
	if _, err := g.Complete(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(kf, []byte("  rotated-key \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Complete(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if a, b := s.headers[0].Get("x-goog-api-key"), s.headers[1].Get("x-goog-api-key"); a != "first-key" || b != "rotated-key" {
		t.Errorf("keys sent: %q then %q", a, b)
	}
	if err := os.WriteFile(kf, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Complete(context.Background(), req); err == nil || len(s.bodies) != 2 {
		t.Errorf("an emptied key file must fail the request before it is sent: %v (%d sent)", err, len(s.bodies))
	}
}

func TestGeminiIgnoresTheEnvironment(t *testing.T) {
	t.Setenv("GOOGLE_API_KEY", "env-key")
	t.Setenv("GEMINI_API_KEY", "env-key-2")
	t.Setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")
	s := newGeminiServer(t, func(int) (int, string) { return 200, okReply })
	g := newTestGemini(t, s, "gemini-2.5-flash", keyFile(t, "file-key"))
	if _, err := g.Complete(context.Background(), Request{Messages: []Message{UserText("hi")}}); err != nil {
		t.Fatal(err)
	}
	if got := s.headers[0].Get("x-goog-api-key"); got != "file-key" {
		t.Errorf("key = %q", got)
	}

	// An empty key file does not fall back to the environment either.
	_, err := NewGemini(context.Background(), GeminiOptions{Auth: AuthKey, Model: "m", KeyFile: keyFile(t, ""), BaseURL: s.URL})
	if err == nil {
		t.Error("an empty key file must fail at construction")
	}
	if _, err := NewGemini(context.Background(), GeminiOptions{Auth: AuthKey, Model: "m"}); err == nil {
		t.Error("no key file must fail at construction")
	}
}

func TestNewGeminiRejectsWhatItCannotUse(t *testing.T) {
	ctx := context.Background()
	if _, err := NewGemini(ctx, GeminiOptions{Auth: AuthFederation, Model: "m"}); err == nil || !strings.Contains(err.Error(), "Anthropic") {
		t.Errorf("federation: %v", err)
	}
	if _, err := NewGemini(ctx, GeminiOptions{Auth: AuthVertex, Model: "m"}); err == nil {
		t.Error("vertex needs a project")
	}
	if _, err := NewGemini(ctx, GeminiOptions{Auth: "oauth", Model: "m"}); err == nil {
		t.Error("unknown auth")
	}
	if _, err := NewGemini(ctx, GeminiOptions{Auth: AuthKey, KeyFile: keyFile(t, "k")}); err == nil {
		t.Error("no model")
	}
}

func TestVertexHost(t *testing.T) {
	for loc, want := range map[string]string{
		"global":       "https://aiplatform.googleapis.com/",
		"us":           "https://aiplatform.us.rep.googleapis.com/",
		"europe-west1": "https://europe-west1-aiplatform.googleapis.com/",
	} {
		if got := vertexHost(loc); got != want {
			t.Errorf("%s: %s, want %s", loc, got, want)
		}
	}
}

func TestGeneratedID(t *testing.T) {
	for id, want := range map[string]bool{"call_1": true, "call_42": true, "call_": false, "call_x": false, "id-1": false, "": false} {
		if generatedID(id) != want {
			t.Errorf("generatedID(%q) = %v", id, !want)
		}
	}
}
