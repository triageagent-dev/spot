package config

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/triageagent-dev/spot/internal/injected"
)

var required = []string{"--tenant", "example", "--core-mcp-url", "https://core.invalid/mcp",
	"--core-token-file", "/s/token", "--anthropic-key-file", "/s/key"}

func TestDefaults(t *testing.T) {
	c, err := Parse(required, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "example-spot" || c.HeartbeatID != "system2-heartbeat-example" {
		t.Errorf("derived names: %q %q", c.Name, c.HeartbeatID)
	}
	if c.StateDir != "/state" || c.MinWake != 2*time.Minute || c.MaxWake != 2*time.Hour {
		t.Errorf("defaults: %+v", c)
	}
	if c.Model != "claude-opus-5-5" || c.Effort != "medium" || c.MaxSteps != 40 || c.MaxToolCalls != 20 || c.MaxTurnTokens != 150000 || !c.RecallFirst || !c.PostGate || !c.FenceResults || !c.IssueWakes || !c.FastLane || c.FastMaxToolCalls != 8 || c.ToolTimeout != time.Minute || c.ToolResultMax != 12288 || !c.SkipQuietWakes || c.HourlyTokenCeil != 600000 || c.TurnTimeout != 20*time.Minute {
		t.Errorf("model and loop defaults: %+v", c)
	}
	if c.LLMAuth != AuthKey || c.VertexRegion != "global" || c.LLMProvider != ProviderAnthropic {
		t.Errorf("auth defaults: %+v", c)
	}
	// An empty model is the provider's default.
	if c, err = Parse(with(required, "--model", ""), io.Discard); err != nil || c.Model != DefaultAnthropicModel {
		t.Errorf("empty --model: %q %v", c.Model, err)
	}
	if c, err = Parse(with(required, "--model", "claude-example"), io.Discard); err != nil || c.Model != "claude-example" {
		t.Errorf("explicit --model: %q %v", c.Model, err)
	}
}

var geminiKey = []string{"--tenant", "example", "--core-mcp-url", "https://core.invalid/mcp",
	"--core-token-file", "/s/token", "--llm-provider", "gemini", "--gemini-key-file", "/s/gemini"}

func TestGeminiProvider(t *testing.T) {
	c, err := Parse(geminiKey, io.Discard)
	if err != nil || c.LLMProvider != ProviderGemini || c.LLMAuth != AuthKey || c.GeminiKeyFile != "/s/gemini" || c.Model != DefaultGeminiModel {
		t.Fatalf("key mode: %+v %v", c, err)
	}
	if c, err = Parse(with(geminiKey, "--model", "gemini-3-pro-preview"), io.Discard); err != nil || c.Model != "gemini-3-pro-preview" {
		t.Fatalf("explicit model: %q %v", c.Model, err)
	}
	vertex := []string{"--tenant", "example", "--core-mcp-url", "https://core.invalid/mcp", "--core-token-file", "/s/token",
		"--llm-provider", "gemini", "--llm-auth", "vertex", "--vertex-project", "example-project", "--vertex-region", "europe-west1"}
	if c, err = Parse(vertex, io.Discard); err != nil || c.LLMAuth != AuthVertex || c.VertexProject != "example-project" || c.VertexRegion != "europe-west1" || c.GeminiKeyFile != "" || c.Model != DefaultGeminiModel {
		t.Fatalf("vertex mode: %+v %v", c, err)
	}
	base := []string{"--tenant", "example", "--core-mcp-url", "https://core.invalid/mcp", "--core-token-file", "/s/token"}
	for name, args := range map[string][]string{
		"unknown provider":         with(base, "--llm-provider", "openai", "--anthropic-key-file", "/s/key"),
		"federation":               with(base, "--llm-provider", "gemini", "--llm-auth", "federation"),
		"federation with ids":      with(federation, "--llm-provider", "gemini"),
		"key mode, no key file":    with(base, "--llm-provider", "gemini"),
		"an anthropic key file":    with(geminiKey, "--anthropic-key-file", "/s/key"),
		"project in key mode":      with(geminiKey, "--vertex-project", "p"),
		"vertex, no project":       with(base, "--llm-provider", "gemini", "--llm-auth", "vertex"),
		"vertex with a key file":   with(vertex, "--gemini-key-file", "/s/gemini"),
		"vertex, anthropic file":   with(vertex, "--anthropic-key-file", "/s/key"),
		"gemini key for anthropic": with(required, "--gemini-key-file", "/s/gemini"),
		"unknown auth":             with(base, "--llm-provider", "gemini", "--llm-auth", "oauth"),
	} {
		if _, err := Parse(args, io.Discard); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if _, err := Parse(with(base, "--llm-provider", "gemini", "--llm-auth", "federation"), io.Discard); err == nil || !strings.Contains(err.Error(), "Anthropic auth mode") {
		t.Errorf("federation message: %v", err)
	}
}

func TestExplicitName(t *testing.T) {
	c, err := Parse(append(append([]string{}, required...), "--name", "custom", "--min-wake", "5m", "--max-wake", "1h"), io.Discard)
	if err != nil || c.Name != "custom" || c.MinWake != 5*time.Minute {
		t.Fatalf("%+v %v", c, err)
	}
}

func TestValidation(t *testing.T) {
	bad := map[string][]string{
		"no args":         nil,
		"max < min":       append(append([]string{}, required...), "--min-wake", "1h", "--max-wake", "1m"),
		"bad effort":      append(append([]string{}, required...), "--effort", "extreme"),
		"zero tool time":  append(append([]string{}, required...), "--tool-timeout", "0s"),
		"zero result":     append(append([]string{}, required...), "--tool-result-max-bytes", "0"),
		"bad level":       append(append([]string{}, required...), "--log-level", "loud"),
		"zero steps":      append(append([]string{}, required...), "--max-steps", "0"),
		"zero tool calls": append(append([]string{}, required...), "--max-tool-calls", "0"),
		"old flag":        append(append([]string{}, required...), "--max-turns", "5"),
		"cli flag":        append(append([]string{}, required...), "--permission-mode", "auto"),
		"negative ceil":   append(append([]string{}, required...), "--daily-token-ceiling", "-1"),
		"tenant slash":    {"--tenant", "a/b", "--core-mcp-url", "u", "--core-token-file", "t", "--anthropic-key-file", "k"},
		"stray argument":  append(append([]string{}, required...), "extra"),
	}
	for name, args := range bad {
		if _, err := Parse(args, io.Discard); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	_, err := Parse(nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--tenant is required") {
		t.Errorf("error should name the flag: %v", err)
	}
}

func TestNoCredentialFlags(t *testing.T) {
	// Credentials arrive as files only. A flag that takes a key or token value
	// would put it in argv.
	for _, f := range []string{"--anthropic-key", "--core-token", "--api-key", "--token"} {
		if _, err := Parse(append(append([]string{}, required...), f, "x"), io.Discard); err == nil {
			t.Errorf("%s must not be accepted", f)
		}
	}
}

var federation = []string{"--tenant", "example", "--core-mcp-url", "https://core.invalid/mcp",
	"--core-token-file", "/s/token", "--llm-auth", "federation",
	"--anthropic-identity-token-file", "/var/run/secrets/anthropic.com/token",
	"--anthropic-federation-rule-id", "fdrl_example", "--anthropic-org-id", "00000000-0000-0000-0000-000000000000",
	"--anthropic-service-account-id", "svac_example"}

func with(base []string, extra ...string) []string {
	return append(append([]string{}, base...), extra...)
}

func TestAuthDefaultsToKey(t *testing.T) {
	c, err := Parse(required, io.Discard)
	if err != nil || c.LLMAuth != AuthKey {
		t.Fatalf("%+v %v", c, err)
	}
}

func TestFederationModeAccepted(t *testing.T) {
	c, err := Parse(federation, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.LLMAuth != AuthFederation || c.AnthropicKeyFile != "" || c.FederationRuleID != "fdrl_example" ||
		c.AnthropicServiceAcctID != "svac_example" || c.IdentityTokenFile == "" || c.AnthropicWorkspaceID != "" {
		t.Fatalf("%+v", c)
	}
	c, err = Parse(with(federation, "--anthropic-workspace-id", "wrkspc_example"), io.Discard)
	if err != nil || c.AnthropicWorkspaceID != "wrkspc_example" {
		t.Fatalf("%+v %v", c, err)
	}
}

func TestFederationValidation(t *testing.T) {
	bad := map[string][]string{
		"key file with federation":    with(federation, "--anthropic-key-file", "/s/key"),
		"unknown mode":                with(required, "--llm-auth", "oauth"),
		"id with a space":             with(federation, "--anthropic-workspace-id", "a b"),
		"federation flag in key mode": with(required, "--anthropic-federation-rule-id", "fdrl_x"),
		"token file in key mode":      with(required, "--anthropic-identity-token-file", "/t"),
	}
	for name, args := range bad {
		if _, err := Parse(args, io.Discard); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	// Each required federation flag is required.
	for _, f := range []string{"--anthropic-identity-token-file", "--anthropic-federation-rule-id", "--anthropic-org-id", "--anthropic-service-account-id"} {
		var args []string
		for i := 0; i < len(federation); i++ {
			if federation[i] == f {
				i++
				continue
			}
			args = append(args, federation[i])
		}
		_, err := Parse(args, io.Discard)
		if err == nil || !strings.Contains(err.Error(), f+" is required") {
			t.Errorf("without %s: %v", f, err)
		}
	}
	// Key mode no longer needs federation flags, and still needs the key file.
	if _, err := Parse(federation[:6], io.Discard); err == nil {
		t.Error("tenant/core flags alone are not a complete config")
	}
	noKey := []string{"--tenant", "example", "--core-mcp-url", "u", "--core-token-file", "t"}
	if _, err := Parse(noKey, io.Discard); err == nil || !strings.Contains(err.Error(), "--anthropic-key-file is required") {
		t.Errorf("key mode without a key file: %v", err)
	}
}

func TestNoFederationSecretFlags(t *testing.T) {
	// The identity token is a file path; no flag takes the JWT itself.
	if _, err := Parse(with(federation, "--anthropic-identity-token", "eyJ..."), io.Discard); err == nil {
		t.Error("a flag carrying the JWT must not exist")
	}
}

func TestTelemetryAndBudgetFlags(t *testing.T) {
	c, err := Parse(append(append([]string{}, required...),
		"--otel-endpoint", "http://ego.example.svc:4318", "--otel-token-file", "/secrets/otel/token", "--otel-content=false"), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.OtelEndpoint != "http://ego.example.svc:4318" || c.OtelTokenFile != "/secrets/otel/token" || c.OtelContent {
		t.Fatalf("%+v", c)
	}
	for name, extra := range map[string][]string{
		"token file without an endpoint": {"--otel-token-file", "/t"},
		"endpoint without a scheme":      {"--otel-endpoint", "ego:4318"},
	} {
		if _, err := Parse(append(append([]string{}, required...), extra...), io.Discard); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if c, _ := Parse(required, io.Discard); c.OtelEndpoint != "" {
		t.Fatalf("telemetry and the new limits must default to off: %+v", c)
	}
}

func TestEachSpotIsItsOwnTenantInEgo(t *testing.T) {
	c, err := Parse(required, io.Discard)
	if err != nil || c.OtelTenant != c.Name || c.OtelTenantName != c.Name || c.OtelTenantAttr != "triage.tenant" {
		t.Fatalf("defaults: label %q name %q attr %q, %v", c.OtelTenant, c.OtelTenantName, c.OtelTenantAttr, err)
	}
	with := func(extra ...string) (Config, error) {
		return Parse(append(append(append([]string{}, required...), "--otel-endpoint", "http://ego.example.svc:4318"), extra...), io.Discard)
	}
	c, err = with("--otel-tenant", "example", "--otel-tenant-name", "Example workspace")
	if err != nil || c.OtelTenant != "example" || c.OtelTenantName != "Example workspace" {
		t.Fatalf("override = %q %q, %v", c.OtelTenant, c.OtelTenantName, err)
	}
	for _, bad := range [][]string{
		{"--otel-tenant", "a b"}, {"--otel-tenant", "a,b"}, {"--otel-tenant", "a|b"},
		{"--otel-tenant-name", strings.Repeat("x", 81)}, {"--otel-tenant-name", " padded"}, {"--otel-tenant-name", "a,b"},
	} {
		if _, err := with(bad...); err == nil {
			t.Errorf("%v must be refused: ego would drop it silently", bad)
		}
	}
}

func TestWakeFlags(t *testing.T) {
	c, err := Parse(required, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.ChatBatch != 10*time.Minute || c.MaxEdgeWakesHr != 12 {
		t.Fatalf("defaults: %+v", c)
	}
	c, err = Parse(append(append([]string{}, required...), "--chat-batch", "0", "--max-edge-wakes-per-hour", "0"), io.Discard)
	if err != nil || c.ChatBatch != 0 || c.MaxEdgeWakesHr != 0 {
		t.Fatalf("%v %+v", err, c)
	}
	if _, err := Parse(append(append([]string{}, required...), "--max-edge-wakes-per-hour", "-1"), io.Discard); err == nil {
		t.Fatal("a negative cap must be refused")
	}
}

func TestEgoMCPNeedsTheSenderToken(t *testing.T) {
	ok := append(append([]string{}, required...), "--otel-endpoint", "http://ego.example.svc:4318", "--otel-token-file", "/t", "--ego-mcp-url", "http://ego.example.svc:8001/mcp")
	if c, err := Parse(ok, io.Discard); err != nil || c.EgoMCPURL != "http://ego.example.svc:8001/mcp" {
		t.Fatalf("%v %+v", err, c)
	}
	for name, extra := range map[string][]string{
		"no token":  {"--otel-endpoint", "http://ego.example.svc:4318", "--ego-mcp-url", "http://ego.example.svc:8001/mcp"},
		"no scheme": {"--otel-endpoint", "http://ego.example.svc:4318", "--otel-token-file", "/t", "--ego-mcp-url", "ego:8001"},
	} {
		if _, err := Parse(append(append([]string{}, required...), extra...), io.Discard); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestCoreToolsIsCuratedOrAll(t *testing.T) {
	if c, err := Parse(required, io.Discard); err != nil || c.CoreTools != "curated" {
		t.Fatalf("default %q %v", c.CoreTools, err)
	}
	if c, err := Parse(append(append([]string{}, required...), "--core-tools", "all"), io.Discard); err != nil || c.CoreTools != "all" {
		t.Fatalf("all: %q %v", c.CoreTools, err)
	}
	if _, err := Parse(append(append([]string{}, required...), "--core-tools", "most"), io.Discard); err == nil {
		t.Fatal("an unknown value must fail")
	}
}

func TestVertexMode(t *testing.T) {
	vertex := []string{"--tenant", "example", "--core-mcp-url", "https://core.invalid/mcp", "--core-token-file", "/s/token",
		"--llm-auth", "vertex", "--vertex-project", "example-project"}
	c, err := Parse(vertex, io.Discard)
	if err != nil || c.LLMAuth != AuthVertex || c.VertexProject != "example-project" || c.VertexRegion != "global" || c.AnthropicKeyFile != "" {
		t.Fatalf("%+v %v", c, err)
	}
	if c, err = Parse(with(vertex, "--vertex-region", "europe-west1"), io.Discard); err != nil || c.VertexRegion != "europe-west1" {
		t.Fatalf("%+v %v", c, err)
	}
	for name, args := range map[string][]string{
		"no project":                 {"--tenant", "example", "--core-mcp-url", "u", "--core-token-file", "t", "--llm-auth", "vertex"},
		"a key file":                 with(vertex, "--anthropic-key-file", "/s/key"),
		"federation flag":            with(vertex, "--anthropic-org-id", "org"),
		"project in key mode":        with(required, "--vertex-project", "p"),
		"project in federation mode": with(federation, "--vertex-project", "p"),
	} {
		if _, err := Parse(args, io.Discard); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestNoCLIFlagsRemain(t *testing.T) {
	for _, f := range []string{"--claude-bin", "--sandbox", "--persistent", "--run-dir", "--allowed-tools", "--disallowed-tools",
		"--fallback-permission-mode", "--daily-cost-ceiling-usd", "--anthropic-auth"} {
		if _, err := Parse(with(required, f, "x"), io.Discard); err == nil {
			t.Errorf("%s must no longer exist", f)
		}
	}
}

func TestTheTemporalEngineIsGoneAndTheErrorSaysWhy(t *testing.T) {
	for _, f := range [][]string{{"--engine", "temporal"}, {"--engine=local"}, {"--temporal-address", "h:7233"}, {"--temporal-local-payloads"}} {
		_, err := Parse(with(required, f...), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "ADR-106") {
			t.Errorf("%v: %v", f, err)
		}
	}
}

func TestMaxTasksIsBetweenZeroAndTen(t *testing.T) {
	for _, n := range []string{"11", "-1"} {
		if _, err := Parse(with(required, "--max-tasks", n), io.Discard); err == nil {
			t.Errorf("--max-tasks %s was accepted", n)
		}
	}
	for _, n := range []string{"0", "3", "10"} {
		if _, err := Parse(with(required, "--max-tasks", n), io.Discard); err != nil {
			t.Errorf("--max-tasks %s: %v", n, err)
		}
	}
}

var certArgs = []string{"--tenant", "example", "--core-mcp-url", "https://core.invalid/mcp",
	"--core-api-url", "http://core.invalid:8080", "--anthropic-key-file", "/s/key",
	"--core-ca-fingerprint", "SHA256:" + strings.Repeat("ab", 32)}

func TestCertificateIdentityNeedsNoTokenFile(t *testing.T) {
	c, err := Parse(certArgs, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !c.CertificateIdentity() || c.IdentityDir != "/state/identity" || c.CoreTokenFile != "/state/identity/token" {
		t.Fatalf("identity: %+v", c)
	}
	c, err = Parse(with(certArgs, "--state-dir", "/data", "--identity-dir", "/id"), io.Discard)
	if err != nil || c.IdentityDir != "/id" || c.CoreTokenFile != "/id/token" {
		t.Fatalf("identity dir: %+v %v", c, err)
	}
}

func TestCertificateIdentityRefusesWhatDoesNotFit(t *testing.T) {
	for name, args := range map[string][]string{
		"a bad fingerprint":    with(certArgs, "--core-ca-fingerprint", "SHA256:nothex"),
		"a token file with it": with(certArgs, "--core-token-file", "/s/token"),
		"no core API URL":      with(certArgs, "--core-api-url", ""),
		"a name a cert can't":  with(certArgs, "--name", "Example_Spot"),
		"a name over 40 chars": with(certArgs, "--name", strings.Repeat("a", 41)),
	} {
		if _, err := Parse(args, io.Discard); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestEnrollKeyFileNeedsTheCertificateIdentity(t *testing.T) {
	if c, err := Parse(with(certArgs, "--enroll-key-file", "/s/enroll/key"), io.Discard); err != nil || c.EnrollKeyFile != "/s/enroll/key" {
		t.Fatalf("%+v %v", c, err)
	}
	if _, err := Parse(with(required, "--enroll-key-file", "/s/enroll/key"), io.Discard); err == nil {
		t.Fatal("an enrollment key was accepted without the certificate identity")
	}
}

var substrateArgs = []string{"--tenant", "example", "--core-mcp-url", "http://core.invalid/mcp",
	"--core-api-url", "http://core.invalid:8080", "--core-ca-fingerprint", "SHA256:" + strings.Repeat("ab", 32),
	"--runtime", "substrate", "--llm-auth", "injected"}

func TestSubstrateRuntimeHoldsNoCredentialAndWaitsForNothing(t *testing.T) {
	c, err := Parse(with(substrateArgs, "--edge-cooldown", "5m", "--chat-batch", "10m"), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Substrate() || c.Listen != ":80" || c.LLMAuth != AuthInjected || c.LLMBaseURL != "" {
		t.Fatalf("%+v", c)
	}
	if c.EdgeCooldown != 0 || c.ChatBatch != 0 {
		t.Errorf("an actor is suspended when idle, so nothing waits: cooldown %s batch %s", c.EdgeCooldown, c.ChatBatch)
	}
	if c.CoreTokenFile != "/state/identity/token" {
		t.Errorf("the token is the identity's own: %q", c.CoreTokenFile)
	}
}

func TestSubstrateRuntimeRefusesWhatDoesNotFit(t *testing.T) {
	for name, args := range map[string][]string{
		"a model key":            with(substrateArgs, "--anthropic-key-file", "/s/key"),
		"key auth":               with(substrateArgs, "--llm-auth", "key", "--llm-base-url", ""),
		"a base URL that is not": with(substrateArgs, "--llm-base-url", "proxy.invalid"),
		"no core API URL":        with(substrateArgs, "--core-api-url", ""),
		"no certificate":         with(substrateArgs, "--core-ca-fingerprint", ""),
		"no listen address":      with(substrateArgs, "--listen", ""),
		"an unknown runtime":     with(substrateArgs, "--runtime", "lambda"),
		"gemini with injection":  with(substrateArgs, "--llm-provider", "gemini"),
	} {
		if _, err := Parse(args, io.Discard); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestATaskActorDoesNotJoinAndKeepsItsTokenOutsideTheState(t *testing.T) {
	task := with(substrateArgs[:6], "--runtime", "substrate", "--llm-auth", "injected", "--task-verifier-file", "/run/task/verifier")
	c, err := Parse(task, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !c.TaskActor() || c.CoreCAFingerprint != "" || c.CoreTokenFile == "" || strings.HasPrefix(c.CoreTokenFile, c.StateDir) {
		t.Fatalf("%+v", c)
	}
	// With the persona's flags too (a template may be shared), it still does not join.
	c, err = Parse(with(substrateArgs, "--task-verifier-file", "/run/task/verifier"), io.Discard)
	if err != nil || !c.TaskActor() || strings.HasSuffix(c.CoreTokenFile, "identity/token") {
		t.Fatalf("%+v %v", c, err)
	}
	for name, args := range map[string][]string{
		"a deployment":    with(required, "--task-verifier-file", "/run/task/verifier"),
		"no core API URL": with(task, "--core-api-url", ""),
	} {
		if _, err := Parse(args, io.Discard); err == nil {
			t.Errorf("a task actor with %s was accepted", name)
		}
	}
}

func TestInjectionIsAnAuthModeNotARuntime(t *testing.T) {
	c, err := Parse(with(required[:6], "--llm-auth", "injected", "--llm-base-url", "http://proxy.invalid"), io.Discard)
	if err != nil || c.Runtime != RuntimeDeployment || c.LLMAuth != AuthInjected {
		t.Fatalf("a deployment may use injection too: %+v %v", c, err)
	}
	if _, err := Parse(with(required, "--llm-base-url", "http://proxy.invalid"), io.Discard); err == nil {
		t.Error("--llm-base-url was accepted without --llm-auth=injected")
	}
	if _, err := Parse(with(required[:6], "--llm-auth", "injected"), io.Discard); err == nil {
		t.Error("injected auth with nothing to replace the placeholder was accepted in a deployment")
	}
}

func TestConnectorsCanBeGivenInlineForARuntimeWithNoFile(t *testing.T) {
	js := `[{"name":"example-kube","url":"http://example.invalid/mcp","tools":["get_pods"],"hint":"read only"}]`
	c, err := Parse(with(substrateArgs, "--connectors-json", js), io.Discard)
	if err != nil || len(c.Connectors) != 1 || c.Connectors[0].Name != "example-kube" {
		t.Fatalf("%+v %v", c.Connectors, err)
	}
	if _, err := Parse(with(substrateArgs, "--connectors-json", js, "--connectors-file", "/x"), io.Discard); err == nil {
		t.Error("both forms were accepted")
	}
	if _, err := Parse(with(substrateArgs, "--connectors-json", `[{"name":"Bad Name","url":"x","tools":[]}]`), io.Discard); err == nil {
		t.Error("an invalid inline connector was accepted")
	}
}

func TestAnActorReachesEgoOnlyWithAnInjectedToken(t *testing.T) {
	// No token file, but the egress replaces a bearer only if the spot sends one.
	ok := with(substrateArgs, "--otel-endpoint", "http://ego.invalid:4318", "--ego-mcp-url", "http://ego.invalid:8001/mcp", "--otel-token-injected")
	if _, err := Parse(ok, io.Discard); err != nil {
		t.Fatalf("the egress policy puts ego's token on the request: %v", err)
	}
	bare := with(substrateArgs, "--ego-mcp-url", "http://ego.invalid/mcp")
	if _, err := Parse(bare, io.Discard); err == nil {
		t.Error("ego tools with no bearer to replace were accepted: the request would go out with no token")
	}
	dep := with(required, "--ego-mcp-url", "http://ego.invalid/mcp")
	if _, err := Parse(dep, io.Discard); err == nil {
		t.Error("a deployment must still name the token file")
	}
}

func TestAnInjectedEnrollmentKeyIsForTheSubstrateRuntimeAndNotWithAKeyFile(t *testing.T) {
	c, err := Parse(with(substrateArgs, "--enroll-key-injected"), io.Discard)
	if err != nil || !c.EnrollKeyInjected {
		t.Fatalf("%+v %v", c.EnrollKeyInjected, err)
	}
	if _, err := Parse(with(substrateArgs, "--enroll-key-injected", "--enroll-key-file", "/k"), io.Discard); err == nil {
		t.Error("both ways to give the key were accepted")
	}
	if _, err := Parse(with(certArgs, "--enroll-key-injected"), io.Discard); err == nil {
		t.Error("a deployment cannot have its key injected")
	}
}

func TestAnInjectedTelemetryTokenIsForTheSubstrateRuntimeAndNeedsNoFile(t *testing.T) {
	args := with(substrateArgs, "--otel-endpoint", "http://ego.invalid:4318", "--ego-mcp-url", "http://ego.invalid:8001/mcp", "--otel-token-injected")
	c, err := Parse(args, io.Discard)
	if err != nil || !c.OtelTokenInjected || c.OtelTokenFile != injected.File {
		t.Fatalf("%+v %v", c.OtelTokenFile, err)
	}
	if _, err := Parse(with(args, "--otel-token-file", "/t"), io.Discard); err == nil {
		t.Error("both ways to give the token were accepted")
	}
	dep := with(required, "--otel-endpoint", "http://ego.invalid:4318", "--otel-token-injected")
	if _, err := Parse(dep, io.Discard); err == nil {
		t.Error("a deployment cannot have its token injected")
	}
}
