package config

import (
	"io"
	"strings"
	"testing"
	"time"
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
	if c.StateDir != "/state" || c.RunDir != "/run/tria-spot" || c.MinWake != 2*time.Minute || c.MaxWake != 2*time.Hour {
		t.Errorf("defaults: %+v", c)
	}
	if c.PermissionMode != "auto" || c.FallbackPermMode != "dontAsk" {
		t.Errorf("permission defaults: %+v", c)
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
		"no args":        nil,
		"max < min":      append(append([]string{}, required...), "--min-wake", "1h", "--max-wake", "1m"),
		"bad mode":       append(append([]string{}, required...), "--permission-mode", "bypassPermissions"),
		"bad fallback":   append(append([]string{}, required...), "--fallback-permission-mode", "auto"),
		"bad level":      append(append([]string{}, required...), "--log-level", "loud"),
		"zero turns":     append(append([]string{}, required...), "--max-turns", "0"),
		"negative ceil":  append(append([]string{}, required...), "--daily-token-ceiling", "-1"),
		"tenant slash":   {"--tenant", "a/b", "--core-mcp-url", "u", "--core-token-file", "t", "--anthropic-key-file", "k"},
		"stray argument": append(append([]string{}, required...), "extra"),
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
	"--core-token-file", "/s/token", "--anthropic-auth", "federation",
	"--anthropic-identity-token-file", "/var/run/secrets/anthropic.com/token",
	"--anthropic-federation-rule-id", "fdrl_example", "--anthropic-org-id", "00000000-0000-0000-0000-000000000000",
	"--anthropic-service-account-id", "svac_example"}

func with(base []string, extra ...string) []string {
	return append(append([]string{}, base...), extra...)
}

func TestAuthDefaultsToKey(t *testing.T) {
	c, err := Parse(required, io.Discard)
	if err != nil || c.AnthropicAuth != AuthKey {
		t.Fatalf("%+v %v", c, err)
	}
}

func TestFederationModeAccepted(t *testing.T) {
	c, err := Parse(federation, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.AnthropicAuth != AuthFederation || c.AnthropicKeyFile != "" || c.FederationRuleID != "fdrl_example" ||
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
		"unknown mode":                with(required, "--anthropic-auth", "oauth"),
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
		"--otel-endpoint", "http://ego.example.svc:4318", "--otel-token-file", "/secrets/otel/token", "--otel-tool-details",
		"--daily-cost-ceiling-usd", "12.5", "--max-context-tokens", "70000"), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.OtelEndpoint != "http://ego.example.svc:4318" || c.OtelTokenFile != "/secrets/otel/token" || !c.OtelToolDetails ||
		c.DailyCostCeilUSD != 12.5 || c.MaxContextTokens != 70000 {
		t.Fatalf("%+v", c)
	}
	for name, extra := range map[string][]string{
		"token file without an endpoint":   {"--otel-token-file", "/t"},
		"tool details without an endpoint": {"--otel-tool-details"},
		"endpoint without a scheme":        {"--otel-endpoint", "ego:4318"},
		"negative cost ceiling":            {"--daily-cost-ceiling-usd", "-1"},
		"negative context limit":           {"--max-context-tokens", "-5"},
	} {
		if _, err := Parse(append(append([]string{}, required...), extra...), io.Discard); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if c, _ := Parse(required, io.Discard); c.OtelEndpoint != "" || c.DailyCostCeilUSD != 0 || c.MaxContextTokens != 0 {
		t.Fatalf("telemetry and the new limits must default to off: %+v", c)
	}
}

func TestOtelTenantDefaultsToOneTenantForEverySpot(t *testing.T) {
	c, err := Parse(required, io.Discard)
	if err != nil || c.OtelTenant != "tria-spot" {
		t.Fatalf("default ego tenant = %q, %v", c.OtelTenant, err)
	}
	c, err = Parse(append(append([]string{}, required...), "--otel-tenant", "example"), io.Discard)
	if err != nil || c.OtelTenant != "example" {
		t.Fatalf("override = %q, %v", c.OtelTenant, err)
	}
}

func TestWakeFlags(t *testing.T) {
	c, err := Parse(required, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.ChatBatch != 10*time.Minute || c.MaxEdgeWakesHr != 12 || c.ExtraDisallowed != "" {
		t.Fatalf("defaults: %+v", c)
	}
	c, err = Parse(append(append([]string{}, required...), "--chat-batch", "0", "--max-edge-wakes-per-hour", "0", "--disallowed-tools", "Bash(curl *)"), io.Discard)
	if err != nil || c.ChatBatch != 0 || c.MaxEdgeWakesHr != 0 || c.ExtraDisallowed != "Bash(curl *)" {
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
