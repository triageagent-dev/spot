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
