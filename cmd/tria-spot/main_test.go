package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/triageagent-dev/tria-spot/internal/config"
)

func TestVersionAndUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"version"}, strings.NewReader(""), &out, &errOut); code != 0 || !strings.HasPrefix(out.String(), "tria-spot ") {
		t.Fatalf("version: %d %q", code, out.String())
	}
	out.Reset()
	if code := run(nil, strings.NewReader(""), &out, &errOut); code != 2 || !strings.Contains(errOut.String(), "--tenant is required") {
		t.Fatalf("missing flags: %d %q", code, errOut.String())
	}
}

func TestCoreServerDefaultsToTheCuratedListsAndAllOptsOut(t *testing.T) {
	cfg := config.Config{CoreMCPURL: "http://core.invalid/mcp", CoreTokenFile: "/t", CoreTools: "curated"}
	s := coreServer(cfg)
	if s.AllowAll || len(s.Allow) == 0 || len(s.Deny) == 0 || s.Name != "core" || s.TokenFile != "/t" {
		t.Fatalf("curated: %+v", s)
	}
	cfg.CoreTools = "all"
	if s = coreServer(cfg); !s.AllowAll || len(s.Deny) != 0 {
		t.Fatalf("all: %+v", s)
	}
}

func TestOtherServersCarryNoTokenForConnectors(t *testing.T) {
	cfg := config.Config{
		EgoMCPURL: "http://ego.invalid/mcp", OtelTokenFile: "/otel",
		Connectors: []config.Connector{{Name: "example-kube", URL: "http://kube.invalid/mcp", Tools: []string{"get_pods"}}},
	}
	got := otherServers(cfg)
	if len(got) != 2 || got[0].Name != "ego" || got[0].TokenFile != "/otel" || got[1].Name != "example-kube" || got[1].TokenFile != "" ||
		len(got[1].Allow) != 1 || got[1].Allow[0] != "get_pods" {
		t.Fatalf("%+v", got)
	}
	if len(otherServers(config.Config{})) != 0 {
		t.Fatal("no ego, no connectors, no servers")
	}
}

func TestBadAuthFailsFastAtStart(t *testing.T) {
	dir := t.TempDir()
	tok := filepath.Join(dir, "token")
	_ = os.WriteFile(tok, []byte("t"), 0o600)
	var out, errOut bytes.Buffer
	code := run([]string{"--tenant", "example", "--core-mcp-url", "http://core.invalid/mcp", "--core-token-file", tok,
		"--anthropic-key-file", filepath.Join(dir, "missing"), "--state-dir", filepath.Join(dir, "state")}, strings.NewReader(""), &out, &errOut)
	if code != 1 || !strings.Contains(errOut.String(), "model client") {
		t.Fatalf("a missing key file must stop the start: %d %s", code, errOut.String())
	}
}

func TestGeminiKeyFileMissingFailsFastAtStart(t *testing.T) {
	dir := t.TempDir()
	tok := filepath.Join(dir, "token")
	_ = os.WriteFile(tok, []byte("t"), 0o600)
	var out, errOut bytes.Buffer
	code := run([]string{"--tenant", "example", "--core-mcp-url", "http://core.invalid/mcp", "--core-token-file", tok,
		"--llm-provider", "gemini", "--gemini-key-file", filepath.Join(dir, "missing"), "--state-dir", filepath.Join(dir, "state")}, strings.NewReader(""), &out, &errOut)
	if code != 1 || !strings.Contains(errOut.String(), "model client") || !strings.Contains(errOut.String(), "gemini-2.5-flash") {
		t.Fatalf("a missing Gemini key file must stop the start, with the provider default model: %d %s", code, errOut.String())
	}
}
