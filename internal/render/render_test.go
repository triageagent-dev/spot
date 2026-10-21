package render

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setup(t *testing.T) (Input, string) {
	t.Helper()
	d := t.TempDir()
	tok := filepath.Join(d, "token")
	key := filepath.Join(d, "key")
	must(t, os.WriteFile(tok, []byte("core-secret-token\n"), 0o600))
	must(t, os.WriteFile(key, []byte("sk-ant-test-key\n"), 0o600))
	run := filepath.Join(d, "run")
	return Input{
		RunDir: run, StateDir: filepath.Join(d, "state"),
		CoreMCPURL: "https://core.example.invalid/mcp", CoreTokenFile: tok, AnthropicKeyFile: key,
		HookBin: "/usr/local/bin/tria-spot", KubectlWritesHr: 7, Instruction: "hello",
	}, d
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRenderModes(t *testing.T) {
	in, _ := setup(t)
	p, err := Render(in)
	must(t, err)
	want := map[string]os.FileMode{p.MCPConfig: 0o400, p.Settings: 0o400, p.Instruction: 0o400, p.KeyHelper: 0o500}
	for path, mode := range want {
		st, err := os.Stat(path)
		must(t, err)
		if st.Mode().Perm() != mode {
			t.Errorf("%s mode %o, want %o", filepath.Base(path), st.Mode().Perm(), mode)
		}
	}
	st, err := os.Stat(in.RunDir)
	must(t, err)
	if st.Mode().Perm() != 0o700 {
		t.Errorf("run dir mode %o, want 700", st.Mode().Perm())
	}
	entries, _ := os.ReadDir(in.RunDir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Errorf("temp file left behind: %s", e.Name())
		}
	}
}

func TestRenderMCPAndSettingsContent(t *testing.T) {
	in, _ := setup(t)
	p, err := Render(in)
	must(t, err)

	var mcp struct {
		MCPServers map[string]struct {
			Type    string            `json:"type"`
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"mcpServers"`
	}
	b, _ := os.ReadFile(p.MCPConfig)
	must(t, json.Unmarshal(b, &mcp))
	core := mcp.MCPServers[ServerName]
	if core.Type != "http" || core.URL != in.CoreMCPURL {
		t.Fatalf("mcp entry: %+v", core)
	}
	if core.Headers["Authorization"] != "Bearer core-secret-token" {
		t.Fatalf("token not rendered from the file: %q", core.Headers["Authorization"])
	}

	sb, _ := os.ReadFile(p.Settings)
	if strings.Contains(string(sb), "sk-ant-test-key") || strings.Contains(string(sb), "core-secret-token") {
		t.Fatal("settings must not contain a credential")
	}
	var st map[string]any
	must(t, json.Unmarshal(sb, &st))
	if st["apiKeyHelper"] != p.KeyHelper {
		t.Fatalf("apiKeyHelper = %v", st["apiKeyHelper"])
	}
	if !strings.Contains(string(sb), `"matcher": "Bash"`) || !strings.Contains(string(sb), "--kubectl-writes-per-hour") {
		t.Fatalf("hook not configured: %s", sb)
	}
	if strings.Contains(string(sb), "7\n") || !strings.Contains(string(sb), `"7"`) {
		t.Fatalf("writes cap not rendered as an arg: %s", sb)
	}
}

func TestKeyHelperPrintsKeyAndHoldsNoSecret(t *testing.T) {
	in, _ := setup(t)
	p, err := Render(in)
	must(t, err)
	b, _ := os.ReadFile(p.KeyHelper)
	if strings.Contains(string(b), "sk-ant-test-key") {
		t.Fatal("helper must read the file, not embed the key")
	}
	if !strings.Contains(string(b), "'"+in.AnthropicKeyFile+"'") {
		t.Fatalf("helper does not read the key file: %s", b)
	}
}

func TestRenderReplacesExistingReadOnlyFiles(t *testing.T) {
	in, _ := setup(t)
	_, err := Render(in)
	must(t, err)
	in.Instruction = "second"
	p, err := Render(in) // files exist with 0400: the rename must still work
	must(t, err)
	b, _ := os.ReadFile(p.Instruction)
	if string(b) != "second" {
		t.Fatalf("instruction not replaced: %q", b)
	}
}

func TestRenderRejectsMissingOrEmptySecrets(t *testing.T) {
	in, _ := setup(t)
	must(t, os.WriteFile(in.CoreTokenFile, []byte("  \n"), 0o600))
	if _, err := Render(in); err == nil {
		t.Fatal("empty token must fail")
	}
	in, _ = setup(t)
	in.AnthropicKeyFile = filepath.Join(t.TempDir(), "nope")
	if _, err := Render(in); err == nil {
		t.Fatal("missing key file must fail")
	}
}

func TestShQuote(t *testing.T) {
	if got := shQuote("a'b"); got != `'a'\''b'` {
		t.Fatalf("got %s", got)
	}
}
