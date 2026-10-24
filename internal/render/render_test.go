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
		Instruction: "hello",
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
	if strings.Contains(string(sb), "hooks") {
		t.Fatalf("no hook: the shell is not among the agent's tools: %s", sb)
	}
	var perm struct {
		Permissions struct{ Deny []string } `json:"permissions"`
	}
	must(t, json.Unmarshal(sb, &perm))
	deny := strings.Join(perm.Permissions.Deny, "\n")
	// Credentials by directory (a volume serves the file through ..data), the
	// kubelet's mounts, and the directories the agent must not write.
	for _, want := range []string{
		"Read(" + absRule(filepath.Dir(in.CoreTokenFile)) + "/**)",
		"Read(" + absRule(filepath.Dir(in.AnthropicKeyFile)) + "/**)",
		"Read(//var/run/secrets/**)",
		"Read(" + absRule(in.RunDir) + "/**)",
		"Edit(" + absRule(filepath.Join(in.StateDir, "supervisor")) + "/**)",
		"Write(" + absRule(filepath.Join(in.StateDir, "work")) + "/**)",
		"Write(" + absRule(filepath.Join(in.StateDir, "claude")) + "/**)",
		"Write(" + absRule(in.RunDir) + "/**)",
	} {
		if !strings.Contains(deny, want) {
			t.Errorf("deny rules lack %q:\n%s", want, deny)
		}
	}
	for _, r := range perm.Permissions.Deny {
		if strings.Contains(r, "notes") || strings.Contains(r, "next.json") {
			t.Errorf("the agent's own files must stay writable: %s", r)
		}
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

func fedInput(t *testing.T) Input {
	in, d := setup(t)
	in.AnthropicKeyFile = ""
	in.Federation = &Federation{
		IdentityTokenFile: filepath.Join(d, "identity-token"), RuleID: "fdrl_example",
		OrganizationID: "00000000-0000-0000-0000-000000000000", ServiceAccountID: "svac_example", WorkspaceID: "wrkspc_example",
	}
	return in
}

func TestFederationRenderHasNoKeyHelperAndNoKeyReference(t *testing.T) {
	in := fedInput(t)
	// A stale helper from key mode must not survive.
	must(t, os.MkdirAll(in.RunDir, 0o700))
	must(t, os.WriteFile(filepath.Join(in.RunDir, "api-key-helper.sh"), []byte("#!/bin/sh\n"), 0o500))
	p, err := Render(in)
	must(t, err)
	if p.KeyHelper != "" {
		t.Fatalf("KeyHelper = %q", p.KeyHelper)
	}
	if _, err := os.Stat(filepath.Join(in.RunDir, "api-key-helper.sh")); !os.IsNotExist(err) {
		t.Fatal("api-key-helper.sh must not exist in federation mode")
	}
	sb, _ := os.ReadFile(p.Settings)
	if strings.Contains(string(sb), "apiKeyHelper") || strings.Contains(string(sb), "api-key-helper") {
		t.Fatalf("federation settings must carry no apiKeyHelper (it outranks federation): %s", sb)
	}
	var st struct {
		Permissions struct{ Deny []string } `json:"permissions"`
	}
	must(t, json.Unmarshal(sb, &st))
	deny := strings.Join(st.Permissions.Deny, "\n")
	for _, want := range []string{"Read(//", filepath.Dir(in.Federation.IdentityTokenFile)[1:] + "/**", filepath.Dir(in.CoreTokenFile)[1:] + "/**", strings.TrimPrefix(in.RunDir, "/") + "/**"} {
		if !strings.Contains(deny, want) {
			t.Errorf("deny rules lack %q:\n%s", want, deny)
		}
	}
	for _, f := range []string{p.MCPConfig, p.Settings, p.Instruction} {
		b, _ := os.ReadFile(f)
		if strings.Contains(string(b), "sk-ant") {
			t.Errorf("%s mentions a key", f)
		}
	}
}

func TestFederationProfile(t *testing.T) {
	in := fedInput(t)
	p, err := Render(in)
	must(t, err)
	if p.Profile != ProfileName || p.AnthropicConfigDir != filepath.Join(in.RunDir, "anthropic") {
		t.Fatalf("paths: %+v", p)
	}
	// On the memory-backed run dir, with the layout the SDK expects.
	prof := filepath.Join(p.AnthropicConfigDir, "configs", ProfileName+".json")
	st, err := os.Stat(prof)
	must(t, err)
	if st.Mode().Perm() != 0o400 {
		t.Errorf("profile mode %o", st.Mode().Perm())
	}
	for _, d := range []string{p.AnthropicConfigDir, filepath.Join(p.AnthropicConfigDir, "configs"), filepath.Join(p.AnthropicConfigDir, "credentials")} {
		st, err := os.Stat(d)
		must(t, err)
		if st.Mode().Perm() != 0o700 {
			t.Errorf("%s mode %o", d, st.Mode().Perm())
		}
	}
	var got struct {
		Version        string `json:"version"`
		Authentication struct {
			Type, FederationRuleID, ServiceAccountID string
			IdentityToken                            struct{ Source, Path string } `json:"identity_token"`
		} `json:"authentication"`
		OrganizationID string `json:"organization_id"`
		WorkspaceID    string `json:"workspace_id"`
		BaseURL        string `json:"base_url"`
	}
	b, _ := os.ReadFile(prof)
	must(t, json.Unmarshal(b, &got))
	// The authentication block is checked by its WIF-reference key names.
	var raw map[string]any
	must(t, json.Unmarshal(b, &raw))
	auth := raw["authentication"].(map[string]any)
	tok := auth["identity_token"].(map[string]any)
	if got.Version != "1.0" || auth["type"] != "oidc_federation" || auth["federation_rule_id"] != "fdrl_example" ||
		auth["service_account_id"] != "svac_example" || tok["source"] != "file" || tok["path"] != in.Federation.IdentityTokenFile ||
		got.OrganizationID != in.Federation.OrganizationID || got.WorkspaceID != "wrkspc_example" || got.BaseURL != "https://api.anthropic.com" {
		t.Fatalf("profile: %s", b)
	}

	// Workspace is optional.
	in2 := fedInput(t)
	in2.Federation.WorkspaceID = ""
	p2, err := Render(in2)
	must(t, err)
	b2, _ := os.ReadFile(filepath.Join(p2.AnthropicConfigDir, "configs", ProfileName+".json"))
	if strings.Contains(string(b2), "workspace_id") {
		t.Fatalf("empty workspace must be omitted: %s", b2)
	}
}

func TestFederationRejectsAKeyFile(t *testing.T) {
	in := fedInput(t)
	in.AnthropicKeyFile = in.CoreTokenFile
	if _, err := Render(in); err == nil {
		t.Fatal("a key file next to federation would shadow it")
	}
}

func TestKeyModeStillRendersTheHelper(t *testing.T) {
	in, _ := setup(t)
	p, err := Render(in)
	must(t, err)
	sb, _ := os.ReadFile(p.Settings)
	if !strings.Contains(string(sb), "apiKeyHelper") || p.AnthropicConfigDir != "" || p.Profile != "" {
		t.Fatalf("key mode: %+v %s", p, sb)
	}
	if _, err := os.Stat(filepath.Join(in.RunDir, "anthropic")); !os.IsNotExist(err) {
		t.Fatal("key mode must not create a federation profile")
	}
}

func TestOtelHeaderHelperReadsTheTokenFromAFile(t *testing.T) {
	in, d := setup(t)
	if p, err := Render(in); err != nil || p.OtelHelper != "" {
		t.Fatalf("no telemetry token, no helper: %+v %v", p, err)
	}
	otel := filepath.Join(d, "otel-token")
	must(t, os.WriteFile(otel, []byte("egs_example-sender-token\n"), 0o600))
	in.OtelTokenFile = otel
	p, err := Render(in)
	must(t, err)
	script, _ := os.ReadFile(p.OtelHelper)
	if !strings.Contains(string(script), otel) || strings.Contains(string(script), "egs_example") {
		t.Fatalf("the helper must read the file and hold no token:\n%s", script)
	}
	sb, _ := os.ReadFile(p.Settings)
	var st map[string]any
	must(t, json.Unmarshal(sb, &st))
	if st["otelHeadersHelper"] != p.OtelHelper {
		t.Fatalf("otelHeadersHelper = %v", st["otelHeadersHelper"])
	}
	if !strings.Contains(string(sb), "Read("+absRule(filepath.Dir(otel))+"/**)") || strings.Contains(string(sb), "egs_example") {
		t.Fatalf("settings must deny the token file and not hold it: %s", sb)
	}
}

func TestWriteMCPAddsEgoWithItsOwnTokenAndReadsItEveryTime(t *testing.T) {
	d := t.TempDir()
	core, ego := filepath.Join(d, "core"), filepath.Join(d, "ego")
	must(t, os.WriteFile(core, []byte("core-tok\n"), 0o600))
	must(t, os.WriteFile(ego, []byte("ego-tok\n"), 0o600))
	path := filepath.Join(d, "mcp.json")
	must(t, WriteMCP(path, "https://core.example.invalid/mcp", core, Server{Name: EgoServerName, URL: "http://ego.example.invalid:8001/mcp", TokenFile: ego}))
	read := func() map[string]struct {
		Type    string
		URL     string
		Headers map[string]string
	} {
		var f struct {
			Servers map[string]struct {
				Type    string
				URL     string
				Headers map[string]string
			} `json:"mcpServers"`
		}
		b, err := os.ReadFile(path)
		must(t, err)
		must(t, json.Unmarshal(b, &f))
		return f.Servers
	}
	got := read()
	if got["core"].Headers["Authorization"] != "Bearer core-tok" || got["ego"].Headers["Authorization"] != "Bearer ego-tok" || got["ego"].URL != "http://ego.example.invalid:8001/mcp" {
		t.Fatalf("servers: %+v", got)
	}
	// A rotated token reaches the next render, and a missing one fails it.
	must(t, os.Chmod(path, 0o600))
	must(t, os.WriteFile(ego, []byte("ego-new\n"), 0o600))
	must(t, WriteMCP(path, "https://core.example.invalid/mcp", core, Server{Name: EgoServerName, URL: "http://ego.example.invalid:8001/mcp", TokenFile: ego}))
	if read()["ego"].Headers["Authorization"] != "Bearer ego-new" {
		t.Fatal("the rotated ego token was not picked up")
	}
	if err := WriteMCP(path, "https://core.example.invalid/mcp", core, Server{Name: EgoServerName, URL: "http://x", TokenFile: filepath.Join(d, "gone")}); err == nil {
		t.Fatal("a missing ego token must fail the render")
	}
	// Without ego there is only the core.
	must(t, WriteMCP(path, "https://core.example.invalid/mcp", core))
	if len(read()) != 1 {
		t.Fatalf("core alone: %+v", read())
	}
}

func TestWriteMCPConnectorCarriesNoCredential(t *testing.T) {
	d := t.TempDir()
	core := filepath.Join(d, "core")
	must(t, os.WriteFile(core, []byte("core-token\n"), 0o400))
	path := filepath.Join(d, "mcp.json")
	must(t, WriteMCP(path, "https://core.example.invalid/mcp", core, Server{Name: "example-kube", URL: "http://tools.example.invalid:8084/mcp"}))
	b, err := os.ReadFile(path)
	must(t, err)
	var got struct {
		MCPServers map[string]struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"mcpServers"`
	}
	must(t, json.Unmarshal(b, &got))
	k := got.MCPServers["example-kube"]
	if k.URL != "http://tools.example.invalid:8084/mcp" || len(k.Headers) != 0 {
		t.Fatalf("connector entry = %+v: it must carry a URL and no header", k)
	}
	if got.MCPServers[ServerName].Headers["Authorization"] != "Bearer core-token" {
		t.Fatalf("the core keeps its token: %+v", got.MCPServers[ServerName])
	}
}

func TestSandboxSettings(t *testing.T) {
	in := Input{RunDir: "/run/tria-spot", StateDir: "/state", CoreTokenFile: "/secrets/core/token", AnthropicKeyFile: "/secrets/anthropic/key", OtelTokenFile: "/secrets/otel/token", Sandbox: true}
	st := Settings(in, "/run/tria-spot/api-key-helper.sh")
	sb, ok := st["sandbox"].(map[string]any)
	if !ok {
		t.Fatalf("no sandbox block: %v", st)
	}
	for k, want := range map[string]any{"enabled": true, "failIfUnavailable": true, "allowUnsandboxedCommands": false, "autoAllowBashIfSandboxed": true} {
		if sb[k] != want {
			t.Errorf("sandbox.%s = %v, want %v", k, sb[k], want)
		}
	}
	if ex, _ := sb["excludedCommands"].([]string); len(ex) != 0 {
		t.Errorf("no command may run outside the sandbox: %v", ex)
	}
	net := sb["network"].(map[string]any)
	if d, _ := net["allowedDomains"].([]string); len(d) != 0 {
		t.Errorf("the sandbox must have no allowed domain: %v", d)
	}
	fs := sb["filesystem"].(map[string]any)
	if w := fs["allowWrite"].([]string); len(w) != 1 || w[0] != "/state/notes" {
		t.Errorf("allowWrite = %v", w)
	}
	deny := strings.Join(fs["denyRead"].([]string), " ")
	for _, d := range []string{"/run/tria-spot", "/var/run/secrets", "/run/secrets", "/secrets/core", "/secrets/otel", "/secrets/anthropic"} {
		if !strings.Contains(deny, d) {
			t.Errorf("denyRead lacks %s: %s", d, deny)
		}
	}
	nw := strings.Join(fs["denyWrite"].([]string), " ")
	for _, d := range []string{"/state/supervisor", "/state/claude", "/state/home", "/state/work", "/run/tria-spot"} {
		if !strings.Contains(nw, d) {
			t.Errorf("denyWrite lacks %s: %s", d, nw)
		}
	}
	if _, has := Settings(Input{RunDir: "/r", StateDir: "/s", CoreTokenFile: "/c/t", AnthropicKeyFile: "/a/k"}, "h")["sandbox"]; has {
		t.Error("no sandbox block unless asked for")
	}
}
