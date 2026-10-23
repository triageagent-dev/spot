package runner

import (
	"strings"
	"testing"
	"time"

	"github.com/triageagent-dev/tria-spot/internal/render"
)

func opts() Options {
	return Options{Bin: "claude", Model: "sonnet", MaxTurns: 12, Timeout: time.Minute, StateDir: "/state",
		Paths:          render.Paths{MCPConfig: "/run/tria-spot/mcp.json", Settings: "/run/tria-spot/settings.json", Instruction: "/run/tria-spot/instruction.md"},
		PermissionMode: "auto", AllowedTools: DefaultAllowed("/state")}
}

func TestBuildArgs(t *testing.T) {
	a := BuildArgs(opts(), "")
	j := "\x00" + strings.Join(a, "\x00") + "\x00"
	for _, want := range []string{
		"-p", "--output-format\x00stream-json", "--verbose", "--permission-mode\x00auto", "--permission-prompts\x00none",
		"--mcp-config\x00/run/tria-spot/mcp.json", "--strict-mcp-config", "--settings\x00/run/tria-spot/settings.json",
		"--append-system-prompt-file\x00/run/tria-spot/instruction.md", "--max-turns\x0012", "--model\x00sonnet",
		"--disallowedTools\x00" + strings.Join(DefaultDisallowed(), ","),
	} {
		if !strings.Contains(j, "\x00"+want+"\x00") {
			t.Errorf("missing %q in %q", want, a)
		}
	}
	if strings.Contains(j, "--resume") {
		t.Error("no resume for a new conversation")
	}
	b := BuildArgs(opts(), "abc-123")
	if !strings.Contains("\x00"+strings.Join(b, "\x00")+"\x00", "\x00--resume\x00abc-123\x00") {
		t.Errorf("resume missing: %q", b)
	}
	o := opts()
	o.Model = ""
	if strings.Contains(strings.Join(BuildArgs(o, ""), " "), "--model") {
		t.Error("an empty model must not be passed")
	}
}

func TestAllowedToolsAreScoped(t *testing.T) {
	allowed := strings.Join(DefaultAllowed("/state"), ",")
	for _, want := range []string{"Edit(//state/notes/**)", "Write(//state/next.json)", "mcp__core__list_open_issues", "mcp__core__say_in_workspace", "Bash(kubectl *)"} {
		if !strings.Contains(allowed, want) {
			t.Errorf("allowed tools lack %q: %s", want, allowed)
		}
	}
	// A bare Bash, an unscoped Edit, a wildcard over the core's tools or a
	// shell command that writes a file would pre-approve too much.
	for _, r := range DefaultAllowed("/state") {
		switch {
		case r == "Bash" || r == "Edit" || r == "Write" || r == "Bash(*)" || r == "Bash(cat *)",
			r == "mcp__core__*" || r == "Edit(//state/**)",
			strings.HasPrefix(r, "Bash(echo"), strings.HasPrefix(r, "Bash(mkdir"), strings.HasPrefix(r, "Bash(sort"):
			t.Errorf("too broad: %s", r)
		}
	}
	dis := strings.Join(DefaultDisallowed(), ",")
	for _, want := range []string{"mcp__core__resolve_issue", "mcp__core__forget_issue", "mcp__core__run_rsi", "Task", "Agent", "WebFetch"} {
		if !strings.Contains(dis, want) {
			t.Errorf("disallowed tools lack %q: %s", want, dis)
		}
	}
	if args := strings.Join(BuildArgs(opts(), ""), "\n"); !strings.Contains(args, "--disallowedTools\n"+dis) {
		t.Errorf("args lack the disallowed list:\n%s", args)
	}
}

func TestNeverBare(t *testing.T) {
	// --bare ignores profiles and federation variables.
	for _, resume := range []string{"", "id"} {
		for _, a := range BuildArgs(opts(), resume) {
			if a == "--bare" {
				t.Fatal("--bare must never be passed")
			}
		}
	}
}

func TestScrubEnv(t *testing.T) {
	in := []string{"PATH=/bin", "ANTHROPIC_API_KEY=x", "ANTHROPIC_BASE_URL=y", "MY_TOKEN=t", "db_password=p",
		"AWS_SECRET_ACCESS_KEY=s", "KUBERNETES_SERVICE_HOST=h", "CLAUDE_CODE_OAUTH_TOKEN=o", "LANG=C",
		"CLAUDE_CODE_USE_BEDROCK=1", "CLAUDE_CODE_USE_VERTEX=1"}
	out := ScrubEnv(in, "HOME=/state/home")
	got := strings.Join(out, " ")
	if got != "PATH=/bin KUBERNETES_SERVICE_HOST=h LANG=C HOME=/state/home" {
		t.Fatalf("got %q", got)
	}
}

func envMap(env []string) map[string]string {
	m := map[string]string{}
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		m[k] = v
	}
	return m
}

func TestFederationEnvAllowsOnlyConfigDirAndProfile(t *testing.T) {
	for k, v := range map[string]string{
		"ANTHROPIC_API_KEY": "k", "ANTHROPIC_AUTH_TOKEN": "t", "CLAUDE_CODE_OAUTH_TOKEN": "o",
		"ANTHROPIC_PROFILE": "other", "ANTHROPIC_CONFIG_DIR": "/elsewhere",
		"ANTHROPIC_FEDERATION_RULE_ID": "x", "ANTHROPIC_IDENTITY_TOKEN": "jwt", "ANTHROPIC_IDENTITY_TOKEN_FILE": "/f",
		"ANTHROPIC_ORGANIZATION_ID": "o", "ANTHROPIC_SERVICE_ACCOUNT_ID": "s", "ANTHROPIC_WORKSPACE_ID": "w",
		"CLAUDE_CODE_USE_BEDROCK": "1",
	} {
		t.Setenv(k, v)
	}
	o := opts()
	o.Paths.AnthropicConfigDir, o.Paths.Profile = "/run/tria-spot/anthropic", "spot"
	env := envMap(New(o).Env())
	var anthropic []string
	for k := range env {
		if strings.HasPrefix(k, "ANTHROPIC_") {
			anthropic = append(anthropic, k)
		}
	}
	if len(anthropic) != 2 || env["ANTHROPIC_CONFIG_DIR"] != "/run/tria-spot/anthropic" || env["ANTHROPIC_PROFILE"] != "spot" {
		t.Fatalf("federation env: %v", anthropic)
	}
	for _, k := range []string{"CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CODE_USE_BEDROCK"} {
		if _, ok := env[k]; ok {
			t.Errorf("%s leaked", k)
		}
	}
	if env["CLAUDE_CONFIG_DIR"] != "/state/claude" {
		t.Errorf("CLAUDE_CONFIG_DIR = %q", env["CLAUDE_CONFIG_DIR"])
	}
}

func TestKeyModeEnvCarriesNoAnthropicVariable(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "k")
	t.Setenv("ANTHROPIC_PROFILE", "p")
	for k := range envMap(New(opts()).Env()) {
		if strings.HasPrefix(k, "ANTHROPIC_") {
			t.Errorf("%s reached the CLI in key mode", k)
		}
	}
}

func TestEnvPutsKoDataFirstOnPath(t *testing.T) {
	t.Setenv("KO_DATA_PATH", "/var/run/ko")
	t.Setenv("PATH", "/usr/bin:/bin")
	var path string
	for _, kv := range New(opts()).Env() {
		if strings.HasPrefix(kv, "PATH=") {
			path = kv // the last one wins in exec
		}
	}
	if path != "PATH=/var/run/ko:/usr/bin:/bin" {
		t.Fatalf("PATH = %q", path)
	}
}

func TestTelemetryEnvCarriesNoSecret(t *testing.T) {
	o := opts()
	o.Telemetry = &Telemetry{Endpoint: "http://ego.example.svc:4318", Service: "example-spot", Tenant: "tenant-a", TenantName: "Tenant A"}
	env := strings.Join(New(o).Env(), "\n")
	for _, want := range []string{
		"CLAUDE_CODE_ENABLE_TELEMETRY=1", "CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1", "OTEL_TRACES_EXPORTER=otlp",
		"OTEL_METRICS_EXPORTER=none", "OTEL_LOGS_EXPORTER=none", "OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://ego.example.svc:4318", "OTEL_SERVICE_NAME=example-spot",
		"OTEL_RESOURCE_ATTRIBUTES=triage.tenant=tenant-a,ego.tenant.name=Tenant A,gen_ai.agent.name=example-spot,ego.thread=example-spot",
	} {
		if !strings.Contains(env, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, banned := range []string{"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_LOG_TOOL_DETAILS", "OTEL_LOG_USER_PROMPTS", "OTEL_LOG_TOOL_CONTENT"} {
		if strings.Contains(env, banned) {
			t.Errorf("%s must not be set by default", banned)
		}
	}
	o.Telemetry.ToolDetails = true
	if !strings.Contains(strings.Join(New(o).Env(), "\n"), "OTEL_LOG_TOOL_DETAILS=1") {
		t.Error("tool details on request")
	}
	if strings.Contains(strings.Join(New(opts()).Env(), "\n"), "OTEL_") {
		t.Error("telemetry off: no OTEL variable")
	}
}

func TestEgoToolsAreReadOnlyAndNamedOneByOne(t *testing.T) {
	rules := strings.Join(EgoAllowed(), " ")
	for _, want := range []string{"mcp__ego__list_sessions", "mcp__ego__get_usage", "mcp__ego__get_findings"} {
		if !strings.Contains(rules, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, banned := range []string{"register_correction", "label_session", "set_role_binding", "delete_role_binding", "run_findings", "evaluate_traces", "mcp__ego__*"} {
		if strings.Contains(rules, banned) {
			t.Errorf("the spot must not be allowed %s", banned)
		}
	}
}

func TestCoreToolsAllDropsTheDenyListButKeepsTheWebAndSubagentsOff(t *testing.T) {
	def := strings.Join(Disallowed(false), " ")
	if !strings.Contains(def, "mcp__core__resolve_issue") || !strings.Contains(def, "WebFetch") {
		t.Fatalf("curated: %s", def)
	}
	all := strings.Join(Disallowed(true), " ")
	if strings.Contains(all, "mcp__core__") || !strings.Contains(all, "WebFetch") || !strings.Contains(all, "Task") {
		t.Fatalf("all: %s", all)
	}
	if CoreAllTool != "mcp__core__*" {
		t.Fatalf("rule %q", CoreAllTool)
	}
}
