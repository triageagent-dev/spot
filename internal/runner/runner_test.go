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
		"--disallowedTools\x00WebFetch,WebSearch",
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
	for _, want := range []string{"Edit(//state/**)", "mcp__core__*", "Bash(kubectl *)"} {
		if !strings.Contains(allowed, want) {
			t.Errorf("allowed tools lack %q: %s", want, allowed)
		}
	}
	// A bare Bash or an unscoped Edit would pre-approve everything.
	for _, r := range DefaultAllowed("/state") {
		if r == "Bash" || r == "Edit" || r == "Write" || r == "Bash(*)" || r == "Bash(cat *)" {
			t.Errorf("too broad: %s", r)
		}
	}
}

func TestScrubEnv(t *testing.T) {
	in := []string{"PATH=/bin", "ANTHROPIC_API_KEY=x", "ANTHROPIC_BASE_URL=y", "MY_TOKEN=t", "db_password=p",
		"AWS_SECRET_ACCESS_KEY=s", "KUBERNETES_SERVICE_HOST=h", "CLAUDE_CODE_OAUTH_TOKEN=o", "LANG=C"}
	out := ScrubEnv(in, "HOME=/state/home")
	got := strings.Join(out, " ")
	if got != "PATH=/bin KUBERNETES_SERVICE_HOST=h LANG=C HOME=/state/home" {
		t.Fatalf("got %q", got)
	}
}
