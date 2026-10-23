package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersionAndUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"version"}, strings.NewReader(""), &out, &errOut); code != 0 || !strings.HasPrefix(out.String(), "tria-spot ") {
		t.Fatalf("version: %d %q", code, out.String())
	}
	out.Reset()
	if code := run([]string{"hook"}, strings.NewReader(""), &out, &errOut); code != 2 {
		t.Fatalf("hook without a subcommand: %d", code)
	}
	if code := run(nil, strings.NewReader(""), &out, &errOut); code != 2 || !strings.Contains(errOut.String(), "--tenant is required") {
		t.Fatalf("missing flags: %d %q", code, errOut.String())
	}
}

func TestHookSubcommandAuditsAndAllows(t *testing.T) {
	dir := t.TempDir()
	ev := `{"session_id":"s","tool_name":"Bash","tool_input":{"command":"ls /state"}}`
	args := []string{"hook", "pretool", "--state-dir", dir}
	var out, errOut bytes.Buffer
	if code := run(args, strings.NewReader(ev), &out, &errOut); code != 0 || out.Len() != 0 {
		t.Fatalf("code %d out %q", code, out.String())
	}
	b, err := os.ReadFile(filepath.Join(dir, "supervisor", "audit.log"))
	if err != nil || !strings.Contains(string(b), "ls /state") {
		t.Fatalf("not audited: %v %s", err, b)
	}
}

func TestHookWithBadFlagsStillAllows(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"hook", "pretool", "--no-such-flag"}, strings.NewReader("{}"), &out, &errOut)
	if code != 0 || out.Len() != 0 {
		t.Fatalf("a broken hook invocation must not block tools: %d %q", code, out.String())
	}
}

func TestResolveClaudeBin(t *testing.T) {
	kd := t.TempDir()
	if got := resolveClaudeBin("claude", kd); got != "claude" {
		t.Fatalf("no kodata copy: %q", got)
	}
	if err := os.WriteFile(filepath.Join(kd, "claude"), []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := resolveClaudeBin("claude", kd); got != filepath.Join(kd, "claude") {
		t.Fatalf("kodata copy not used: %q", got)
	}
	if got := resolveClaudeBin("/opt/claude", kd); got != "/opt/claude" {
		t.Fatalf("explicit --claude-bin overridden: %q", got)
	}
	if got := resolveClaudeBin("claude", ""); got != "claude" {
		t.Fatalf("without KO_DATA_PATH: %q", got)
	}
}
