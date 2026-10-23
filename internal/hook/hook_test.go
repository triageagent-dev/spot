package hook

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func run(t *testing.T, o Options, ev map[string]any) (string, string, int) {
	t.Helper()
	b, _ := json.Marshal(ev)
	var out, errOut bytes.Buffer
	code := Run(bytes.NewReader(b), &out, &errOut, o)
	return out.String(), errOut.String(), code
}

func bash(cmd string) map[string]any {
	return map[string]any{"session_id": "s", "tool_use_id": "t", "tool_name": "Bash", "tool_input": map[string]any{"command": cmd}}
}

func TestAuditsEveryBashCommandAndIgnoresOtherTools(t *testing.T) {
	d := t.TempDir()
	o := Options{StateDir: d}
	for _, c := range []string{"jq .a x", "ls", "head -n1 y"} {
		if out, _, code := run(t, o, bash(c)); code != 0 || out != "" {
			t.Fatalf("%q: code %d out %q", c, code, out)
		}
	}
	if out, _, code := run(t, o, map[string]any{"tool_name": "Read", "tool_input": map[string]any{"file_path": "/x"}}); code != 0 || out != "" {
		t.Fatalf("non-bash: %d %q", code, out)
	}
	b, err := os.ReadFile(filepath.Join(d, SupervisorDir, AuditFile))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 audit lines (Bash only), got %d: %s", len(lines), b)
	}
	if !strings.Contains(lines[2], `"verdict":"allowed"`) || !strings.Contains(lines[2], "head -n1 y") {
		t.Fatalf("audit line: %s", lines[2])
	}
}

func TestFailsOpen(t *testing.T) {
	// Unwritable state dir (a path under a plain file): audit and counter
	// fail, the call is still allowed.
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	o := Options{StateDir: filepath.Join(blocker, "dir")}
	out, errOut, code := run(t, o, bash("ls"))
	if code != 0 || out != "" || errOut == "" {
		t.Fatalf("code=%d out=%q err=%q", code, out, errOut)
	}
	var out2, err2 bytes.Buffer
	if code := Run(strings.NewReader("{not json"), &out2, &err2, o); code != 0 || out2.Len() != 0 {
		t.Fatalf("garbage input must be allowed, code %d", code)
	}
}

func TestAuditRotates(t *testing.T) {
	d := t.TempDir()
	o := Options{StateDir: d, MaxAuditSize: 200}
	for i := 0; i < 10; i++ {
		run(t, o, bash("echo "+strings.Repeat("x", 50)))
	}
	if _, err := os.Stat(filepath.Join(d, SupervisorDir, AuditFile+".1")); err != nil {
		t.Fatalf("audit log did not rotate: %v", err)
	}
}
