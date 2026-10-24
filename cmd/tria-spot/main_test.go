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
	if code := run(nil, strings.NewReader(""), &out, &errOut); code != 2 || !strings.Contains(errOut.String(), "--tenant is required") {
		t.Fatalf("missing flags: %d %q", code, errOut.String())
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
