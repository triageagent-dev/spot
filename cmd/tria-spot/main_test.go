package main

import (
	"bytes"
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

func TestHookSubcommandDeniesOverTheCap(t *testing.T) {
	dir := t.TempDir()
	ev := `{"session_id":"s","tool_name":"Bash","tool_input":{"command":"kubectl delete pod x"}}`
	args := []string{"hook", "pretool", "--state-dir", dir, "--kubectl-writes-per-hour", "1"}
	var out, errOut bytes.Buffer
	if code := run(args, strings.NewReader(ev), &out, &errOut); code != 0 || out.Len() != 0 {
		t.Fatalf("first write: code %d out %q", code, out.String())
	}
	if code := run(args, strings.NewReader(ev), &out, &errOut); code != 0 || !strings.Contains(out.String(), `"permissionDecision":"deny"`) {
		t.Fatalf("second write must be denied: code %d out %q", code, out.String())
	}
}

func TestHookWithBadFlagsStillAllows(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"hook", "pretool", "--no-such-flag"}, strings.NewReader("{}"), &out, &errOut)
	if code != 0 || out.Len() != 0 {
		t.Fatalf("a broken hook invocation must not block tools: %d %q", code, out.String())
	}
}
