package hook

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMutatingKubectl(t *testing.T) {
	cases := []struct {
		cmd  string
		want []string
	}{
		{"kubectl get pods -A", nil},
		{"kubectl describe pod x -n ns && kubectl logs x", nil},
		{"kubectl delete pod x -n ns", []string{"delete"}},
		{"kubectl -n ns --context c delete pod x", []string{"delete"}},
		{"/usr/local/bin/kubectl scale deploy/x --replicas=2", []string{"scale"}},
		{"kubectl rollout restart deploy/x", []string{"rollout restart"}},
		{"kubectl rollout undo deploy/x", []string{"rollout undo"}},
		{"kubectl rollout status deploy/x", nil},
		{"kubectl rollout history deploy/x", nil},
		{"kubectl get pods | xargs kubectl delete", []string{"delete"}},
		{`bash -c "kubectl apply -f x.yaml"`, []string{"apply"}},
		{`sh -c 'kubectl patch deploy x -p {} ; kubectl label pod y a=b'`, []string{"patch", "label"}},
		{"kubectl delete pod x --dry-run=client", nil},
		{"kubectl delete pod x --dry-run=none", []string{"delete"}},
		{"kubectl annotate pod x a=b && kubectl cordon n1 && kubectl drain n1 && kubectl taint nodes n1 k=v:NoSchedule", []string{"annotate", "cordon", "drain", "taint"}},
		{"kubectl edit cm x; kubectl replace -f y; kubectl create ns z", []string{"edit", "replace", "create"}},
		{"echo kubectl", nil},
		{"kubectl", nil},
		{"kubectl -n ns", nil},
		{"ls /tmp; kubectl get pods; rm x", nil},
		{"mykubectl delete pod x", nil},
	}
	for _, c := range cases {
		got := MutatingKubectl(c.cmd)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%q: got %v, want %v", c.cmd, got, c.want)
		}
	}
}

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
	o := Options{StateDir: d, MaxPerHour: 5}
	for _, c := range []string{"kubectl get pods", "ls", "kubectl delete pod a"} {
		if out, _, code := run(t, o, bash(c)); code != 0 || out != "" {
			t.Fatalf("%q: code %d out %q", c, code, out)
		}
	}
	if out, _, code := run(t, o, map[string]any{"tool_name": "Read", "tool_input": map[string]any{"file_path": "/x"}}); code != 0 || out != "" {
		t.Fatalf("non-bash: %d %q", code, out)
	}
	b, err := os.ReadFile(filepath.Join(d, AuditFile))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 audit lines (Bash only), got %d: %s", len(lines), b)
	}
	if !strings.Contains(lines[2], `"mutating":["delete"]`) || !strings.Contains(lines[2], "kubectl delete pod a") {
		t.Fatalf("audit line: %s", lines[2])
	}
}

func TestHourlyCapDeniesMutatingOnly(t *testing.T) {
	d := t.TempDir()
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	o := Options{StateDir: d, MaxPerHour: 2, Now: func() time.Time { return now }}

	for i := 0; i < 2; i++ {
		if out, _, _ := run(t, o, bash("kubectl delete pod p")); out != "" {
			t.Fatalf("write %d under the cap was denied: %s", i, out)
		}
	}
	out, _, code := run(t, o, bash("kubectl rollout restart deploy/x"))
	if code != 0 {
		t.Fatalf("a denial is JSON on exit 0, got code %d", code)
	}
	var d1 decision
	if err := json.Unmarshal([]byte(out), &d1); err != nil {
		t.Fatalf("not a decision: %q", out)
	}
	h := d1.HookSpecificOutput
	if h.HookEventName != "PreToolUse" || h.PermissionDecision != "deny" || !strings.Contains(h.PermissionDecisionReason, "cap 2") {
		t.Fatalf("decision: %+v", h)
	}
	// Reads still pass over the cap.
	if out, _, _ := run(t, o, bash("kubectl get pods")); out != "" {
		t.Fatalf("a read was denied: %s", out)
	}
	// A denied command does not use up the budget; an hour later it frees up.
	now = now.Add(61 * time.Minute)
	if out, _, _ := run(t, o, bash("kubectl delete pod p")); out != "" {
		t.Fatalf("cap did not reset after an hour: %s", out)
	}
	b, _ := os.ReadFile(filepath.Join(d, AuditFile))
	if !strings.Contains(string(b), `"verdict":"denied"`) {
		t.Fatalf("denial not audited: %s", b)
	}
}

func TestCompoundCommandCountsEveryWrite(t *testing.T) {
	o := Options{StateDir: t.TempDir(), MaxPerHour: 2}
	out, _, _ := run(t, o, bash("kubectl delete pod a; kubectl delete pod b; kubectl delete pod c"))
	if out == "" {
		t.Fatal("three writes in one command must exceed a cap of 2")
	}
}

func TestZeroCapDisablesCounting(t *testing.T) {
	o := Options{StateDir: t.TempDir(), MaxPerHour: 0}
	for i := 0; i < 50; i++ {
		if out, _, _ := run(t, o, bash("kubectl delete pod a")); out != "" {
			t.Fatal("cap 0 means no cap")
		}
	}
}

func TestFailsOpen(t *testing.T) {
	// Unwritable state dir: audit and counter fail, the call is still allowed.
	o := Options{StateDir: filepath.Join(t.TempDir(), "missing", "dir"), MaxPerHour: 1}
	out, errOut, code := run(t, o, bash("kubectl delete pod a"))
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
	if _, err := os.Stat(filepath.Join(d, AuditFile+".1")); err != nil {
		t.Fatalf("audit log did not rotate: %v", err)
	}
}
