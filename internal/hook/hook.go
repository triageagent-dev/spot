// Package hook implements `tria-spot hook pretool`, the Claude Code PreToolUse
// hook (https://code.claude.com/docs/en/hooks). It appends every Bash command
// to the audit log and refuses a mutating kubectl command once the per-hour
// cap is used up.
//
// This is NOT the security boundary. A shell can route around any text check
// (an interpreter, a script written to disk, a client library). The boundary
// is the ServiceAccount's RBAC. The hook makes mistakes visible and slows a
// runaway loop.
package hook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Input is the part of the hook's stdin JSON used here.
type Input struct {
	SessionID string `json:"session_id"`
	ToolName  string `json:"tool_name"`
	ToolUseID string `json:"tool_use_id"`
	ToolInput struct {
		Command string `json:"command"`
	} `json:"tool_input"`
}

// Options configure Run.
type Options struct {
	StateDir     string
	MaxPerHour   int // 0 = no cap
	Now          func() time.Time
	MaxAuditSize int64 // rotate audit.log past this; default 5 MiB
}

// Files, under the supervisor directory of the state directory, which the
// agent's Edit and Write tools are denied.
const (
	SupervisorDir = "supervisor"
	AuditFile     = "audit.log"
	CounterFile   = "hook-writes.json"
)

// dir is where the hook keeps its files. It is created if missing, so the
// hook works against a state directory the supervisor has not laid out.
func dir(o Options) string {
	d := filepath.Join(o.StateDir, SupervisorDir)
	_ = os.MkdirAll(d, 0o700)
	return d
}

// decision is the PreToolUse JSON output for a denial.
type decision struct {
	HookSpecificOutput struct {
		HookEventName            string `json:"hookEventName"`
		PermissionDecision       string `json:"permissionDecision"`
		PermissionDecisionReason string `json:"permissionDecisionReason"`
	} `json:"hookSpecificOutput"`
}

// Run reads one hook event from in and writes the decision, if any, to out.
// It returns the process exit code. It fails open: an internal error is
// reported on errOut and the call is allowed (exit 0 with no output means no
// decision), because a broken hook must not stop the agent from investigating.
func Run(in io.Reader, out, errOut io.Writer, o Options) int {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxAuditSize == 0 {
		o.MaxAuditSize = 5 << 20
	}
	raw, err := io.ReadAll(io.LimitReader(in, 4<<20))
	if err != nil {
		fmt.Fprintln(errOut, "tria-spot hook: read stdin:", err)
		return 0
	}
	var ev Input
	if err := json.Unmarshal(raw, &ev); err != nil {
		fmt.Fprintln(errOut, "tria-spot hook: bad input:", err)
		return 0
	}
	if ev.ToolName != "Bash" || strings.TrimSpace(ev.ToolInput.Command) == "" {
		return 0
	}
	cmd := ev.ToolInput.Command
	writes := MutatingKubectl(cmd)

	verdict := "allowed"
	reason := ""
	if len(writes) > 0 && o.MaxPerHour > 0 {
		ok, used, err := reserve(o, len(writes))
		switch {
		case err != nil:
			fmt.Fprintln(errOut, "tria-spot hook: counter:", err)
		case !ok:
			verdict = "denied"
			reason = fmt.Sprintf("tria-spot: %d mutating kubectl commands already ran in the last hour (cap %d). Stop making changes, record what you know on the issue with comment_on_issue, and escalate to a human.", used, o.MaxPerHour)
		}
	}
	audit(o, auditLine{
		Time: o.Now().UTC().Format(time.RFC3339), Session: ev.SessionID, ToolUseID: ev.ToolUseID,
		Verdict: verdict, Mutating: writes, Command: clip(cmd, 4000),
	}, errOut)

	if verdict == "denied" {
		var d decision
		d.HookSpecificOutput.HookEventName = "PreToolUse"
		d.HookSpecificOutput.PermissionDecision = "deny"
		d.HookSpecificOutput.PermissionDecisionReason = reason
		_ = json.NewEncoder(out).Encode(d)
	}
	return 0
}

type auditLine struct {
	Time      string   `json:"ts"`
	Session   string   `json:"session,omitempty"`
	ToolUseID string   `json:"toolUseId,omitempty"`
	Verdict   string   `json:"verdict"`
	Mutating  []string `json:"mutating,omitempty"`
	Command   string   `json:"command"`
}

func audit(o Options, l auditLine, errOut io.Writer) {
	path := filepath.Join(dir(o), AuditFile)
	if st, err := os.Stat(path); err == nil && st.Size() > o.MaxAuditSize {
		_ = os.Rename(path, path+".1")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintln(errOut, "tria-spot hook: audit log:", err)
		return
	}
	defer f.Close()
	b, _ := json.Marshal(l)
	_, _ = f.Write(append(b, '\n'))
}

type counter struct {
	Writes []int64 `json:"writes"` // unix seconds
}

// reserve records n mutating commands if the hour has room. It returns
// whether they may run and how many ran in the last hour before this call.
func reserve(o Options, n int) (ok bool, used int, err error) {
	path := filepath.Join(dir(o), CounterFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return true, 0, err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return true, 0, err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	var c counter
	b, _ := io.ReadAll(f)
	_ = json.Unmarshal(b, &c)
	cutoff := o.Now().Add(-time.Hour).Unix()
	kept := c.Writes[:0]
	for _, t := range c.Writes {
		if t > cutoff {
			kept = append(kept, t)
		}
	}
	c.Writes = kept
	used = len(c.Writes)
	if used+n > o.MaxPerHour {
		return false, used, nil
	}
	for i := 0; i < n; i++ {
		c.Writes = append(c.Writes, o.Now().Unix())
	}
	out, _ := json.Marshal(c)
	if err := f.Truncate(0); err != nil {
		return true, used, err
	}
	if _, err := f.WriteAt(out, 0); err != nil {
		return true, used, err
	}
	return true, used, nil
}

// mutatingVerbs are kubectl verbs that change cluster state. "rollout" is
// handled separately (restart and undo mutate; status and history do not).
var mutatingVerbs = map[string]bool{
	"apply": true, "patch": true, "delete": true, "scale": true,
	"annotate": true, "label": true, "edit": true, "replace": true,
	"cordon": true, "uncordon": true, "drain": true, "taint": true,
	"create": true, "set": true, "run": true, "expose": true, "autoscale": true,
}

// flagsWithValue are kubectl global flags that take a separate value, so the
// word after them is not the verb.
var flagsWithValue = map[string]bool{
	"-n": true, "--namespace": true, "--context": true, "--cluster": true,
	"--user": true, "--kubeconfig": true, "-s": true, "--server": true,
	"--as": true, "--as-group": true, "--as-uid": true, "--token": true,
	"--certificate-authority": true, "--client-certificate": true, "--client-key": true,
	"--request-timeout": true, "-v": true, "--v": true, "--cache-dir": true,
	"--tls-server-name": true, "--profile": true, "--profile-output": true,
}

// MutatingKubectl returns one entry per mutating kubectl invocation found in
// a shell command line ("delete", "rollout restart", ...). It scans words, so
// it also sees kubectl inside `bash -c "..."`, `xargs kubectl ...` and a
// pipeline. A `--dry-run` other than `=none` or `=false` is not mutating.
func MutatingKubectl(command string) []string {
	words := words(command)
	var found []string
	for i := 0; i < len(words); i++ {
		if !isKubectl(words[i]) {
			continue
		}
		verb, rest, end := verbAfter(words, i+1)
		if verb == "" {
			i = end
			continue
		}
		mut := mutatingVerbs[verb]
		name := verb
		if verb == "rollout" {
			sub, _, _ := verbAfter(words, rest)
			mut = sub == "restart" || sub == "undo"
			name = "rollout " + sub
		}
		if mut && !dryRun(words[rest:end]) {
			found = append(found, name)
		}
		i = end
	}
	return found
}

// words splits a command line into words, treating shell separators and
// quotes as word boundaries. Separators become a ";" word.
func words(s string) []string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case ';', '|', '&', '(', ')', '`', '\n', '{', '}':
			b.WriteString(" ; ")
		case '"', '\'', '\t', '\r', '\\':
			b.WriteByte(' ')
		case '$':
			b.WriteByte(' ')
		default:
			b.WriteRune(r)
		}
	}
	return strings.Fields(b.String())
}

func isKubectl(w string) bool {
	return w == "kubectl" || strings.HasSuffix(w, "/kubectl")
}

// verbAfter finds the first non-flag word at or after i, stopping at a
// separator. It returns the verb, the index just past it, and the index of
// the separator (or end) that closes the command.
func verbAfter(words []string, i int) (verb string, next, end int) {
	end = len(words)
	for j := i; j < len(words); j++ {
		if words[j] == ";" {
			end = j
			break
		}
	}
	for j := i; j < end; j++ {
		w := words[j]
		if strings.HasPrefix(w, "-") {
			if flagsWithValue[w] {
				j++
			}
			continue
		}
		return w, j + 1, end
	}
	return "", end, end
}

func dryRun(ws []string) bool {
	for _, w := range ws {
		switch {
		case w == "--dry-run":
			return true
		case strings.HasPrefix(w, "--dry-run="):
			v := strings.TrimPrefix(w, "--dry-run=")
			return v != "none" && v != "false"
		}
	}
	return false
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return string(bytes.ToValidUTF8([]byte(s[:n]), nil)) + "...[truncated]"
}
