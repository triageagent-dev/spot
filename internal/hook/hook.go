// Package hook implements `tria-spot hook pretool`, the Claude Code PreToolUse
// hook (https://code.claude.com/docs/en/hooks). It appends every Bash command
// to the audit log and decides nothing: the spot has no cluster tool in its
// shell (a cluster is reached through connectors), so the log only makes what
// the shell did visible.
package hook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	Now          func() time.Time
	MaxAuditSize int64 // rotate audit.log past this; default 5 MiB
}

// Files, under the supervisor directory of the state directory, which the
// agent's Edit and Write tools are denied.
const (
	SupervisorDir = "supervisor"
	AuditFile     = "audit.log"
)

// dir is where the hook keeps its files. It is created if missing, so the
// hook works against a state directory the supervisor has not laid out.
func dir(o Options) string {
	d := filepath.Join(o.StateDir, SupervisorDir)
	_ = os.MkdirAll(d, 0o700)
	return d
}

// Run reads one hook event from in and writes the decision, if any, to out.
// It returns the process exit code and never denies: an internal error is
// reported on errOut and the call goes on (exit 0 with no output means no
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
	audit(o, auditLine{
		Time: o.Now().UTC().Format(time.RFC3339), Session: ev.SessionID, ToolUseID: ev.ToolUseID,
		Verdict: "allowed", Command: clip(cmd, 4000),
	}, errOut)
	return 0
}

type auditLine struct {
	Time      string `json:"ts"`
	Session   string `json:"session,omitempty"`
	ToolUseID string `json:"toolUseId,omitempty"`
	Verdict   string `json:"verdict"`
	Command   string `json:"command"`
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

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return string(bytes.ToValidUTF8([]byte(s[:n]), nil)) + "...[truncated]"
}
