package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/triageagent-dev/tria-spot/internal/render"
)

// The test binary doubles as the CLI: with TRIA_FAKE_CLAUDE=stream it speaks
// the stream-json protocol the persistent runner expects (verified against the
// real CLI: init per turn, a per-turn result, an interrupt receipt then an
// aborted result with zero usage). The prompt steers a turn: "sleep" waits for
// an interrupt, "die" exits mid-turn, "authfail" retries a 401 forever.
func TestMain(m *testing.M) {
	if os.Getenv("TRIA_FAKE_CLAUDE") == "stream" {
		fakeClaude()
		return
	}
	os.Exit(m.Run())
}

func fakeClaude() {
	session := fmt.Sprintf("sess-%d", os.Getpid())
	perm := "auto"
	for i, a := range os.Args {
		switch a {
		case "--resume":
			session = os.Args[i+1]
		case "--permission-mode":
			perm = os.Args[i+1]
		}
	}
	out := func(v map[string]any) {
		v["session_id"] = session
		b, _ := json.Marshal(v)
		fmt.Println(string(b))
	}
	lines := make(chan map[string]any)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 1<<20), 32<<20)
		for sc.Scan() {
			var m map[string]any
			if json.Unmarshal(sc.Bytes(), &m) == nil {
				lines <- m
			}
		}
		close(lines)
	}()
	usage := map[string]any{"input_tokens": 100, "output_tokens": 50, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
	zero := map[string]any{"input_tokens": 0, "output_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
	n := 0
	receipt := func(m map[string]any) {
		out(map[string]any{"type": "control_response", "response": map[string]any{
			"subtype": "success", "request_id": m["request_id"], "response": map[string]any{"still_queued": []string{}}}})
	}
	for m := range lines {
		switch m["type"] {
		case "control_request":
			receipt(m) // idle: nothing to abort
		case "user":
			n++
			text, _ := m["message"].(map[string]any)["content"].(string)
			out(map[string]any{"type": "system", "subtype": "init", "model": "fake", "permissionMode": perm,
				"mcp_servers": []map[string]any{{"name": "core", "status": "connected"}}})
			out(map[string]any{"type": "assistant", "message": map[string]any{"id": fmt.Sprintf("m%d", n), "usage": usage}})
			switch text {
			case "die":
				os.Exit(3)
			case "authfail":
				for i := 1; i <= 6; i++ {
					out(map[string]any{"type": "system", "subtype": "api_retry", "attempt": i, "error_status": 401, "error": "authentication_failed"})
				}
				select {} // retries forever until killed
			case "sleep":
				for c := range lines {
					if c["type"] == "control_request" {
						receipt(c)
						out(map[string]any{"type": "result", "subtype": "error_during_execution", "is_error": true,
							"terminal_reason": "aborted_streaming", "num_turns": 1, "result": "", "usage": zero})
						break
					}
				}
				continue
			}
			out(map[string]any{"type": "result", "subtype": "success", "is_error": false, "num_turns": 1,
				"result": fmt.Sprintf("pid=%d n=%d", os.Getpid(), n), "total_cost_usd": 0.01, "usage": usage})
		}
	}
}

func persistentOpts(t *testing.T) Options {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRIA_FAKE_CLAUDE", "stream")
	d := t.TempDir()
	return Options{Bin: exe, Model: "sonnet", MaxTurns: 5, Timeout: 20 * time.Second, Grace: 5 * time.Second, StateDir: filepath.Join(d, "state"),
		Paths:          render.Paths{MCPConfig: filepath.Join(d, "mcp.json"), Settings: filepath.Join(d, "settings.json"), Instruction: filepath.Join(d, "instruction.md")},
		PermissionMode: "auto"}
}

// pid reads the process id the fake reports in its result text.
func pid(t *testing.T, r Result) string {
	t.Helper()
	if r.Err != nil || !r.Summary.GotResult {
		t.Fatalf("turn failed: err=%v result=%+v stderr=%q", r.Err, r.Summary, r.Stderr)
	}
	return strings.Fields(r.Summary.ResultText)[0]
}

func newPersistent(t *testing.T, o Options) *Persistent {
	p := NewPersistent(o)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPersistentKeepsOneProcessAcrossTurns(t *testing.T) {
	p := newPersistent(t, persistentOpts(t))
	ctx := context.Background()
	r1 := p.Run(ctx, "hello", "")
	id1 := pid(t, r1)
	sid := r1.Summary.SessionID
	if sid == "" || !r1.Summary.GotInit || r1.Summary.Tokens().Billable() != 150 {
		t.Fatalf("turn 1 = %+v", r1.Summary)
	}
	if !strings.Contains(strings.Join(r1.Args, " "), "--input-format stream-json") {
		t.Errorf("args lack the streaming input: %v", r1.Args)
	}
	r2 := p.Run(ctx, "again", sid)
	if pid(t, r2) != id1 || !strings.HasSuffix(r2.Summary.ResultText, "n=2") {
		t.Fatalf("turn 2 ran in another process or conversation: %q then %q", r1.Summary.ResultText, r2.Summary.ResultText)
	}
	if r2.Summary.Tokens().Billable() != 150 {
		t.Errorf("a turn's usage is its own, got %+v", r2.Summary.Tokens())
	}
	if r2.Summary.SessionID != sid {
		t.Errorf("session changed: %q -> %q", sid, r2.Summary.SessionID)
	}
}

func TestPersistentReplacesTheProcessWhenTheConversationChanges(t *testing.T) {
	p := newPersistent(t, persistentOpts(t))
	ctx := context.Background()
	r1 := p.Run(ctx, "a", "")
	id1 := pid(t, r1)

	// A rotation asks for a fresh conversation.
	r2 := p.Run(ctx, "b", "")
	id2 := pid(t, r2)
	if id2 == id1 {
		t.Fatal("a fresh conversation must not reuse the process")
	}
	// Another session resumes in a new process, started with that id.
	r3 := p.Run(ctx, "c", "other-session")
	if id3 := pid(t, r3); id3 == id2 {
		t.Fatal("another session must not reuse the process")
	}
	if !strings.Contains(strings.Join(r3.Args, " "), "--resume other-session") {
		t.Errorf("args = %v", r3.Args)
	}
}

func TestPersistentReplacesTheProcessWhenItsConfigurationChanges(t *testing.T) {
	o := persistentOpts(t)
	d := filepath.Dir(o.Paths.MCPConfig)
	o.CoreMCPURL = "http://core.example.invalid/mcp"
	o.CoreTokenFile = filepath.Join(d, "core-token")
	if err := os.WriteFile(o.CoreTokenFile, []byte("token-one\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	p := newPersistent(t, o)
	ctx := context.Background()
	r1 := p.Run(ctx, "a", "")
	id1 := pid(t, r1)
	sid := r1.Summary.SessionID
	if id := pid(t, p.Run(ctx, "b", sid)); id != id1 {
		t.Fatal("same configuration: the process stays")
	}

	// The core token rotated: the MCP file differs, so the process is replaced and resumes.
	if err := os.WriteFile(o.CoreTokenFile+".new", []byte("token-two\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(o.CoreTokenFile+".new", o.CoreTokenFile); err != nil {
		t.Fatal(err)
	}
	r3 := p.Run(ctx, "c", sid)
	if id := pid(t, r3); id == id1 {
		t.Fatal("a rotated token must replace the process")
	}
	if !strings.Contains(strings.Join(r3.Args, " "), "--resume "+sid) {
		t.Errorf("the replacement must resume the session: %v", r3.Args)
	}

	// A permission mode change does too.
	p.SetPermissionMode("dontAsk")
	r4 := p.Run(ctx, "d", sid)
	if r4.Summary.PermissionMode != "dontAsk" {
		t.Errorf("mode = %q", r4.Summary.PermissionMode)
	}
}

func TestPersistentInterruptsATurnThatRunsPastItsTimeout(t *testing.T) {
	o := persistentOpts(t)
	o.Timeout = 300 * time.Millisecond
	p := newPersistent(t, o)
	ctx := context.Background()

	r := p.Run(ctx, "sleep", "")
	if !r.TimedOut || r.Canceled || r.AuthFailed {
		t.Fatalf("flags = timedOut %v canceled %v auth %v", r.TimedOut, r.Canceled, r.AuthFailed)
	}
	if !r.Summary.GotResult || !r.Summary.IsError || r.Summary.TerminalReason != "aborted_streaming" {
		t.Fatalf("an interrupted turn ends with an aborted result: %+v", r.Summary)
	}
	if got := r.Summary.Tokens().Billable(); got != 150 {
		t.Errorf("the aborted result reports zero usage; the assistant message's 150 tokens must still be counted, got %d", got)
	}

	// The process survived the interrupt and takes the next turn.
	o2 := p.r.options()
	o2.Timeout = time.Minute
	p.r.mu.Lock()
	p.r.o = o2
	p.r.mu.Unlock()
	r2 := p.Run(ctx, "after", r.Summary.SessionID)
	pid(t, r2)
	if !strings.HasSuffix(r2.Summary.ResultText, "n=2") {
		t.Errorf("the process should have been kept: %q", r2.Summary.ResultText)
	}
}

func TestPersistentReportsACrashAndStartsAgain(t *testing.T) {
	p := newPersistent(t, persistentOpts(t))
	ctx := context.Background()
	r1 := p.Run(ctx, "ok", "")
	pid(t, r1)
	sid := r1.Summary.SessionID

	r := p.Run(ctx, "die", sid)
	if r.Err == nil || r.ExitCode != 3 || r.Summary.GotResult {
		t.Fatalf("a process that dies mid-turn is a failed turn: err=%v exit=%d %+v", r.Err, r.ExitCode, r.Summary)
	}
	if got := r.Summary.Tokens().Billable(); got != 150 {
		t.Errorf("the tokens it spent before dying still count, got %d", got)
	}
	r3 := p.Run(ctx, "back", sid)
	pid(t, r3)
	if !strings.Contains(strings.Join(r3.Args, " "), "--resume "+sid) {
		t.Errorf("the new process resumes: %v", r3.Args)
	}
}

func TestPersistentStopsAProcessThatKeepsFailingAuthentication(t *testing.T) {
	p := newPersistent(t, persistentOpts(t))
	ctx := context.Background()
	r := p.Run(ctx, "authfail", "")
	if !r.AuthFailed {
		t.Fatalf("auth failure not reported: %+v err=%v", r.Summary, r.Err)
	}
	r2 := p.Run(ctx, "later", "")
	pid(t, r2)
}

func TestPersistentCancelStopsTheTurnWithoutBlaming(t *testing.T) {
	p := newPersistent(t, persistentOpts(t))
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	r := p.Run(ctx, "sleep", "")
	if !r.Canceled || r.TimedOut {
		t.Fatalf("shutdown is not a timeout: canceled=%v timedOut=%v", r.Canceled, r.TimedOut)
	}
}

func TestPersistentCloseStopsTheProcess(t *testing.T) {
	p := NewPersistent(persistentOpts(t))
	pid(t, p.Run(context.Background(), "x", ""))
	pr := p.p
	if pr == nil || !pr.alive() {
		t.Fatal("a process should be running")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if pr.alive() {
		t.Fatal("Close left the process running")
	}
}
