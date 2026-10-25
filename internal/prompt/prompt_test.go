package prompt

import (
	"strings"
	"testing"
	"time"
)

func TestInstructionHasNoUnfilledPlaceholders(t *testing.T) {
	s := Instruction(Params{Name: "example-spot", Tenant: "example", MinWake: 2 * time.Minute, MaxWake: 2 * time.Hour})
	if strings.Contains(s, "{{") {
		t.Fatalf("unfilled placeholder in: %s", s[strings.Index(s, "{{"):])
	}
	for _, want := range []string{
		"example-spot", "`example`", "`schedule_next`", "`finish`", "2m0s", "2h0m0s", "`<server>__<tool>`", "`core__<tool>`",
		"get_triage_instructions", "get_tenant_situation", "search_memory", "search_corpus",
		"verify_hypothesis", "comment_on_issue", "Close an issue with `resolve_issue` only",
		"Two failed fixes", "say_in_workspace", "read_workspace_file", "Answer first", "## Finding out", "## Your status line", "set_workspace_status", "## Your notes to yourself", "spot-note,spot:example-spot", "## Learning", "Never call `approve_pattern`", "## Taking the initiative", "## Memory", "supersede_memory_item", "refute_memory_item", "get_memory_context", "inspect_service", "get_world_context", "Never say the core has nothing", "general knowledge", "`ego__*` tools", `room: "team"`, `room: "agents"`, "hop", "The cluster boundary is the connector", "exit status 1", "from a listing you fetched in this wake", "is not a new note", "`explain_session` is for a scored session only", "`get_session_spans` with `errors_only`", "for the same finding because", "You have no shell", "Nothing is kept on a\ndisk for you",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("instruction lacks %q", want)
		}
	}
}

func TestWakePromptSaysWhatTheMessengerCallsTheSpot(t *testing.T) {
	now := time.Date(2026, 10, 23, 8, 0, 0, 0, time.UTC)
	p := Turn(Wake{Reason: Human, Now: now, ChatName: "Scout"})
	if !strings.Contains(p, "In the messenger you appear as Scout") {
		t.Errorf("wake prompt lacks the messenger name:\n%s", p)
	}
	if p := Turn(Wake{Reason: Human, Now: now}); strings.Contains(p, "In the messenger you appear as") {
		t.Errorf("an unknown name was stated:\n%s", p)
	}
}

func TestTemplateHoldsOnlyKnownPlaceholders(t *testing.T) {
	// The template ships in the binary: deployment-specific values come from
	// placeholders, never from the text.
	known := map[string]bool{"{{NAME}}": true, "{{TENANT}}": true, "{{MIN_WAKE}}": true, "{{MAX_WAKE}}": true, "{{CONNECTORS}}": true}
	raw := Raw()
	for {
		i := strings.Index(raw, "{{")
		if i < 0 {
			break
		}
		j := strings.Index(raw[i:], "}}")
		if j < 0 {
			t.Fatal("unterminated placeholder")
		}
		if ph := raw[i : i+j+2]; !known[ph] {
			t.Errorf("unknown placeholder %s", ph)
		}
		raw = raw[i+j+2:]
	}
}

func TestTurnPrompt(t *testing.T) {
	now := time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)
	p := Turn(Wake{Reason: Edge, Now: now, Facts: []string{"new issue a (critical): boom"}, TokensUsed: 100, TokensCeil: 1000})
	for _, want := range []string{"Wake reason: edge", "2026-10-21T08:00:00Z", "- new issue a (critical): boom", "100 used, 900 left of 1000", "schedule_next"} {
		if !strings.Contains(p, want) {
			t.Errorf("turn prompt lacks %q:\n%s", want, p)
		}
	}
	p = Turn(Wake{Reason: Tick, Now: now, ScheduledBy: "check rollout", Interrupted: true, Note: "n"})
	for _, want := range []string{"Your reason: check rollout", "cut off", "Supervisor note: n", "no ceiling"} {
		if !strings.Contains(p, want) {
			t.Errorf("tick prompt lacks %q:\n%s", want, p)
		}
	}
	for _, r := range []Reason{Startup, Tick, Edge, Human} {
		if !strings.Contains(Turn(Wake{Reason: r, Now: now}), "new, empty conversation") {
			t.Errorf("%s: the prompt must say the conversation is empty", r)
		}
	}
}

func TestInstructionListsConnectorsOnlyWhenThereAreSome(t *testing.T) {
	none := Instruction(Params{Name: "n", Tenant: "t"})
	if strings.Contains(none, "{{") || strings.Contains(none, "## Connectors") {
		t.Fatal("no connectors: no section and no leftover placeholder")
	}
	with := Instruction(Params{Name: "n", Tenant: "t", Connectors: []Connector{{Name: "example-kube", Hint: "The workspace's cluster, read-only.", Tools: []string{"k8s_get_resources", "k8s_get_pod_logs"}}}})
	if !strings.Contains(with, "## Connectors") || !strings.Contains(with, "`example-kube`: The workspace's cluster, read-only.") {
		t.Fatalf("the connector and its hint belong in the prompt:\n%s", with)
	}
	for _, want := range []string{"`example-kube__k8s_get_resources`", "`example-kube__k8s_get_pod_logs`", "and no others"} {
		if !strings.Contains(with, want) {
			t.Fatalf("the prompt must name the exact tool %q:\n%s", want, with)
		}
	}
	if strings.Contains(with, "example-kube__*") {
		t.Fatalf("the prompt must not promise every tool of the connector:\n%s", with)
	}
}

func TestInstructionHasNoFilesNoShellAndNoCLI(t *testing.T) {
	s := Instruction(Params{Name: "n", Tenant: "t"})
	for _, banned := range []string{"next.json", "/state", "Write tool", "Edit tool", "mcp__", "Bash", "sandbox", "--allowedTools", "{{"} {
		if strings.Contains(s, banned) {
			t.Errorf("the instruction must not mention %q", banned)
		}
	}
	if !strings.Contains(s, "You have no shell") {
		t.Error("the instruction must say there is no shell")
	}
	if strings.Contains(Turn(Wake{Reason: Startup, Now: time.Now()}), "notes files") {
		t.Error("the wake prompt must not send the agent to files")
	}
}

func TestInstructionIsTheSameForEveryWake(t *testing.T) {
	// It is the cached prefix of every request: nothing in it may depend on
	// the time or on the wake.
	p := Params{Name: "n", Tenant: "t", MinWake: time.Minute, MaxWake: time.Hour}
	first, second := Instruction(p), Instruction(p)
	if first != second {
		t.Error("the instruction must be deterministic")
	}
}
