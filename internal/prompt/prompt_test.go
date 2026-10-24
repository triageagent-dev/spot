package prompt

import (
	"strings"
	"testing"
	"time"
)

func TestInstructionHasNoUnfilledPlaceholders(t *testing.T) {
	s := Instruction(Params{Name: "example-spot", Tenant: "example", StateDir: "/state/", MinWake: 2 * time.Minute, MaxWake: 2 * time.Hour})
	if strings.Contains(s, "{{") {
		t.Fatalf("unfilled placeholder in: %s", s[strings.Index(s, "{{"):])
	}
	for _, want := range []string{
		"example-spot", "`example`", "/state/next.json", "/state/notes/", "2m0s", "2h0m0s",
		"get_triage_instructions", "get_tenant_situation", "search_memory", "search_corpus",
		"verify_hypothesis", "comment_on_issue", "Close an issue with `resolve_issue` only",
		"Two failed fixes", "say_in_workspace", "read_workspace_file", "Answer first", "## Finding out", "## Your status line", "set_spot_status", "## Your notes to yourself", "spot-note,spot:example-spot", "## Learning", "Never call `approve_pattern`", "## Taking the initiative", "## Memory", "supersede_memory_item", "refute_memory_item", "get_memory_context", "inspect_service", "get_world_context", "Never say the core has nothing", "general knowledge", "mcp__ego__*", `room: "team"`, `room: "agents"`, "hop", "The cluster boundary is the connector", "You have no shell",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("instruction lacks %q", want)
		}
	}
}

func TestWakePromptSaysWhatTheMessengerCallsTheSpot(t *testing.T) {
	now := time.Date(2026, 10, 23, 8, 0, 0, 0, time.UTC)
	p := Turn(Wake{Reason: Human, Now: now, ChatName: "Scout", StateDir: "/state"})
	if !strings.Contains(p, "In the messenger you appear as Scout") {
		t.Errorf("wake prompt lacks the messenger name:\n%s", p)
	}
	if p := Turn(Wake{Reason: Human, Now: now, StateDir: "/state"}); strings.Contains(p, "In the messenger you appear as") {
		t.Errorf("an unknown name was stated:\n%s", p)
	}
}

func TestTemplateHoldsOnlyKnownPlaceholders(t *testing.T) {
	// The template ships in the binary: deployment-specific values come from
	// placeholders, never from the text.
	known := map[string]bool{"{{NAME}}": true, "{{TENANT}}": true, "{{STATE_DIR}}": true, "{{MIN_WAKE}}": true, "{{MAX_WAKE}}": true, "{{CONNECTORS}}": true, "{{SHELL}}": true}
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
	p := Turn(Wake{Reason: Edge, Now: now, Facts: []string{"new issue a (critical): boom"}, TokensUsed: 100, TokensCeil: 1000, StateDir: "/state"})
	for _, want := range []string{"Wake reason: edge", "2026-10-21T08:00:00Z", "- new issue a (critical): boom", "100 used, 900 left of 1000", "/state/next.json"} {
		if !strings.Contains(p, want) {
			t.Errorf("turn prompt lacks %q:\n%s", want, p)
		}
	}
	p = Turn(Wake{Reason: Tick, Now: now, ScheduledBy: "check rollout", StateDir: "/state", Interrupted: true, Note: "n"})
	for _, want := range []string{"Your reason: check rollout", "cut off", "Supervisor note: n", "no ceiling"} {
		if !strings.Contains(p, want) {
			t.Errorf("tick prompt lacks %q:\n%s", want, p)
		}
	}
	for _, r := range []Reason{Startup, Tick, Edge, Human} {
		if !strings.Contains(Turn(Wake{Reason: r, Now: now, StateDir: "/s"}), "new, empty conversation") {
			t.Errorf("%s: the prompt must say the conversation is empty", r)
		}
	}
}

func TestInstructionListsConnectorsOnlyWhenThereAreSome(t *testing.T) {
	none := Instruction(Params{Name: "n", Tenant: "t", StateDir: "/state"})
	if strings.Contains(none, "{{") || strings.Contains(none, "## Connectors") {
		t.Fatal("no connectors: no section and no leftover placeholder")
	}
	with := Instruction(Params{Name: "n", Tenant: "t", StateDir: "/state", Connectors: []Connector{{Name: "example-kube", Hint: "The workspace's cluster, read-only.", Tools: []string{"k8s_get_resources", "k8s_get_pod_logs"}}}})
	if !strings.Contains(with, "## Connectors") || !strings.Contains(with, "`example-kube`: The workspace's cluster, read-only.") {
		t.Fatalf("the connector and its hint belong in the prompt:\n%s", with)
	}
	for _, want := range []string{"`mcp__example-kube__k8s_get_resources`", "`mcp__example-kube__k8s_get_pod_logs`", "and no others"} {
		if !strings.Contains(with, want) {
			t.Fatalf("the prompt must name the exact tool %q:\n%s", want, with)
		}
	}
	if strings.Contains(with, "mcp__example-kube__*") {
		t.Fatalf("the prompt must not promise every tool of the connector:\n%s", with)
	}
}

func TestInstructionSaysWhatTheShellIs(t *testing.T) {
	none := Instruction(Params{Name: "n", Tenant: "t", StateDir: "/state"})
	if !strings.Contains(none, "You have no shell") || strings.Contains(none, "{{") {
		t.Fatalf("without the sandbox the prompt says there is no shell:\n%s", none)
	}
	with := Instruction(Params{Name: "n", Tenant: "t", StateDir: "/state", Sandbox: true})
	if strings.Contains(with, "You have no shell") || !strings.Contains(with, "Your Bash tool is a sandbox") || !strings.Contains(with, "Write `next.json`") || strings.Contains(with, "{{") {
		t.Fatalf("with the sandbox the prompt describes it:\n%s", with)
	}
}
