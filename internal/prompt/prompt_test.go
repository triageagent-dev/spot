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
		"verify_hypothesis", "comment_on_issue", "Never call `resolve_issue`",
		"Two failed fixes", "say_in_workspace", "read_workspace_file", `room: "team"`, `room: "agents"`, "hop", "ServiceAccount",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("instruction lacks %q", want)
		}
	}
}

func TestTemplateHoldsOnlyKnownPlaceholders(t *testing.T) {
	// The template ships in the binary: deployment-specific values come from
	// placeholders, never from the text.
	known := map[string]bool{"{{NAME}}": true, "{{TENANT}}": true, "{{STATE_DIR}}": true, "{{MIN_WAKE}}": true, "{{MAX_WAKE}}": true}
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
	if !strings.Contains(Turn(Wake{Reason: Startup, Resumed: true, Now: now, StateDir: "/s"}), "resumed") {
		t.Error("resumed startup not mentioned")
	}
}
