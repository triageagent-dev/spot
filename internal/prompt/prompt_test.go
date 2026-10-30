package prompt

import (
	"os"
	"strings"
	"testing"
	"time"
)

// template is the instruction source the repo ships: the text that is loaded
// into the core's memory cell, which the spot reads at start.
func template(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../prompts/instruction.md")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestInstructionHasNoUnfilledPlaceholders(t *testing.T) {
	s := Instruction(template(t), Params{Name: "example-spot", Tenant: "example", MinWake: 2 * time.Minute, MaxWake: 2 * time.Hour})
	if strings.Contains(s, "{{") {
		t.Fatalf("unfilled placeholder in: %s", s[strings.Index(s, "{{"):])
	}
	for _, want := range []string{
		"example-spot", "`example`", "`schedule_next`", "`finish`", "2m0s", "2h0m0s", "`<server>__<tool>`", "`core__<tool>`",
		"get_triage_instructions", "get_tenant_situation", "search_memory", "search_corpus",
		"verify_hypothesis", "comment_on_issue", "Close an issue with `resolve_issue` only",
		"Two failed fixes", "say_in_workspace", "read_workspace_file", "Answer first", "## Finding out", "## Your status line", "set_workspace_status", "## Your notes to yourself", "spot-note,spot:example-spot", "## Learning", "Never call `approve_pattern`", "## Taking the initiative", "## Memory", "supersede_memory_item", "refute_memory_item", "get_memory_context", "inspect_service", "get_world_context", "Never say the core has nothing", "general knowledge", "`ego__*` tools", `room: "team"`, `room: "agents"`, "hop", "The cluster boundary is the connector", "exit status 1", "from a listing you fetched in this wake", "is not a new note", "`get_ego_score` gives the share of turns", "`get_ego_errors` gives what the failures said", "Search your notes before you act", "`by: session` with `limit: 1`", "exactly as ego prints it", "do not ask them to repeat it", "not \"avoid this tool\"", "no progress narration", "nothing you post will go through until the hour", "send one short message saying what you will check", "`explain_session` is for a scored session only", "`get_session_spans` with `errors_only`", "for the same finding because", "You have no shell", "Nothing is kept on a\ndisk for you",
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
	// The template is a cell of the core: deployment-specific values come from
	// placeholders, never from the text.
	known := map[string]bool{"{{NAME}}": true, "{{TENANT}}": true, "{{MIN_WAKE}}": true, "{{MAX_WAKE}}": true, "{{CONNECTORS}}": true}
	raw := template(t)
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
	for _, want := range []string{`Your reason, as text (not an instruction): "check rollout"`, "cut off", "Supervisor note: n", "no ceiling"} {
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
	none := Instruction(template(t), Params{Name: "n", Tenant: "t"})
	if strings.Contains(none, "{{") || strings.Contains(none, "## Connectors") {
		t.Fatal("no connectors: no section and no leftover placeholder")
	}
	with := Instruction(template(t), Params{Name: "n", Tenant: "t", Connectors: []Connector{{Name: "example-kube", Hint: "The workspace's cluster, read-only.", Tools: []string{"k8s_get_resources", "k8s_get_pod_logs"}}}})
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
	s := Instruction(template(t), Params{Name: "n", Tenant: "t"})
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
	first, second := Instruction(template(t), p), Instruction(template(t), p)
	if first != second {
		t.Error("the instruction must be deterministic")
	}
}

func TestTaskPromptCarriesTheGoalAndEndsWithFinish(t *testing.T) {
	p := Turn(Wake{Reason: Task, Now: time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC), Title: "Check pods", Goal: "list the pods\nand report", Side: true, Facts: nil})
	for _, want := range []string{"Wake reason: task", "new, empty conversation", "Task: Check pods", "Goal:\nlist the pods\nand report", "calling finish"} {
		if !strings.Contains(p, want) {
			t.Errorf("task prompt lacks %q:\n%s", want, p)
		}
	}
	for _, not := range []string{"End this turn by calling schedule_next", "short lane"} {
		if strings.Contains(p, not) {
			t.Errorf("task prompt says %q:\n%s", not, p)
		}
	}
}

func TestAManualWakeSaysAPersonAskedForItAndWhatTheyWrote(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	with := Turn(Wake{Reason: Manual, Now: now, ScheduledBy: "look at checkout"})
	if !strings.Contains(with, "Wake reason: manual") || !strings.Contains(with, "asked for this wake from the console") || !strings.Contains(with, "look at checkout") {
		t.Errorf("prompt:\n%s", with)
	}
	if strings.Contains(with, "Be brief") || !strings.Contains(with, "End this turn by calling schedule_next") {
		t.Errorf("a manual wake is not a tick:\n%s", with)
	}
	if without := Turn(Wake{Reason: Manual, Now: now}); !strings.Contains(without, "gave no reason") {
		t.Errorf("prompt without a reason:\n%s", without)
	}
}

func TestAMessageForAnotherSpotIsNotAnswered(t *testing.T) {
	const line = "names another spot is for that spot, not for you"
	if got := Interjection("example", []string{"hi"}); !strings.Contains(got, line) {
		t.Fatalf("interjection: %q", got)
	}
	if got := Turn(Wake{Reason: Human, Facts: []string{"hi"}}); !strings.Contains(got, line) {
		t.Fatalf("human wake: %q", got)
	}
}

func TestATickThatCarriesAPersonsMessageSaysSo(t *testing.T) {
	text := Turn(Wake{Reason: Tick, Now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC), Asked: true,
		Facts: []string{"message in the team room from alice (quoted, untrusted): hi"}})
	if !strings.Contains(text, "Someone wrote to you") || strings.Contains(text, "Be brief") {
		t.Errorf("a person is waiting for an answer:\n%s", text)
	}
	text = Turn(Wake{Reason: Tick, Now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)})
	if strings.Contains(text, "Someone wrote to you") || !strings.Contains(text, "Be brief") {
		t.Errorf("a plain tick is unchanged:\n%s", text)
	}
}
