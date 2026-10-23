// Package prompt holds the agent's system prompt and builds each turn's
// wake-up prompt.
package prompt

import (
	_ "embed"
	"fmt"
	"strings"
	"time"
)

//go:embed instruction.md
var instruction string

// Params fill the placeholders in the system prompt.
type Params struct {
	Name     string
	ChatName string // how the messenger shows the spot; empty = Name
	Tenant   string
	StateDir string
	MinWake  time.Duration
	MaxWake  time.Duration
}

// Instruction renders the system prompt.
func Instruction(p Params) string {
	chat := p.ChatName
	if chat == "" {
		chat = p.Name
	}
	return strings.NewReplacer(
		"{{CHAT_NAME}}", chat,
		"{{NAME}}", p.Name,
		"{{TENANT}}", p.Tenant,
		"{{STATE_DIR}}", strings.TrimRight(p.StateDir, "/"),
		"{{MIN_WAKE}}", p.MinWake.String(),
		"{{MAX_WAKE}}", p.MaxWake.String(),
	).Replace(instruction)
}

// Raw returns the unrendered template, for tests.
func Raw() string { return instruction }

// Reason is why the agent was woken.
type Reason string

const (
	Startup Reason = "startup"
	Tick    Reason = "tick"
	Edge    Reason = "edge"
	Human   Reason = "human"
)

// Wake describes one turn.
type Wake struct {
	Reason      Reason
	Now         time.Time
	Facts       []string // edges or messages that caused it
	ScheduledBy string   // the "why" the agent left for itself (tick)
	Resumed     bool     // a conversation is being resumed
	Interrupted bool     // the previous turn was cut off (restart or kill)
	Note        string   // supervisor note, e.g. the schedule was defaulted
	TokensUsed  int64
	TokensCeil  int64 // 0 = no ceiling
	StateDir    string
}

// Turn builds the prompt for a turn.
func Turn(w Wake) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Wake reason: %s\nTime (UTC): %s\n", w.Reason, w.Now.UTC().Format(time.RFC3339))
	switch w.Reason {
	case Startup:
		if w.Resumed {
			b.WriteString("The pod (re)started and your conversation was resumed. Read your notes, then look at the tenant.\n")
		} else {
			b.WriteString("This is your first turn in a new conversation. Read your notes if there are any, then look at the tenant.\n")
		}
	case Tick:
		if w.ScheduledBy != "" {
			fmt.Fprintf(&b, "You scheduled this wake. Your reason: %s\n", w.ScheduledBy)
		} else {
			b.WriteString("You scheduled this wake (or the schedule was defaulted).\n")
		}
	case Edge:
		b.WriteString("The core changed while you slept:\n")
	case Human:
		b.WriteString("Someone wrote to you:\n")
	}
	for _, f := range w.Facts {
		fmt.Fprintf(&b, "- %s\n", f)
	}
	if w.Interrupted {
		b.WriteString("Your previous turn was cut off before it ended. Check your notes and the cluster for a half-finished change before you continue.\n")
	}
	if w.Note != "" {
		fmt.Fprintf(&b, "Supervisor note: %s\n", w.Note)
	}
	if w.TokensCeil > 0 {
		left := w.TokensCeil - w.TokensUsed
		if left < 0 {
			left = 0
		}
		fmt.Fprintf(&b, "Tokens today: %d used, %d left of %d.\n", w.TokensUsed, left, w.TokensCeil)
	} else {
		fmt.Fprintf(&b, "Tokens today: %d used (no ceiling set).\n", w.TokensUsed)
	}
	fmt.Fprintf(&b, "End this turn by writing %s/next.json.\n", strings.TrimRight(w.StateDir, "/"))
	return b.String()
}
