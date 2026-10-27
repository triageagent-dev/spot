// Package durable runs the spot on Temporal: one long-running workflow per spot
// is the scheduler (hold rule, promised wakes, the tick, the two lanes), a child
// workflow per wake is one turn of the agent loop, and the model calls, the tool
// calls and everything that touches the world are activities. A crash or a
// restart resumes a turn at its step; a promised wake is a durable timer.
//
// The intake (the core's trigger stream, the polls, the held edges) stays in
// the supervisor and tells the workflow with signals; the activities call back
// into it through Host, in the same process.
package durable

import (
	"context"
	"time"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/tools"
)

// Names of the signals and the query of the spot workflow.
const (
	SignalWake = "wake"
	QueryState = "state"
)

// Lanes: the long one runs the scheduled, startup and edge wakes, one at a time;
// the short one a person's message and a wake a person asked for, one at a time
// and beside the long one.
const (
	LaneLong  = "long"
	LaneShort = "short"
)

// Wake reasons, as the prompt names them.
const (
	ReasonStartup = "startup"
	ReasonTick    = "tick"
	ReasonEdge    = "edge"
	ReasonHuman   = "human"
)

// WakeSignal is what the intake tells the spot workflow: something wants the
// agent. Seq names the wake in the intake, which keeps the edges it was made
// of, in case the wake fails before the model sees it.
type WakeSignal struct {
	Seq    int64
	Reason string
	Facts  []string
	Room   string
}

// PrepareIn asks the process to ready a wake: read the standing notes, drop the
// facts that are stale, build the prompt.
type PrepareIn struct {
	Key     string // names the wake in the workflow
	Lane    string
	Reason  string
	Seqs    []int64
	Facts   []string
	Room    string
	Why     string // what the agent left for itself, for a tick
	Promise bool   // this is the wake a person asked for
	// PromisePending is set while a wake a person asked for has not fired.
	PromisePending bool
}

// PrepareOut is what the wake needs, read from the process's own settings so a
// changed flag takes effect at the next wake.
type PrepareOut struct {
	// Paused is set when the daily ceiling (or the core's cap) holds the agent
	// back: no wake runs until Until.
	Paused bool
	Until  time.Time
	// Hold is set when a standing rule says to check nothing until a person asks:
	// the wake ends here, without a model call.
	Hold bool
	// Quiet is set when the wake has nothing to work on (no facts, no open issue,
	// nobody asked for it): it ends here, without a model call.
	Quiet   bool
	Limited bool
	Prompt  string
	Caps    agent.Caps
	// Min and Max are the shortest and longest wait between wakes.
	Min, Max time.Duration
	// Turn is how long a turn may last; Tool how long one tool call may.
	Turn, Tool time.Duration
	Typing     bool
	Model      string
}

// WakeIn starts a turn.
type WakeIn struct {
	// SpotID and SpotRun are the workflow that started this wake, for the trace.
	SpotID, SpotRun string
	Key             string
	Reason          string
	Lane            string
	Prompt          string
	Room            string
	Limited         bool
	Caps            agent.Caps
	Tool            time.Duration
	Turn            time.Duration
	Typing          bool
	Model           string
}

// WakeOut is what a turn did: the counts and flags of agent.Result, the
// schedule the model asked for and what it said at the end.
type WakeOut struct {
	Text    string
	Summary string
	Next    *tools.ScheduleRequest

	Steps, ToolCalls int
	Input, Output    int64
	CacheRead        int64
	CacheWrite       int64
	ContextTokens    int64
	Model            string
	Seconds          float64

	StepsExhausted, ToolLimit, TokenLimit bool
	TimedOut, AuthFailed, CoreDown        bool
	Failed                                bool
	Err                                   string
}

// AccountIn books a turn that ended.
type AccountIn struct {
	Key    string
	Lane   string
	Reason string
	Out    WakeOut
}

// AccountOut is what the workflow needs to plan the next wake.
type AccountOut struct {
	Failed   bool
	Failures int
	// Backoff is the wait after the failures so far.
	Backoff time.Duration
}

// AnnounceIn tells the process the next wake, once the workflow planned it, so
// the status, the digest and the heartbeat say it.
type AnnounceIn struct {
	Key     string
	Lane    string
	Reason  string
	Held    bool // the wake ended at a hold rule, without a turn
	Failed  bool
	At      time.Time
	Why     string
	After   time.Duration
	Source  string
	Note    string
	Summary string
	Idle    bool // nothing else is running
}

// Host is the process the activities call back into: the supervisor's intake.
type Host interface {
	Prepare(in PrepareIn) (PrepareOut, error)
	Account(in AccountIn) (AccountOut, error)
	Announce(in AnnounceIn) error
	// Typing shows or clears the "typing…" indicator in a room.
	Typing(ctx context.Context, room string, on bool) error
}

// SpotState is what the spot workflow answers to a query.
type SpotState struct {
	Wakes   int
	Open    int
	Long    bool // a long wake is running
	Short   bool
	Queued  int
	Next    time.Time
	NextWhy string
	Promise time.Time
	Held    int // wakes that ended at a hold rule
	Tokens  int64
	Log     []string
}
