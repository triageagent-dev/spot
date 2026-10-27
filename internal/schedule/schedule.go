// Package schedule turns the agent's own wake-up request (its schedule_next
// call) into a wake time, clamped so a confused agent can neither spin nor
// sleep forever.
package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Source says where a Wake came from.
type Source string

const (
	SourceAgent   Source = "agent"           // taken as asked
	SourceClamped Source = "agent-clamped"   // asked for, then moved into [min,max]
	SourceMissing Source = "default-missing" // no request: min-wake
	SourceGarbled Source = "default-garbled" // unusable request: min-wake
)

// Wake is the delay and reason for the next scheduled turn.
type Wake struct {
	After  time.Duration
	Why    string
	Source Source
	Note   string // why it was defaulted or clamped, for the log
}

// maxWhy bounds the reason carried into prompts, the status file and the
// heartbeat.
const maxWhy = 400

// Request turns what the agent asked for (a Go duration string and a reason)
// into a Wake clamped to [min,max]. It never fails: an unusable request yields
// min with SourceGarbled.
func Request(after, why string, min, max time.Duration) Wake {
	d, err := parseAfter(after)
	if err != nil {
		return Wake{After: min, Why: clip(why), Source: SourceGarbled, Note: err.Error()}
	}
	w := Wake{After: d, Why: clip(why), Source: SourceAgent}
	switch {
	case d < min:
		w.After, w.Source, w.Note = min, SourceClamped, fmt.Sprintf("requested %s, raised to min-wake %s", d, min)
	case d > max:
		w.After, w.Source, w.Note = max, SourceClamped, fmt.Sprintf("requested %s, lowered to max-wake %s", d, max)
	}
	return w
}

// Default is the wake used when the agent made no usable request.
func Default(min time.Duration, src Source, note string) Wake {
	return Wake{After: min, Source: src, Note: note}
}

func parseAfter(s string) (time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		return 0, errors.New(`schedule_next has no "after"`)
	}
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf(`"after" %q is not a duration: %v`, s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf(`"after" %q is not positive`, s)
	}
	return d, nil
}

func clip(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxWhy {
		s = s[:maxWhy]
	}
	return s
}

// Promise is the earliest wake the agent itself asked for that has not fired.
// A later turn that wants a later (or no) wake does not move it. Human is set
// when it answers a person's request for later: such a wake runs even under a
// hold rule. A zero At is no promise.
type Promise struct {
	At    time.Time
	Why   string
	Human bool
}

// None reports whether there is no promise.
func (p Promise) None() bool { return p.At.IsZero() }

// HumanMin is the least a wake the agent schedules in a turn a person started
// may be: a person who asked for an answer in a few minutes gets it.
const HumanMin = time.Minute

// TurnEnd is what the next wake depends on once a turn has ended.
type TurnEnd struct {
	Now      time.Time
	Min, Max time.Duration
	// Human is set when a person started the turn; Tick when it was the
	// scheduled wake (so it fired the promise).
	Human, Tick bool
	// Asked is set when the agent called schedule_next; After and Why are what it
	// said.
	Asked      bool
	After, Why string
	Failed     bool
	// Backoff is the wait after a failure, when Failed.
	Backoff time.Duration
	Promise Promise
}

// Plan is the next wake and the promise after a turn.
type Plan struct {
	// Wake is the wake as it is shown (its source and note say whether the
	// agent's request was changed).
	Wake Wake
	// At and Why are when the next scheduled wake is and what it is for.
	At      time.Time
	Why     string
	Promise Promise
}

// AfterTurn plans the next wake. A person who asks to be told in a few minutes
// is not held to the loop's floor (the floor is there to stop the agent
// spinning). An earlier wake the agent promised itself is kept: every edge wake
// would otherwise push it back and it would never come. A wake asked for on a
// person's request, sooner than the longest the spot sleeps, is a promise; an
// idle schedule is not.
func AfterTurn(in TurnEnd) Plan {
	promise := in.Promise
	if in.Tick {
		promise = Promise{} // it fired
	}
	var w Wake
	if in.Asked {
		floor := in.Min
		if in.Human && floor > HumanMin {
			floor = HumanMin
		}
		w = Request(in.After, in.Why, floor, in.Max)
	} else {
		w = Default(in.Min, SourceMissing, "the agent did not call schedule_next")
	}
	if in.Failed && in.Backoff > w.After {
		w.After = in.Backoff
	}
	at, why := in.Now.Add(w.After), w.Why
	switch {
	case !promise.None() && (promise.At.After(in.Now) || promise.Human) && promise.At.Before(at):
		at, why = promise.At, promise.Why
		w.After, w.Why, w.Source, w.Note = promise.At.Sub(in.Now), promise.Why, SourceAgent, "an earlier wake the agent asked for is kept"
	case w.Source == SourceAgent || w.Source == SourceClamped:
		promise = Promise{At: at, Why: w.Why, Human: in.Human && w.After < in.Max}
	}
	return Plan{Wake: w, At: at, Why: why, Promise: promise}
}

// AfterShortTurn is the promise after a turn of the short lane: it asked for a
// wake or it did not, and the long turn's own end settles the schedule. The
// promise moves only to an earlier time.
func AfterShortTurn(now time.Time, min, max time.Duration, asked bool, after, why string, current Promise) Promise {
	if !asked {
		return current
	}
	if min > HumanMin {
		min = HumanMin
	}
	w := Request(after, why, min, max)
	if w.Source != SourceAgent && w.Source != SourceClamped {
		return current
	}
	at := now.Add(w.After)
	if current.None() || at.Before(current.At) {
		return Promise{At: at, Why: w.Why, Human: w.After < max}
	}
	return current
}
