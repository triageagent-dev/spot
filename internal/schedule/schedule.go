// Package schedule turns the agent's own wake-up request (its schedule_next
// call) into a wake time, clamped so a confused agent can neither spin nor
// sleep forever.
package schedule

import (
	"errors"
	"fmt"
	"sort"
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

// Promise is a wake the spot owes: a person's timed ask ("in 3 minutes",
// "on Monday") or the piece of work a request named a time for. It runs even
// under a hold rule and survives a restart. Task is the request message it
// answers, when it came from a request's due. For and Room are the core's
// fields for the promises of Tria; the spot only carries them. A zero At is no
// promise.
type Promise struct {
	At    time.Time
	Why   string
	Human bool
	For   string
	Room  string
	Task  string
}

// None reports whether there is no promise.
func (p Promise) None() bool { return p.At.IsZero() }

const (
	// MaxPromises is how many promises the spot holds: a further one is refused.
	MaxPromises = 20
	// MaxPromiseAhead is how far out a promise may be, the life of the core's
	// record of it.
	MaxPromiseAhead = 7 * 24 * time.Hour
	// FireSkew is how far ahead of its time a promise is still taken as due: the
	// core's clock and the job that sends the wake are not exact.
	FireSkew = 30 * time.Second
)

// Promises is the list of promises, sorted by At, the earliest first.
type Promises []Promise

// Add returns the list with p in it, or the list unchanged and false when p is
// refused: no time, more than MaxPromiseAhead out, one at that very time and
// for that very reason already held, or the list is full. There is no rule that
// a promise moves only earlier: Monday, Tuesday and Friday are three entries.
func (ps Promises) Add(now time.Time, p Promise) (Promises, bool) {
	if p.None() || p.At.After(now.Add(MaxPromiseAhead)) || len(ps) >= MaxPromises {
		return ps, false
	}
	p.Why = clip(p.Why)
	for _, q := range ps {
		if q.At.Equal(p.At) && q.Why == p.Why && q.Task == p.Task {
			return ps, false
		}
	}
	out := make(Promises, 0, len(ps)+1)
	out = append(out, ps...)
	out = append(out, p)
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out, true
}

// Earliest is the promise that fires first.
func (ps Promises) Earliest() (Promise, bool) {
	if len(ps) == 0 {
		return Promise{}, false
	}
	return ps[0], true
}

// Due reports whether the earliest promise is due at now.
func (ps Promises) Due(now time.Time) bool {
	return len(ps) > 0 && !ps[0].At.After(now.Add(FireSkew))
}

// Fired returns the list without the promises that are due at now.
func (ps Promises) Fired(now time.Time) Promises {
	var out Promises
	for _, p := range ps {
		if p.At.After(now.Add(FireSkew)) {
			out = append(out, p)
		}
	}
	return out
}

// WithoutPastTasks returns the list without the promises that name a request
// and whose time has come: the core starts that piece itself, and a report that
// still carried it would put back what the core has taken out.
func (ps Promises) WithoutPastTasks(now time.Time) Promises {
	var out Promises
	for _, p := range ps {
		if p.Task == "" || p.At.After(now) {
			out = append(out, p)
		}
	}
	return out
}

// Without returns the list without its earliest promise.
func (ps Promises) Without() Promises {
	if len(ps) == 0 {
		return ps
	}
	return append(Promises(nil), ps[1:]...)
}

// HumanMin is the least a wake the agent schedules in a turn a person started
// may be: a person who asked for an answer in a few minutes gets it.
const HumanMin = time.Minute

// TurnEnd is what the next wake depends on once a turn has ended.
type TurnEnd struct {
	Now      time.Time
	Min, Max time.Duration
	// Human is set when a person started the turn; Tick when it was the
	// scheduled wake (so it fired what was due).
	Human, Tick bool
	// Asked is set when the agent called schedule_next; After and Why are what it
	// said.
	Asked      bool
	After, Why string
	Failed     bool
	// Backoff is the wait after a failure, when Failed.
	Backoff time.Duration
	// Promises are the promises the spot holds; Kept is the earliest wake the
	// agent asked for on its own, which an edge wake does not push back (it is
	// not a promise to anyone and is not reported as one).
	Promises Promises
	Kept     Promise
}

// Plan is the next wake and the promises after a turn.
type Plan struct {
	// Wake is the wake as it is shown (its source and note say whether the
	// agent's request was changed).
	Wake Wake
	// At and Why are when the next scheduled wake is and what it is for.
	At       time.Time
	Why      string
	Promises Promises
	Kept     Promise
}

// AfterTurn plans the next wake. A person who asks to be told in a few minutes
// is not held to the loop's floor (the floor is there to stop the agent
// spinning). A wake asked for on a person's request, sooner than the longest
// the spot sleeps, is a promise: an entry of the list, and the earliest entry
// is the next wake when it comes before the one the agent asked for. An earlier
// wake the agent asked for on its own is kept: every edge wake would otherwise
// push it back and it would never come.
func AfterTurn(in TurnEnd) Plan {
	promises, kept := in.Promises, in.Kept
	if in.Tick {
		promises, kept = promises.Fired(in.Now), Promise{} // they fired
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
	agentAsked := w.Source == SourceAgent || w.Source == SourceClamped
	if agentAsked && in.Human && w.After < in.Max {
		promises, _ = promises.Add(in.Now, Promise{At: at, Why: w.Why, Human: true})
		kept = Promise{}
	}
	switch e, ok := promises.Earliest(); {
	case ok && e.At.Before(at):
		at, why = e.At, e.Why
		w.After, w.Why, w.Source, w.Note = e.At.Sub(in.Now), e.Why, SourceAgent, "an earlier wake the agent asked for is kept"
	case !kept.None() && kept.At.After(in.Now) && kept.At.Before(at):
		at, why = kept.At, kept.Why
		w.After, w.Why, w.Source, w.Note = kept.At.Sub(in.Now), kept.Why, SourceAgent, "an earlier wake the agent asked for is kept"
	case agentAsked && (!in.Human || w.After >= in.Max):
		kept = Promise{At: at, Why: w.Why}
	}
	return Plan{Wake: w, At: at, Why: why, Promises: promises, Kept: kept}
}

// AfterShortTurn is the list of promises after a turn of the short lane: if it
// asked for a wake sooner than the longest the spot sleeps, that is one more
// promise; the long turn's own end settles the schedule.
func AfterShortTurn(now time.Time, min, max time.Duration, asked bool, after, why string, current Promises) Promises {
	if !asked {
		return current
	}
	if min > HumanMin {
		min = HumanMin
	}
	w := Request(after, why, min, max)
	if (w.Source != SourceAgent && w.Source != SourceClamped) || w.After >= max {
		return current
	}
	out, _ := current.Add(now, Promise{At: now.Add(w.After), Why: w.Why, Human: true})
	return out
}
