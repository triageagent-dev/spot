package durable

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/triageagent-dev/spot/internal/schedule"
)

// Carry is what a spot workflow hands to the next run of itself (continue as
// new) so a long-lived spot does not grow its history for ever.
type Carry struct {
	Next     time.Time
	NextWhy  string
	Promise  schedule.Promise
	Failures int
	Wakes    int
	Min, Max time.Duration
}

// queued is a wake that waits for its lane: what came in while the lane was busy
// is one wake when the lane is free.
type queued struct {
	reason string
	seqs   []int64
	facts  []string
	room   string
}

type spot struct {
	name string
	st   SpotState
	c    Carry

	busy    map[string]bool
	waiting map[string]*queued
	changed workflow.Channel
}

// SpotWorkflow is the spot: it waits for the intake's signals and for its own
// timers, and runs a wake for each, in two lanes. The tick and the wake a person
// asked for are durable timers; a hold rule, the schedule and the failure
// backoff are its state. The intake, the notes, the prompt and the books are the
// process's, reached through activities.
func SpotWorkflow(ctx workflow.Context, name string, carry Carry) error {
	s := &spot{name: name, c: carry, busy: map[string]bool{}, waiting: map[string]*queued{}}
	s.st.Wakes = carry.Wakes
	s.st.Next, s.st.NextWhy, s.st.Promise = carry.Next, carry.NextWhy, carry.Promise.At
	s.changed = workflow.NewBufferedChannel(ctx, 64)
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (SpotState, error) {
		s.st.Long, s.st.Short = s.busy[LaneLong], s.busy[LaneShort]
		s.st.Queued = len(s.waiting)
		s.st.Promise = s.c.Promise.At
		s.st.Next, s.st.NextWhy = s.c.Next, s.c.NextWhy
		return s.st, nil
	}); err != nil {
		return err
	}
	wakes := workflow.GetSignalChannel(ctx, SignalWake)

	for {
		for {
			var w WakeSignal
			if !wakes.ReceiveAsync(&w) {
				break
			}
			s.onSignal(ctx, w)
		}
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() && s.st.Open == 0 && len(s.waiting) == 0 {
			c := s.c
			c.Wakes = s.st.Wakes
			return workflow.NewContinueAsNewError(ctx, SpotWorkflow, name, c)
		}
		timerCtx, cancel := workflow.WithCancel(ctx)
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(wakes, func(c workflow.ReceiveChannel, _ bool) {
			var w WakeSignal
			c.Receive(ctx, &w)
			s.onSignal(ctx, w)
		})
		sel.AddReceive(s.changed, func(c workflow.ReceiveChannel, _ bool) { c.Receive(ctx, nil) })
		if at, kind := s.due(); kind != "" {
			d := at.Sub(workflow.Now(ctx))
			if d < 0 {
				d = 0
			}
			sel.AddFuture(workflow.NewTimer(timerCtx, d), func(f workflow.Future) {
				if f.Get(ctx, nil) == nil {
					s.onDue(ctx)
				}
			})
		}
		sel.Select(ctx)
		cancel()
	}
}

// due is the next time something is to happen on its own, and what: the wake a
// person asked for ("promise") or the scheduled one ("tick"). A promise that is
// due waits while both lanes are busy; the end of a wake looks again.
func (s *spot) due() (time.Time, string) {
	var at time.Time
	kind := ""
	if p := s.c.Promise.At; !p.IsZero() && (!s.busy[LaneLong] || !s.busy[LaneShort]) {
		at, kind = p, "promise"
	}
	if n := s.c.Next; !n.IsZero() && (kind == "" || n.Before(at)) {
		at, kind = n, "tick"
	}
	return at, kind
}

func (s *spot) log(ctx workflow.Context, format string, args ...any) {
	line := fmt.Sprintf("%s %s", workflow.Now(ctx).UTC().Format("15:04:05"), fmt.Sprintf(format, args...))
	s.st.Log = append(s.st.Log, line)
	if len(s.st.Log) > 20 {
		s.st.Log = s.st.Log[len(s.st.Log)-20:]
	}
}

func (s *spot) poke() { s.changed.SendAsync(struct{}{}) }

// onDue starts what is due. A promise runs on the long lane when it is free (it
// is the scheduled wake that was kept) and on the short lane when a long wake is
// working.
func (s *spot) onDue(ctx workflow.Context) {
	now := workflow.Now(ctx)
	if p := s.c.Promise; !p.None() && !now.Before(p.At) {
		lane := LaneLong
		if s.busy[LaneLong] {
			lane = LaneShort
		}
		if !s.busy[lane] {
			s.c.Promise = schedule.Promise{}
			if lane == LaneLong {
				s.c.Next = time.Time{}
			}
			s.start(ctx, lane, &queued{reason: ReasonTick}, p.Why, true)
			s.poke()
			return
		}
	}
	if n := s.c.Next; !n.IsZero() && !now.Before(n) {
		why := s.c.NextWhy
		s.c.Next = time.Time{}
		if s.busy[LaneLong] {
			return // the wake working now plans the next one
		}
		s.start(ctx, LaneLong, &queued{reason: ReasonTick}, why, false)
	}
}

// onSignal takes a wake the intake asked for. A person's message goes to the
// short lane, everything else to the long one; what comes while a lane is busy
// waits and is one wake when it frees.
func (s *spot) onSignal(ctx workflow.Context, w WakeSignal) {
	lane := LaneLong
	if w.Reason == ReasonHuman {
		lane = LaneShort
	}
	if w.Reason == ReasonStartup && s.st.Wakes > 0 && !s.c.Next.IsZero() {
		s.log(ctx, "startup: the schedule survived, no wake")
		return
	}
	q := &queued{reason: w.Reason, seqs: []int64{w.Seq}, facts: w.Facts, room: w.Room}
	if s.busy[lane] {
		if cur := s.waiting[lane]; cur != nil {
			cur.seqs = append(cur.seqs, q.seqs...)
			cur.facts = append(cur.facts, q.facts...)
			if q.room != "" {
				cur.room = q.room
			}
		} else {
			s.waiting[lane] = q
		}
		return
	}
	s.start(ctx, lane, q, "", false)
}

func (s *spot) start(ctx workflow.Context, lane string, q *queued, why string, promise bool) {
	s.busy[lane] = true
	s.st.Open++
	s.st.Wakes++
	key := fmt.Sprintf("%s-wake-%d", s.name, s.st.Wakes)
	workflow.Go(ctx, func(ctx workflow.Context) {
		s.runWake(ctx, key, lane, q, why, promise)
		s.st.Open--
		s.busy[lane] = false
		if next := s.waiting[lane]; next != nil {
			delete(s.waiting, lane)
			s.start(ctx, lane, next, "", false)
		}
		s.poke()
	})
}

// activity options of the process-side steps. The books run once: a retry would
// count a turn twice.
func (s *spot) opts(ctx workflow.Context, attempts int32, timeout time.Duration) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: attempts, InitialInterval: time.Second},
	})
}

// runWake is one wake from the signal to the plan of the next.
func (s *spot) runWake(ctx workflow.Context, key, lane string, q *queued, why string, promise bool) {
	var po PrepareOut
	err := workflow.ExecuteActivity(s.opts(ctx, 2, 90*time.Second), (*Acts).Prepare, PrepareIn{
		Key: key, Lane: lane, Reason: q.reason, Seqs: q.seqs, Facts: q.facts, Room: q.room, Why: why,
		Promise: promise, PromisePending: s.c.Promise.Human && !s.c.Promise.None(),
	}).Get(ctx, &po)
	if err != nil {
		s.log(ctx, "%s: not prepared: %s", key, errText(err))
		return
	}
	if po.Min > 0 {
		s.c.Min, s.c.Max = po.Min, po.Max
	}
	now := workflow.Now(ctx)

	announce := func(in AnnounceIn) {
		in.Key, in.Lane, in.Reason = key, lane, q.reason
		if err := workflow.ExecuteActivity(s.opts(ctx, 1, time.Minute), (*Acts).Announce, in).Get(ctx, nil); err != nil {
			s.log(ctx, "%s: not announced: %s", key, errText(err))
		}
	}

	switch {
	case po.Paused:
		// The ceiling holds the agent back until it lifts.
		if lane == LaneLong {
			s.c.Next, s.c.NextWhy = po.Until, "the daily ceiling was reached; resumed after UTC midnight"
		}
		s.log(ctx, "%s: paused until %s", key, po.Until.UTC().Format(time.RFC3339))
		announce(AnnounceIn{Held: true, At: s.c.Next, Why: s.c.NextWhy, Source: "ceiling", After: po.Until.Sub(now)})
		return
	case po.Quiet:
		// Nothing to work on: no turn. The next wake is the longest the spot allows,
		// or the wake a person asked for; an event wakes it sooner.
		if lane == LaneLong {
			s.c.Next, s.c.NextWhy = now.Add(po.Max), "nothing to act on: waiting for an event"
			if p := s.c.Promise; p.Human && !p.None() && p.At.Before(s.c.Next) {
				s.c.Next, s.c.NextWhy = p.At, p.Why
			}
		}
		s.log(ctx, "%s: quiet, no model call", key)
		announce(AnnounceIn{Held: true, At: s.c.Next, Why: s.c.NextWhy, Source: "quiet", After: s.c.Next.Sub(now)})
		return
	case po.Hold:
		// A rule says to check nothing until a person asks: no turn. What woke it is
		// dropped and the next wake is the longest the spot allows, or the wake a
		// person asked for.
		if lane == LaneLong {
			s.c.Next, s.c.NextWhy = now.Add(po.Max), "a hold rule is active"
			if p := s.c.Promise; p.Human && !p.None() && p.At.Before(s.c.Next) {
				s.c.Next, s.c.NextWhy = p.At, p.Why
			}
		}
		s.st.Held++
		s.log(ctx, "%s: held by a rule, no model call", key)
		announce(AnnounceIn{Held: true, At: s.c.Next, Why: s.c.NextWhy, Source: "hold", After: s.c.Next.Sub(now)})
		return
	}

	cctx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: key, WorkflowExecutionTimeout: po.Turn + 5*time.Minute,
	})
	var out WakeOut
	err = workflow.ExecuteChildWorkflow(cctx, WakeWorkflow, WakeIn{
		Key: key, Reason: q.reason, Lane: lane, Prompt: po.Prompt, Room: q.room, Limited: po.Limited,
		Caps: po.Caps, Tool: po.Tool, Turn: po.Turn, Typing: po.Typing, Model: po.Model,
	}).Get(ctx, &out)
	if err != nil {
		out = WakeOut{Failed: true, Err: "the turn did not end well: " + errText(err)}
		var te *temporal.TimeoutError
		out.TimedOut = errors.As(err, &te)
	}

	var ao AccountOut
	if aerr := workflow.ExecuteActivity(s.opts(ctx, 1, time.Minute), (*Acts).Account, AccountIn{Key: key, Lane: lane, Reason: q.reason, Out: out}).Get(ctx, &ao); aerr != nil {
		s.log(ctx, "%s: not booked: %s", key, errText(aerr))
		ao = AccountOut{Failed: out.Failed}
	}
	s.st.Tokens += out.Input + out.Output + out.CacheWrite
	now = workflow.Now(ctx)

	in := AnnounceIn{Failed: ao.Failed, Summary: out.Summary}
	asked, after, nwhy := out.Next != nil, "", ""
	if asked {
		after, nwhy = out.Next.After, out.Next.Why
	}
	if lane == LaneShort {
		s.c.Promise = schedule.AfterShortTurn(now, po.Min, po.Max, asked, after, nwhy, s.c.Promise)
		in.At, in.Why = s.c.Next, s.c.NextWhy
		in.Source = "agent"
		if !s.c.Next.IsZero() {
			in.After = s.c.Next.Sub(now)
		}
	} else {
		s.c.Failures = ao.Failures
		plan := schedule.AfterTurn(schedule.TurnEnd{
			Now: now, Min: po.Min, Max: po.Max, Human: q.reason == ReasonHuman, Tick: q.reason == ReasonTick,
			Asked: asked, After: after, Why: nwhy, Failed: ao.Failed, Backoff: ao.Backoff, Promise: s.c.Promise,
		})
		s.c.Next, s.c.NextWhy, s.c.Promise = plan.At, plan.Why, plan.Promise
		in.At, in.Why, in.After, in.Source, in.Note = plan.At, plan.Why, plan.Wake.After, string(plan.Wake.Source), plan.Wake.Note
	}
	s.log(ctx, "%s: %s %s steps=%d tools=%d failed=%v", key, lane, q.reason, out.Steps, out.ToolCalls, ao.Failed)
	announce(in)
}
