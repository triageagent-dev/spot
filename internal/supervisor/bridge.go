package supervisor

import (
	"context"
	"errors"
	"time"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/corecli"
	"github.com/triageagent-dev/spot/internal/durable"
	"github.com/triageagent-dev/spot/internal/edges"
	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/prompt"
	"github.com/triageagent-dev/spot/internal/schedule"
	"github.com/triageagent-dev/spot/internal/triggers"
)

// The durable engine (internal/durable) runs the schedule, the lanes and the
// turns in a Temporal workflow. The supervisor is then its eyes and hands: it
// watches the core (the stream, the polls, the held edges, the cooldowns, the
// caps) and tells the workflow with a signal when the agent is wanted, and the
// workflow's activities call back into it, through Host, to read the notes and
// build the prompt, to book what a turn cost and to say what is next. It owns
// no timer and runs no turn.

// Sink receives a wake the intake asked for. durable.Engine is one.
type Sink interface {
	Wake(ctx context.Context, w durable.WakeSignal) error
}

// wakeRec is what the supervisor keeps of a wake between its head and its end.
type wakeRec struct {
	p    *pending
	pre  edges.Snapshot
	lane string
}

var errBridgeStopped = errors.New("the supervisor has stopped")

// RunBridge is Run for the durable engine: it blocks until ctx is cancelled,
// watching the core and signalling wakes to sink. A killed pod starts fresh: the
// token ledger and the room cursors are in /state when it survives, and the
// schedule is the workflow's.
func (s *Supervisor) RunBridge(ctx context.Context, sink Sink) error {
	defer s.stopOnce.Do(func() { close(s.stopped) })
	s.sink, s.bctx = sink, ctx
	s.status.StartedAt = s.now().UTC().Format(time.RFC3339)
	s.setState("starting")

	s.refreshIdentity(ctx, true, 0)
	s.dispatch(ctx, &pending{reason: prompt.Startup})
	for ctx.Err() == nil {
		read, alive := s.wait(ctx)
		if !alive {
			break
		}
		if read {
			if es := s.poll(ctx); len(es) > 0 {
				s.hold(es)
			}
		}
		if p := s.dueFromHeld(); p != nil {
			s.dispatch(ctx, p)
		} else if len(s.held) > 0 {
			s.setState(s.status.State) // the held count is visible while it waits
		}
	}
	s.setState("stopping")
	s.setState("stopped")
	return nil
}

// dispatch tells the workflow that the agent is wanted. If it cannot be told the
// edges go back to be carried by the next wake.
func (s *Supervisor) dispatch(ctx context.Context, p *pending) {
	s.bseq++
	seq := s.bseq
	s.inflight[seq] = p
	w := durable.WakeSignal{Seq: seq, Reason: string(p.reason), Facts: p.facts, Room: p.room}
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := s.sink.Wake(sctx, w); err != nil {
		delete(s.inflight, seq)
		if ctx.Err() != nil {
			return
		}
		s.log.Error("the workflow could not be told", "reason", p.reason, "err", err)
		s.noteCoreError("workflow: " + err.Error())
		s.requeue(p.edges)
		return
	}
	s.clearCoreError()
	s.log.Info("wake signalled", "reason", p.reason, "facts", len(p.facts), "seq", seq)
}

// call runs f on the loop's goroutine and waits for it. The supervisor's state
// is touched by that goroutine only.
func (s *Supervisor) call(f func()) error {
	done := make(chan struct{})
	select {
	case s.callc <- func() { f(); close(done) }:
	case <-s.stopped:
		return errBridgeStopped
	}
	select {
	case <-done:
		return nil
	case <-s.stopped:
		return errBridgeStopped
	}
}

// Host is what the durable engine's activities call back into.
func (s *Supervisor) Host() durable.Host { return bridgeHost{s} }

type bridgeHost struct{ s *Supervisor }

func (h bridgeHost) Prepare(in durable.PrepareIn) (out durable.PrepareOut, err error) {
	err = h.s.call(func() { out = h.s.prepare(in) })
	return out, err
}

func (h bridgeHost) Account(in durable.AccountIn) (out durable.AccountOut, err error) {
	err = h.s.call(func() { out = h.s.account(in) })
	return out, err
}

func (h bridgeHost) Announce(in durable.AnnounceIn) error {
	return h.s.call(func() { h.s.announce(in) })
}

// Typing needs nothing of the supervisor's state: it is the core's, and safe to
// call from any goroutine.
func (h bridgeHost) Typing(ctx context.Context, room string, on bool) error {
	err := h.s.core.Typing(ctx, room, on)
	if err != nil && !errors.Is(err, corecli.ErrNoTypingTool) && ctx.Err() == nil {
		h.s.log.Debug("typing indicator failed", "room", room, "err", err)
	}
	return err
}

// caps are the limits of a wake of the lane.
func (s *Supervisor) caps(lane string) agent.Caps {
	c := agent.Caps{MaxSteps: s.cfg.MaxSteps, MaxToolCalls: s.cfg.MaxToolCalls, MaxTurnTokens: s.cfg.MaxTurnTokens}
	if c.MaxSteps <= 0 {
		c.MaxSteps = agent.DefaultMaxSteps
	}
	if c.MaxToolCalls <= 0 {
		c.MaxToolCalls = agent.DefaultMaxToolCalls
	}
	if lane == durable.LaneShort && s.cfg.FastLane {
		c.MaxSteps = min(c.MaxSteps, s.cfg.FastMaxToolCalls+4)
		c.MaxToolCalls, c.MaxTurnTokens = s.cfg.FastMaxToolCalls, s.cfg.FastMaxTurnTokens
	}
	return c
}

// prepare is the head of a wake: the facts, the standing notes, the hold rule and
// the prompt.
func (s *Supervisor) prepare(in durable.PrepareIn) (out durable.PrepareOut) {
	ctx := s.bctx
	out = durable.PrepareOut{
		Min: s.cfg.MinWake, Max: s.cfg.MaxWake, Turn: s.cfg.TurnTimeout, Tool: s.cfg.ToolTimeout,
		Model: s.cfg.Model, Typing: true, Caps: s.caps(in.Lane),
	}
	reason := prompt.Reason(in.Reason)

	// What woke it, as the intake made it. A wake that merged several signals
	// carries all their edges.
	p := &pending{reason: reason, facts: append([]string(nil), in.Facts...), scheduledBy: in.Why, room: in.Room}
	for _, seq := range in.Seqs {
		if sp := s.inflight[seq]; sp != nil {
			p.edges = append(p.edges, sp.edges...)
			delete(s.inflight, seq)
		}
	}
	if in.Lane == durable.LaneLong && len(s.held) > 0 && reason != prompt.Startup {
		for _, e := range s.held {
			p.facts = append(p.facts, e.Fact)
		}
		p.edges = append(p.edges, s.held...)
		s.held = nil
		s.status.HeldEdges = 0
	}

	if s.paused() {
		s.pauseForCeiling()
		out.Paused, out.Until = true, s.nextTick
		s.requeue(p.edges)
		return out
	}
	s.refreshIdentity(ctx, false, identityEvery)
	rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
	rules, hold, rerr := s.core.StandingNotes(rctx, s.cfg.Name)
	rcancel()
	if rerr != nil && ctx.Err() == nil {
		s.log.Warn("standing notes not read; the wake goes on without them", "err", rerr)
	}
	// The wake a person asked for runs under a rule, and so does the scheduled one
	// while such a wake is waiting (it is that one, kept).
	exempt := in.Promise || in.PromisePending
	if hold && reason != prompt.Human && (reason != prompt.Tick || !exempt) {
		s.log.Info("a hold rule is active: no model call", "reason", reason, "dropped", len(p.facts))
		out.Hold = true
		return out
	}
	p.facts = s.currentFacts(ctx, p)
	out.Limited = hold && reason == prompt.Human
	out.Prompt = prompt.Turn(prompt.Wake{
		Limited: out.Limited, Reason: reason, Now: s.now(), ChatName: s.messengerName(), Facts: p.facts, ScheduledBy: in.Why,
		Digest: s.digestLines(), Rules: rules, Side: in.Lane == durable.LaneShort,
		TokensUsed: s.led.Usage().Tokens, TokensCeil: s.cfg.DailyTokenCeil,
	})

	s.recs[in.Key] = &wakeRec{p: p, pre: s.tracker.Snapshot(), lane: in.Lane}
	s.open++
	s.setState("running")
	s.rollDay()
	if in.Lane == durable.LaneShort {
		s.wakes["side-"+string(reason)]++
	} else {
		s.wakes[string(reason)]++
	}
	s.log.Info("turn start", "lane", in.Lane, "reason", reason, "facts", len(p.facts), "wake", in.Key)
	return out
}

// resultOf is the workflow's account of a turn as the agent's Result, so the
// supervisor's own reading of one (failureText, the books) serves both engines.
func resultOf(o durable.WakeOut) agent.Result {
	res := agent.Result{
		Text: o.Text, Summary: o.Summary, Next: o.Next, Steps: o.Steps, ToolCalls: o.ToolCalls,
		Usage:         llm.Usage{Input: o.Input, Output: o.Output, CacheRead: o.CacheRead, CacheWrite: o.CacheWrite},
		ContextTokens: o.ContextTokens, Model: o.Model, Duration: time.Duration(o.Seconds * float64(time.Second)),
		StepsExhausted: o.StepsExhausted, ToolLimit: o.ToolLimit, TokenLimit: o.TokenLimit,
		TimedOut: o.TimedOut, AuthFailed: o.AuthFailed, CoreDown: o.CoreDown, Failed: o.Failed,
	}
	if o.Err != "" {
		res.Err = errors.New(o.Err)
	}
	return res
}

// account books a turn that ended: the tokens first, whether or not it went
// well; then what a failure means for the facts it carried.
func (s *Supervisor) account(in durable.AccountIn) durable.AccountOut {
	res := resultOf(in.Out)
	rec := s.recs[in.Key]
	long := in.Lane == durable.LaneLong
	if long {
		s.lastTurnEnd = s.now()
	}
	s.book(res)

	failed := res.Failed || res.CoreDown
	if rec != nil && res.Steps == 0 && failed {
		s.requeue(rec.p.edges)
	}
	if long {
		s.status.LastTurn = s.lastTurn(prompt.Reason(in.Reason), res, failed)
		if failed {
			s.failures++
			s.status.Failures = s.failures
			s.status.LastError = failureText(res, s.cfg.LLMAuth)
		} else {
			s.failures, s.status.Failures, s.status.LastError = 0, 0, ""
		}
	}
	u := res.Usage
	switch {
	case failed:
		s.log.Error("turn failed", "lane", in.Lane, "err", failureText(res, s.cfg.LLMAuth), "failures", s.failures, "timedOut", res.TimedOut,
			"coreDown", res.CoreDown, "tokens", u.Billable())
	default:
		if res.StepsExhausted {
			s.log.Warn("turn stopped at max-steps; the work may be unfinished", "tokens", u.Billable(), "steps", res.Steps)
		}
		if res.ToolLimit {
			cap := "max-tool-calls"
			if res.TokenLimit {
				cap = "max-turn-tokens"
			}
			s.log.Warn("turn stopped at "+cap+"; the work may be unfinished", "tokens", u.Billable(), "toolCalls", res.ToolCalls)
		}
		s.log.Info("turn done", "lane", in.Lane, "billable", u.Billable(), "read", u.CacheRead, "out", u.Output, "steps", res.Steps, "toolCalls", res.ToolCalls, "seconds", int(res.Duration.Seconds()))
	}
	out := durable.AccountOut{Failed: failed, Failures: s.failures}
	if failed {
		out.Backoff = s.backoff()
	}
	return out
}

// announce is the end of a wake: the next wake is known, so the status says it,
// the digest keeps the turn's summary, the room's watermark moves, what changed
// during the turn is held, and the heartbeat is refreshed.
func (s *Supervisor) announce(in durable.AnnounceIn) {
	ctx := s.bctx
	rec := s.recs[in.Key]
	delete(s.recs, in.Key)
	if rec != nil && s.open > 0 {
		s.open--
	}
	long := in.Lane == durable.LaneLong
	reason := prompt.Reason(in.Reason)
	if in.Held {
		// No turn ran (a rule, or the ceiling): only the schedule is told.
		if long {
			s.lastTurnEnd = s.now()
			s.nextTick, s.scheduledWhy = in.At, in.Why
			s.status.NextWake = &Next{At: in.At.UTC().Format(time.RFC3339), Why: in.Why, Source: in.Source}
		}
		if in.Source != "ceiling" {
			s.setState("idle")
		}
		return
	}
	if long {
		s.nextTick, s.scheduledWhy = in.At, in.Why
		s.status.NextWake = &Next{At: in.At.UTC().Format(time.RFC3339), Why: in.Why, Source: in.Source, Note: in.Note}
		if in.Source != string(schedule.SourceAgent) && in.Source != "" {
			s.log.Warn("schedule not taken as written", "source", in.Source, "note", in.Note, "after", in.After)
		}
	}
	if !in.Failed {
		s.addDigest(reason, agent.Result{Summary: in.Summary})
		s.markRoomsRead()
	}
	if long && rec != nil {
		// Baseline after the turn. What changed during it, other than comments (the
		// agent writes those itself), is held and wakes the agent again by the usual
		// rules.
		if issues, err := s.core.ListOpenIssues(ctx); err == nil {
			if late := s.tracker.SettleFrom(rec.pre, issues); len(late) > 0 {
				s.log.Info("changes during the turn", "count", len(late), "kinds", kindCounts(late))
				s.hold(late)
			}
		}
		if !in.Failed {
			s.heartbeat(ctx, schedule.Wake{After: in.After, Why: in.Why}, reason)
		} else {
			s.log.Warn("heartbeat not refreshed: the turn failed, so reflection stays the floor")
		}
	}
	if s.open == 0 {
		s.setState("idle")
	}
	s.reportStats(true)
}

// book counts what a turn cost: in the ledger and, for the core, as the spot's
// own count (the core books it under the spot's kind and answers with the budget
// after it).
func (s *Supervisor) book(res agent.Result) {
	u := res.Usage
	if _, err := s.led.AddUsage(u.Billable(), u.CacheRead, u.Output); err != nil {
		s.log.Error("saving usage", "err", err)
	}
	if s.trig != nil && (u.Billable() > 0 || u.CacheRead > 0) {
		s.trig.ReportTurn(triggers.Turn{
			Model: modelLabel(res.Model), Input: u.Input + u.CacheWrite + u.CacheRead,
			Cached: u.CacheRead, Output: u.Output, DurationMs: res.Duration.Milliseconds(), Failed: res.Failed,
		})
	}
}

// lastTurn is the status file's line about the turn that just ended.
func (s *Supervisor) lastTurn(reason prompt.Reason, res agent.Result, failed bool) *Turn {
	return &Turn{
		At: s.now().UTC().Format(time.RFC3339), Reason: string(reason), Seconds: res.Duration.Seconds(),
		Tokens: res.Usage.Billable(), Steps: res.Steps, ToolCalls: res.ToolCalls, Model: res.Model, Summary: res.Summary,
		IsError: failed, StepsExhausted: res.StepsExhausted, TimedOut: res.TimedOut, AuthFailed: res.AuthFailed,
		CoreDown: res.CoreDown, ContextTokens: res.ContextTokens,
	}
}
