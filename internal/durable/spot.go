package durable

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/triageagent-dev/spot/internal/schedule"
	"github.com/triageagent-dev/spot/internal/tools"
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

// maxRunAge is how long one run of the spot's workflow lasts before it goes on
// as a new run when nothing is open. A run is replayed from its history after a
// restart, and that reads every payload the run kept: the payloads of a run
// must be younger than the sweep (see Options.PayloadTTL).
const maxRunAge = 7 * 24 * time.Hour

// queued is a wake that waits for its lane: what came in while the lane was busy
// is one wake when the lane is free.
type queued struct {
	reason string
	seqs   []int64
	facts  []string
	room   string
	// title and goal are the task of a task wake.
	title, goal string
}

type spot struct {
	name string
	st   SpotState
	c    Carry

	busy    map[string]bool
	waiting map[string]*queued
	changed workflow.Channel

	// Tasks: how many run, how many may, and those waiting for a place.
	tasks, maxTasks int
	taskQ           []queuedTask
	qseq            int

	// running is every wake in hand, by key, for the operations on it.
	running map[string]*work
}

// queuedTask is a task waiting for a place, with a key to drop it by.
type queuedTask struct {
	key string
	req tools.TaskRequest
}

// work is a wake in hand: what the operations act on.
type work struct {
	info    TaskInfo
	req     tools.TaskRequest // a task's own words, to start it again
	cancel  workflow.CancelFunc
	stopped bool // stop was asked; the turn is not a failure
	restart bool // start the same task again when it ends
}

// SpotWorkflow is the spot: it waits for the intake's signals and for its own
// timers, and runs a wake for each, in two lanes. The tick and the wake a person
// asked for are durable timers; a hold rule, the schedule and the failure
// backoff are its state. The intake, the notes, the prompt and the books are the
// process's, reached through activities.
func SpotWorkflow(ctx workflow.Context, name string, carry Carry) error {
	s := &spot{name: name, c: carry, busy: map[string]bool{}, waiting: map[string]*queued{}, running: map[string]*work{}}
	s.st.Wakes = carry.Wakes
	s.st.Next, s.st.NextWhy, s.st.Promise = carry.Next, carry.NextWhy, carry.Promise.At
	s.changed = workflow.NewBufferedChannel(ctx, 64)
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (SpotState, error) {
		s.st.Long, s.st.Short = s.busy[LaneLong], s.busy[LaneShort]
		s.st.Queued = len(s.waiting)
		s.st.Tasks, s.st.TaskQ = s.tasks, len(s.taskQ)
		s.st.Work = s.work()
		s.st.Promise = s.c.Promise.At
		s.st.Next, s.st.NextWhy = s.c.Next, s.c.NextWhy
		return s.st, nil
	}); err != nil {
		return err
	}
	wakes := workflow.GetSignalChannel(ctx, SignalWake)
	controls := workflow.GetSignalChannel(ctx, SignalControl)

	for {
		for {
			var w WakeSignal
			if !wakes.ReceiveAsync(&w) {
				break
			}
			s.onSignal(ctx, w)
		}
		for {
			var c ControlSignal
			if !controls.ReceiveAsync(&c) {
				break
			}
			s.onControl(ctx, c)
		}
		if (workflow.GetInfo(ctx).GetContinueAsNewSuggested() || workflow.Now(ctx).Sub(workflow.GetInfo(ctx).WorkflowStartTime) > maxRunAge) && s.st.Open == 0 && len(s.waiting) == 0 {
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
		sel.AddReceive(controls, func(c workflow.ReceiveChannel, _ bool) {
			var cs ControlSignal
			c.Receive(ctx, &cs)
			s.onControl(ctx, cs)
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
	wk := &work{info: TaskInfo{Key: key, Title: q.reason, Lane: lane, State: "running", Started: workflow.Now(ctx)}}
	s.running[key] = wk
	workflow.Go(ctx, func(ctx workflow.Context) {
		s.runWake(ctx, key, lane, q, why, promise)
		delete(s.running, key)
		s.st.Open--
		s.busy[lane] = false
		if next := s.waiting[lane]; next != nil {
			delete(s.waiting, lane)
			s.start(ctx, lane, next, "", false)
		}
		s.poke()
	})
}

// startTask runs a task the agent asked for in a turn of its own, beside the
// lanes. Past the limit it waits for a place; past the queue it is dropped.
func (s *spot) startTask(ctx workflow.Context, t tools.TaskRequest) {
	if s.tasks >= s.maxTasks {
		if len(s.taskQ) >= MaxQueuedTasks {
			s.log(ctx, "task %q dropped: %d are waiting", t.Title, len(s.taskQ))
			return
		}
		s.qseq++
		s.taskQ = append(s.taskQ, queuedTask{key: fmt.Sprintf("queued-%d", s.qseq), req: t})
		return
	}
	s.tasks++
	s.st.Open++
	s.st.Wakes++
	key := fmt.Sprintf("%s-wake-%d", s.name, s.st.Wakes)
	wk := &work{req: t, info: TaskInfo{Key: key, Title: t.Title, Lane: LaneTask, State: "running", Started: workflow.Now(ctx)}}
	s.running[key] = wk
	workflow.Go(ctx, func(ctx workflow.Context) {
		s.runWake(ctx, key, LaneTask, &queued{reason: ReasonTask, title: t.Title, goal: t.Goal}, "", false)
		delete(s.running, key)
		s.st.Open--
		s.tasks--
		if wk.restart {
			s.startTask(ctx, t) // a restart keeps its place: it is not behind what waited
		}
		if len(s.taskQ) > 0 && s.tasks < s.maxTasks {
			next := s.taskQ[0]
			s.taskQ = s.taskQ[1:]
			s.startTask(ctx, next.req)
		}
		s.poke()
	})
}

// work lists what the spot has in hand: its wakes and tasks, then the tasks
// waiting for a place.
func (s *spot) work() []TaskInfo {
	out := make([]TaskInfo, 0, len(s.running)+len(s.taskQ))
	for _, w := range s.running {
		out = append(out, w.info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Started.Before(out[j].Started) })
	for _, q := range s.taskQ {
		out = append(out, TaskInfo{Key: q.key, Title: q.req.Title, Lane: LaneTask, State: "queued"})
	}
	return out
}

// onControl acts on a wake an operator named. The engine checked the key; one
// that has ended meanwhile is only logged.
func (s *spot) onControl(ctx workflow.Context, c ControlSignal) {
	switch c.Op {
	case OpDrop:
		for i, q := range s.taskQ {
			if q.key == c.Key {
				s.taskQ = append(s.taskQ[:i], s.taskQ[i+1:]...)
				s.log(ctx, "task %q dropped by an operator", q.req.Title)
				return
			}
		}
	case OpStop, OpRestart:
		w := s.running[c.Key]
		if w == nil {
			s.log(ctx, "%s %s: not running any more", c.Op, c.Key)
			return
		}
		w.stopped = true
		w.restart = c.Op == OpRestart && w.info.Lane == LaneTask
		if w.cancel != nil {
			w.cancel()
		}
		s.log(ctx, "%s: %s asked by an operator", c.Key, c.Op)
	}
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
		Promise: promise, PromisePending: s.c.Promise.Human && !s.c.Promise.None(), Title: q.title, Goal: q.goal,
	}).Get(ctx, &po)
	if err != nil {
		s.log(ctx, "%s: not prepared: %s", key, errText(err))
		return
	}
	if po.Min > 0 {
		s.c.Min, s.c.Max = po.Min, po.Max
	}
	s.maxTasks = po.MaxTasks
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
		if lane == LaneTask {
			s.log(ctx, "%s: task %q not run: the ceiling is reached", key, q.title)
			announce(AnnounceIn{Held: true, Source: "ceiling"})
			return
		}
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
	cctx, cancelChild := workflow.WithCancel(cctx)
	defer cancelChild()
	wk := s.running[key]
	if wk != nil {
		wk.cancel = cancelChild
		if wk.stopped {
			cancelChild() // stop was asked while the wake was being prepared
		}
	}
	var out WakeOut
	err = workflow.ExecuteChildWorkflow(cctx, WakeWorkflow, WakeIn{
		SpotID: workflow.GetInfo(ctx).WorkflowExecution.ID, SpotRun: workflow.GetInfo(ctx).WorkflowExecution.RunID,
		Key: key, Reason: q.reason, Lane: lane, Prompt: po.Prompt, Room: q.room, Limited: po.Limited,
		Caps: po.Caps, Tool: po.Tool, Turn: po.Turn, Typing: po.Typing, Model: po.Model, Task: lane == LaneTask,
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
	if lane == LaneTask {
		// A task changes no schedule and starts no task: it ends with its result.
		in.Source = "task"
		switch {
		case ao.Failed:
			in.Summary = "failed: " + out.Err
		case wk != nil && wk.stopped && in.Summary == "":
			in.Summary = "stopped by an operator before it finished"
		}
		s.log(ctx, "%s: task %q steps=%d tools=%d failed=%v", key, q.title, out.Steps, out.ToolCalls, ao.Failed)
		announce(in)
		return
	}
	for _, t := range out.Tasks {
		s.startTask(ctx, t)
	}
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
