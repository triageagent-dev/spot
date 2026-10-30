package supervisor

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/prompt"
	"github.com/triageagent-dev/spot/internal/tools"
	"github.com/triageagent-dev/spot/internal/triggers"
)

// Lanes: the long one runs the scheduled, startup and edge wakes, one at a time;
// the short one a person's message and a wake a person asked for, one at a time
// and beside the long one. The task lane runs the tasks a turn started, up to
// the spot's limit at once, beside both.
const (
	LaneLong  = "long"
	LaneShort = "short"
	LaneTask  = "task"
)

// Operations on the work the spot has in hand.
const (
	OpList    = "list"
	OpStop    = "stop"    // cancel: the turn ends at its next step and closes its tools
	OpKill    = "kill"    // cancel and take it off the list at once; its cost is still booked
	OpRestart = "restart" // stop a task and start the same one again
	OpDrop    = "drop"    // remove a task that has not started
)

// MaxQueuedTasks is how many tasks wait for a free place; a task past it is
// dropped and the spot's log says so.
const MaxQueuedTasks = 10

// controlWait is how long an operation waits to see its effect before it answers
// with what the spot has in hand.
const controlWait = 3 * time.Second

// job is one wake the spot has in hand: running, or (a task only) waiting for a
// place. Its key names it in the operations.
type job struct {
	key     string
	title   string // a task's title; the reason of any other wake
	lane    string
	state   string // running | queued
	started time.Time
	req     tools.TaskRequest
	cancel  context.CancelFunc
	// stopped, killed and restart are set by an operation; the table's lock
	// guards them.
	stopped, killed, restart bool
}

// work is what the spot has in hand. The loop adds and removes the wakes it runs;
// the core's controls come from another goroutine, so the table has its own lock.
type work struct {
	mu      sync.Mutex
	seq     int
	qseq    int
	running map[string]*job
	queue   []*job
}

// taskEnd is a task's turn that has ended, for the loop to account.
type taskEnd struct {
	j   *job
	res agent.Result
}

// SetTaskTurner gives the spot what runs a task: a turner of its own for each,
// because the gates and the connections of a turn are its own. Without it a task
// is not run.
func (s *Supervisor) SetTaskTurner(f func() Turner) { s.newTask = f }

// begin enters a wake in the table and returns the context its turn runs in; an
// operation cancels it.
func (s *Supervisor) begin(ctx context.Context, lane, title string, req tools.TaskRequest) (*job, context.Context) {
	cctx, cancel := context.WithCancel(ctx)
	w := &s.work
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	j := &job{key: fmt.Sprintf("%s-wake-%d", s.cfg.Name, w.seq), title: title, lane: lane, state: "running", started: s.now(), req: req, cancel: cancel}
	w.running[j.key] = j
	return j, cctx
}

// end takes a wake off the table when its turn is over and says whether an
// operator stopped it and, for a task, asked for it again.
func (s *Supervisor) end(j *job) (stopped, restart bool) {
	w := &s.work
	w.mu.Lock()
	delete(w.running, j.key)
	stopped, restart = j.stopped, j.restart
	w.mu.Unlock()
	j.cancel()
	return stopped, restart
}

// taskLines is the work the spot has in hand, for the stats and the controls'
// answers: the wakes running, oldest first, then the tasks waiting for a place.
func (s *Supervisor) taskLines() []triggers.TaskLine {
	w := &s.work
	w.mu.Lock()
	defer w.mu.Unlock()
	run := make([]*job, 0, len(w.running))
	for _, j := range w.running {
		run = append(run, j)
	}
	sort.Slice(run, func(a, b int) bool { return run[a].started.Before(run[b].started) })
	now := s.now()
	var out []triggers.TaskLine
	for _, j := range append(run, w.queue...) {
		l := triggers.TaskLine{Key: j.key, Title: clipBytes(j.title, triggers.MaxTitle), Lane: j.lane, State: j.state}
		if !j.started.IsZero() {
			l.StartedMs, l.AgeMs = j.started.UnixMilli(), now.Sub(j.started).Milliseconds()
		}
		out = append(out, l)
	}
	return out
}

// Control acts on one wake of the spot (c.Task) and answers with what the spot
// has in hand after it. A key the spot does not have is refused.
func (s *Supervisor) Control(ctx context.Context, c triggers.Control) triggers.ControlResult {
	ok, msg := s.control(ctx, c.Op, c.Task)
	if !ok {
		// A refused control tells nothing of what the spot has in hand.
		return triggers.ControlResult{ID: c.ID, Msg: msg}
	}
	return triggers.ControlResult{ID: c.ID, OK: ok, Msg: msg, Tasks: s.taskLines()}
}

func (s *Supervisor) control(ctx context.Context, op, key string) (bool, string) {
	if op == OpList {
		return true, ""
	}
	w := &s.work
	w.mu.Lock()
	j := w.running[key]
	queued := -1
	if j == nil {
		for i, q := range w.queue {
			if q.key == key {
				j, queued = q, i
			}
		}
	}
	refuse := func(msg string) (bool, string) { w.mu.Unlock(); return false, msg }
	switch {
	case j == nil:
		return refuse(fmt.Sprintf("%q is not running or waiting", key))
	case op == OpDrop && j.state != "queued":
		return refuse("only a task that has not started can be dropped; stop or kill it")
	case op != OpDrop && j.state != "running":
		return refuse("it has not started: drop it")
	case op == OpRestart && j.lane != LaneTask:
		return refuse("only a task can be restarted")
	}
	switch op {
	case OpDrop:
		w.queue = append(w.queue[:queued], w.queue[queued+1:]...)
		s.log.Info("task dropped by an operator", "title", j.title)
	case OpKill:
		j.stopped, j.killed = true, true
		delete(w.running, key)
		j.cancel()
	case OpStop, OpRestart:
		j.stopped, j.restart = true, op == OpRestart && j.lane == LaneTask
		j.cancel()
	default:
		return refuse(fmt.Sprintf("%q is not one of list, stop, kill, restart, drop", op))
	}
	w.mu.Unlock()
	s.log.Info("asked by an operator", "op", op, "wake", key)

	// Wait for the effect: gone from the list.
	deadline := time.Now().Add(controlWait)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return true, op + " asked: the turn ends at its next step"
		case <-time.After(150 * time.Millisecond):
		}
		w.mu.Lock()
		_, still := w.running[key]
		w.mu.Unlock()
		if !still {
			return true, op + " done"
		}
	}
	return true, op + " asked: the turn ends at its next step"
}

// startTasks starts the tasks a turn asked for, when the turn ends.
func (s *Supervisor) startTasks(ctx context.Context, reqs []tools.TaskRequest) {
	for _, t := range reqs {
		s.startTask(ctx, t, false)
	}
}

// startTask runs a task now, or queues it when the limit is reached. A task past
// the queue is dropped. front puts a restarted task ahead of those waiting.
func (s *Supervisor) startTask(ctx context.Context, t tools.TaskRequest, front bool) {
	if s.cfg.MaxTasks <= 0 || s.newTask == nil {
		return
	}
	w := &s.work
	w.mu.Lock()
	running := 0
	for _, j := range w.running {
		if j.lane == LaneTask {
			running++
		}
	}
	if running >= s.cfg.MaxTasks {
		defer w.mu.Unlock()
		if len(w.queue) >= MaxQueuedTasks && !front {
			s.log.Warn("task dropped: the queue is full", "title", t.Title, "waiting", len(w.queue))
			return
		}
		w.qseq++
		q := &job{key: fmt.Sprintf("queued-%d", w.qseq), title: t.Title, lane: LaneTask, state: "queued", req: t}
		if front {
			w.queue = append([]*job{q}, w.queue...)
		} else {
			w.queue = append(w.queue, q)
		}
		return
	}
	w.mu.Unlock()
	s.launchTask(ctx, t)
}

// nextTasks starts the tasks that wait while there is room.
func (s *Supervisor) nextTasks(ctx context.Context) {
	for {
		w := &s.work
		w.mu.Lock()
		running := 0
		for _, j := range w.running {
			if j.lane == LaneTask {
				running++
			}
		}
		if len(w.queue) == 0 || running >= s.cfg.MaxTasks {
			w.mu.Unlock()
			return
		}
		next := w.queue[0]
		w.queue = w.queue[1:]
		w.mu.Unlock()
		s.launchTask(ctx, next.req)
	}
}

// launchTask runs a task in a turn of its own with an empty conversation that
// holds only its goal, beside the lanes. It is not run under a hold rule or at
// a ceiling.
func (s *Supervisor) launchTask(ctx context.Context, t tools.TaskRequest) {
	j, jctx := s.begin(ctx, LaneTask, t.Title, t)
	label := "task: " + t.Title
	held := func(why string) {
		s.end(j)
		s.log.Info("task not run", "title", t.Title, "why", why)
		s.reportWakeAs(j.key, t.Title, label, LaneTask, why, agent.Result{}, time.Time{}, why)
		s.reportStats(true)
	}
	if s.paused() {
		held("ceiling")
		return
	}
	rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
	rules, hold, rerr := s.core.StandingNotes(rctx, s.cfg.Name)
	rcancel()
	if rerr != nil && ctx.Err() == nil {
		s.log.Warn("standing notes not read; the task goes on without them", "err", rerr)
	}
	if hold {
		held("hold")
		return
	}
	text := prompt.Turn(prompt.Wake{
		Reason: prompt.Task, Now: s.now(), ChatName: s.messengerName(), Digest: s.digestLines(), Rules: rules,
		Title: t.Title, Goal: t.Goal, TokensUsed: s.led.Usage().Tokens, TokensCeil: s.cfg.DailyTokenCeil,
	})
	s.rollDay()
	s.wakes["task"]++
	s.log.Info("task start", "title", t.Title, "wake", j.key)
	runner := s.newTask()
	s.twg.Add(1)
	go func() {
		defer s.twg.Done()
		res := runner.Run(jctx, agent.Turn{Reason: string(prompt.Task), Prompt: text, Task: true})
		s.end(j)
		s.taskc <- taskEnd{j: j, res: res}
	}()
	s.reportStats(true)
}

// drainTasks accounts the tasks that ended while the loop was busy.
func (s *Supervisor) drainTasks(ctx context.Context) {
	for {
		select {
		case e := <-s.taskc:
			s.finishTask(ctx, e)
		default:
			return
		}
	}
}

// finishTask accounts a task that ended: its tokens first, whatever happened;
// its one-line result for the digest of the turn that started it; the line of
// activity; and the task that takes its place.
func (s *Supervisor) finishTask(ctx context.Context, e taskEnd) {
	res := e.res
	s.book(res)
	if ctx.Err() != nil {
		return // cut off by shutdown
	}
	w := &s.work
	w.mu.Lock()
	stopped, restart, killed := e.j.stopped, e.j.restart, e.j.killed
	w.mu.Unlock()
	failed := (res.Failed || res.CoreDown) && !res.Canceled
	summary := res.Summary
	switch {
	case failed:
		summary = "failed: " + failureText(res, s.cfg.LLMAuth)
	case stopped && summary == "":
		summary = "stopped by an operator before it finished"
	}
	s.log.Info("task done", "title", e.j.title, "wake", e.j.key, "steps", res.Steps, "toolCalls", res.ToolCalls, "failed", failed, "stopped", stopped, "billable", res.Usage.Billable())
	s.reportWakeAs(e.j.key, e.j.title, "task: "+e.j.title, LaneTask, turnOutcome(failed), res, time.Time{}, "task")
	if !killed {
		// The result is for the turn that started the task: its digest. A task read
		// the room on its own account and moves no watermark.
		s.addDigest(prompt.Reason("task "+e.j.title), agent.Result{Summary: summary})
	}
	if restart {
		s.startTask(ctx, e.j.req, true)
	}
	s.nextTasks(ctx)
	s.reportStats(true)
}
