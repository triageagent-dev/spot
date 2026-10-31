package supervisor

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/prompt"
	"github.com/triageagent-dev/spot/internal/tools"
	"github.com/triageagent-dev/spot/internal/triggers"
)

// A task actor (the core's ADR-109) is an actor the core made for one piece of
// work that Tria asked for. It has no schedule, no rooms to poll and no lanes:
// one goal, one turn, then the result goes where Tria reads it. The core is told
// by the actor's A2A task ending (triggers.TaskServer), not by a report.

// TaskCore is what the task leaves in the workspace: the result as one memory
// item, and one line in the bus that carries the item's id.
type TaskCore interface {
	RememberTask(ctx context.Context, key, title, result, spot string) (string, error)
	SayInBus(ctx context.Context, text, replyTo, memory string, hop int) error
}

// taskReplyHop is the hop of a task's answer. The task frame does not carry the
// hop of the request, which Tria made at hop 1: its answer is 2.
const taskReplyHop = 2

// maxBusLine is how much of the result the line in the bus carries; the memory
// item holds the rest.
const maxBusLine = 300

// RunTask runs the task a task actor was made for and returns how it ended: the
// memory item and the bus line it left, and whether the run failed. It returns
// an error only when the actor cannot do what it is for (the core does not offer
// what it needs); a task that fails is a result like any other and Tria is told
// so.
func (s *Supervisor) RunTask(ctx context.Context, t triggers.Task) (triggers.TaskResult, error) {
	tc, ok := s.core.(TaskCore)
	if !ok {
		return triggers.TaskResult{}, errors.New("the core client cannot leave a task result")
	}
	title := t.Title
	if title == "" {
		title = taskTitle(t.Goal)
	}
	s.setState("running")
	rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
	rules, _, rerr := s.core.StandingNotes(rctx, s.cfg.Name) // a rule to check nothing holds for the wake of a spot, not for a request Tria made
	rcancel()
	if rerr != nil && ctx.Err() == nil {
		s.log.Warn("standing notes not read; the task goes on without them", "err", rerr)
	}
	text := prompt.Turn(prompt.Wake{
		Reason: prompt.Task, Now: s.now(), Rules: rules, Title: title, Goal: t.Goal, ReplyTo: t.ReplyTo,
		TokensCeil: s.cfg.DailyTokenCeil,
	})
	j, jctx := s.begin(ctx, LaneTask, title, tools.TaskRequest{Title: title, Goal: t.Goal})
	s.work.mu.Lock()
	if s.work.stopAsked { // a stop that came before the turn did
		j.stopped = true
		j.cancel()
	}
	s.work.mu.Unlock()
	s.log.Info("task start", "wake", j.key, "replyTo", t.ReplyTo != "")
	res := s.turner.Run(jctx, agent.Turn{Reason: string(prompt.Task), Prompt: text, Task: true, NotesRead: rerr == nil})
	stopped, _ := s.end(j)
	s.book(res)

	failed := (res.Failed || res.CoreDown) && !res.Canceled
	result := strings.TrimSpace(res.Summary)
	switch {
	case failed:
		result = "failed: " + failureText(res, s.cfg.LLMAuth)
	case stopped && result == "":
		result = "stopped by an operator before it finished"
	case result == "":
		result = "the task ended without a result"
	}
	s.log.Info("task done", "wake", j.key, "steps", res.Steps, "toolCalls", res.ToolCalls, "failed", failed, "billable", res.Usage.Billable())

	// The result is left where Tria reads it, even when the task failed: she tells
	// the person. A fresh context: the task may have ended because ctx did.
	dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
	defer dcancel()
	id, err := tc.RememberTask(dctx, t.Key, title, result, s.cfg.Name)
	line := oneLine(result, maxBusLine)
	if err != nil {
		s.log.Error("the task result could not be saved to memory", "err", err)
		line, id = oneLine(result, maxBusLine-60)+" (the full result could not be saved)", ""
	}
	if err := tc.SayInBus(dctx, line, t.ReplyTo, id, taskReplyHop); err != nil {
		s.log.Error("the task result could not be sent to the bus", "err", err)
		failed = true
	}

	if s.flush != nil {
		s.flush() // the spans of the task leave before the core ends the actor
	}
	s.setState("idle")
	return triggers.TaskResult{Memory: id, Line: line, Failed: failed}, nil
}

// StopTask ends the turn of the task this actor runs at its next step, as an
// operator's stop does for a task of a deployment spot. A stop that comes before
// the turn has begun ends it as it begins.
func (s *Supervisor) StopTask() {
	w := &s.work
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopAsked = true
	for _, j := range w.running {
		if j.lane == LaneTask {
			j.stopped = true
			j.cancel()
		}
	}
}

// taskTitle is the request line of a goal: its first line, cut.
func taskTitle(goal string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(goal), "\n")
	return clipBytes(strings.TrimSpace(line), triggers.MaxTitle)
}

// oneLine is s as a single line of at most n bytes.
func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return clipBytes(s, n-3) + "..."
}
