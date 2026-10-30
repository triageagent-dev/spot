package supervisor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/triageagent-dev/spot/internal/config"
	"github.com/triageagent-dev/spot/internal/triggers"
)

// taskHarness starts a supervisor whose first turn asks for the given tasks
// (a JSON list of title and goal) and whose tasks run on their own turner.
func taskHarness(t *testing.T, tasks string, mode string, maxTasks int) (*harness, *scriptTurner, *fakeTrigger) {
	t.Helper()
	trig := newFakeTrigger(true)
	h := newHarness(t, func(c *config.Config) { c.PollInterval = 5 * time.Millisecond; c.MaxTasks = maxTasks })
	h.trig = trig
	h.set("tasks", tasks)
	h.set("next", `{"after":"2h","why":"fake"}`)
	tk := newScriptTurner(time.Minute)
	tk.set("mode", mode)
	h.task = tk
	h.start()
	return h, tk, trig
}

func (h *harness) control(op, key string) triggers.ControlResult {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	return h.sup.Control(ctx, triggers.Control{ID: "c", Op: op, Task: key})
}

func titles(ls []triggers.TaskLine) string {
	var out []string
	for _, l := range ls {
		out = append(out, l.Title+":"+l.State)
	}
	return strings.Join(out, ",")
}

func TestATaskRunsBesideTheTurnThatStartedItAndItsResultIsInTheDigest(t *testing.T) {
	h, tk, trig := taskHarness(t, `[{"title":"check checkout","goal":"look at the checkout pods and report"}]`, "ok", 3)
	tk.set("summary", "all pods ready")
	defer h.stop()
	h.waitFor("the task's turn", func() bool { return tk.turnCount() == 1 })
	in := tk.prompt(1)
	for _, want := range []string{"Wake reason: task", "check checkout", "look at the checkout pods and report", "You are a task"} {
		if !strings.Contains(in, want) {
			t.Errorf("task prompt lacks %q:\n%s", want, in)
		}
	}
	if !tk.turns[1].Task {
		t.Error("the task's turn was not marked as a task's")
	}
	h.waitFor("its line of activity", func() bool {
		for _, w := range trig.wakeLines() {
			if w.Lane == "task" {
				return w.Outcome == "turn" && w.Key != "" && w.Title == "check checkout" && w.Next == 0 && len(w.Promises) == 0
			}
		}
		return false
	})
	h.waitFor("its digest entry", func() bool {
		for _, d := range h.sup.led.LoadDigest() {
			if d.Reason == "task check checkout" && d.Summary == "all pods ready" {
				return true
			}
		}
		return false
	})
	// It is booked: the long turn's 150 tokens and the task's.
	h.waitFor("its tokens", func() bool { return h.sup.led.Usage().Tokens == 300 })
	if got := h.sup.taskLines(); len(got) != 0 {
		t.Errorf("a task that ended is still listed: %v", titles(got))
	}
}

func TestTasksPastTheLimitWaitAndThoseBeyondTheQueueAreDropped(t *testing.T) {
	var reqs []string
	for i := 1; i <= 13; i++ {
		reqs = append(reqs, fmt.Sprintf(`{"title":"t%d","goal":"g%d"}`, i, i))
	}
	h, tk, _ := taskHarness(t, "["+strings.Join(reqs, ",")+"]", "sleep", 1)
	defer h.stop()
	h.waitFor("one task running", func() bool { return tk.turnCount() == 1 })
	time.Sleep(quiet)
	lines := h.sup.taskLines()
	if len(lines) != 1+MaxQueuedTasks || lines[0].State != "running" || lines[1].State != "queued" || lines[1].Title != "t2" {
		t.Fatalf("one running, %d waiting, the rest dropped: %s", MaxQueuedTasks, titles(lines))
	}
	if tk.turnCount() != 1 {
		t.Errorf("more than the limit ran at once: %d", tk.turnCount())
	}
}

func TestStoppingATaskLetsTheNextOneWaitingStart(t *testing.T) {
	h, tk, trig := taskHarness(t, `[{"title":"a","goal":"ga"},{"title":"b","goal":"gb"}]`, "sleep", 1)
	defer h.stop()
	h.waitFor("a to run", func() bool { return tk.turnCount() == 1 })
	time.Sleep(quiet)

	list := h.control("list", "")
	if !list.OK || list.ID != "c" || titles(list.Tasks) != "a:running,b:queued" {
		t.Fatalf("list: %+v", list)
	}
	a, b := list.Tasks[0].Key, list.Tasks[1].Key

	if r := h.control("restart", b); r.OK || !strings.Contains(r.Msg, "has not started") {
		t.Errorf("restart of a task that waits: %+v", r)
	}
	if r := h.control("drop", a); r.OK || !strings.Contains(r.Msg, "only a task that has not started") {
		t.Errorf("drop of a task that runs: %+v", r)
	}
	if r := h.control("stop", "nobody-wake-9"); r.OK || !strings.Contains(r.Msg, "is not running or waiting") {
		t.Errorf("stop of a key the spot does not have: %+v", r)
	}
	if r := h.control("pause", a); r.OK || !strings.Contains(r.Msg, "not one of") {
		t.Errorf("an operation that does not exist: %+v", r)
	}

	// Stop a: it ends at once, says so in the digest, and b takes its place.
	tk.set("mode.2", "sleep")
	r := h.control("stop", a)
	if !r.OK || !strings.HasPrefix(r.Msg, "stop done") {
		t.Fatalf("stop: %+v", r)
	}
	h.waitFor("b to run", func() bool { return tk.turnCount() == 2 })
	h.waitFor("a's digest entry", func() bool {
		for _, d := range h.sup.led.LoadDigest() {
			if d.Reason == "task a" && d.Summary == "stopped by an operator before it finished" {
				return true
			}
		}
		return false
	})
	if got := titles(h.sup.taskLines()); got != "b:running" {
		t.Errorf("after the stop: %s", got)
	}
	var ended bool
	for _, w := range trig.wakeLines() {
		ended = ended || (w.Lane == "task" && w.Title == "a" && w.Outcome == "turn")
	}
	if !ended {
		t.Errorf("a stopped task is not a failure: %+v", trig.wakeLines())
	}
}

func TestAnOperatorCanDropAWaitingTaskKillOneAndRestartOne(t *testing.T) {
	h, tk, _ := taskHarness(t, `[{"title":"a","goal":"ga"},{"title":"b","goal":"gb"},{"title":"c","goal":"gc"}]`, "sleep", 1)
	defer h.stop()
	h.waitFor("a to run", func() bool { return tk.turnCount() == 1 })
	time.Sleep(quiet)
	lines := h.sup.taskLines()
	a, b := lines[0].Key, lines[1].Key

	if r := h.control("drop", b); !r.OK || titles(r.Tasks) != "a:running,c:queued" {
		t.Fatalf("drop: %+v", r)
	}
	// A restart: a ends and starts again, ahead of c.
	if r := h.control("restart", a); !r.OK {
		t.Fatalf("restart: %+v", r)
	}
	h.waitFor("a to run again", func() bool { return tk.turnCount() == 2 })
	if got := tk.prompt(2); !strings.Contains(got, "Task: a") {
		t.Errorf("the restarted task is not a:\n%s", got)
	}
	time.Sleep(quiet)
	lines = h.sup.taskLines()
	if titles(lines) != "a:running,c:queued" || lines[0].Key == a {
		t.Fatalf("after the restart: %s (key %s was %s)", titles(lines), lines[0].Key, a)
	}
	// A kill takes it off the list at once; its place goes to c.
	killed := lines[0].Key
	if r := h.control("kill", killed); !r.OK || !strings.HasPrefix(r.Msg, "kill done") {
		t.Fatalf("kill: %+v", r)
	}
	h.waitFor("c to run", func() bool { return tk.turnCount() == 3 })
	for _, l := range h.sup.taskLines() {
		if l.Key == killed {
			t.Errorf("a killed task is still listed: %+v", l)
		}
	}
}

func TestAnOperatorCanStopTheLongTurnAndTheSpotGoesOn(t *testing.T) {
	trig := newFakeTrigger(true)
	h := newHarness(t, func(c *config.Config) { c.PollInterval = 5 * time.Millisecond })
	h.trig = trig
	h.set("mode.1", "sleep")
	h.set("next", `{"after":"3m","why":"fake"}`)
	h.start()
	defer h.stop()
	h.waitFor("the long turn", func() bool { return h.script.hasStarted(1) })
	lines := h.sup.taskLines()
	if len(lines) != 1 || lines[0].Lane != LaneLong || lines[0].State != "running" {
		t.Fatalf("the wake in hand: %+v", lines)
	}
	if r := h.control("restart", lines[0].Key); r.OK || !strings.Contains(r.Msg, "only a task can be restarted") {
		t.Errorf("restart of a wake: %+v", r)
	}
	if r := h.control("stop", lines[0].Key); !r.OK {
		t.Fatalf("stop: %+v", r)
	}
	h.waitIdle(1)
	if st := h.status(); st.State != "idle" || st.NextWake == nil {
		t.Fatalf("a stopped turn is not the supervisor stopping: %+v", st)
	}
	h.clock.Advance(4 * time.Minute)
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick"}})
	h.waitIdle(2)
}

func TestATaskDoesNotRunWhileACeilingHoldsTheSpotBack(t *testing.T) {
	trig := newFakeTrigger(true)
	h := newHarness(t, func(c *config.Config) { c.PollInterval = 5 * time.Millisecond; c.MaxTasks = 3; c.DailyTokenCeil = 100 })
	h.trig = trig
	h.set("tasks", `[{"title":"a","goal":"ga"}]`)
	tk := newScriptTurner(time.Minute)
	h.task = tk
	h.start()
	defer h.stop()
	h.waitFor("the task's line", func() bool {
		for _, w := range trig.wakeLines() {
			if w.Lane == "task" {
				return w.Outcome == "ceiling" && w.Key != ""
			}
		}
		return false
	})
	if tk.turnCount() != 0 {
		t.Error("the task ran past the ceiling")
	}
}
