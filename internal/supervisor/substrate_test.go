package supervisor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/config"
	"github.com/triageagent-dev/spot/internal/ledger"
	"github.com/triageagent-dev/spot/internal/triggers"
)

func substrateHarness(t *testing.T) (*harness, *fakeTrigger) {
	t.Helper()
	trig := newFakeTrigger(true)
	h := newHarness(t, func(c *config.Config) {
		c.Runtime = config.RuntimeSubstrate
		c.PollInterval = time.Hour
		c.MaxEdgeWakesHr = 0
	})
	h.trig = trig
	h.set("next", `{"after":"2h"}`)
	return h, trig
}

func TestAnActorStartsNoWakeOfItsOwnAndWaitsForTheCore(t *testing.T) {
	h, trig := substrateHarness(t)
	h.start()
	defer h.stop()

	// No startup wake, and no timer: the core's request is the only wake.
	time.Sleep(150 * time.Millisecond)
	if h.turns() != 0 || len(trig.wakes) != 0 {
		t.Fatalf("the actor woke itself: %d turns, %d wake reports", h.turns(), len(trig.wakes))
	}
	h.clock.Advance(48 * time.Hour)
	time.Sleep(100 * time.Millisecond)
	if h.turns() != 0 {
		t.Fatalf("a timer woke the actor")
	}
}

func TestTheCoresOrderIsTakenAsItIsBecauseTheProcessKnowsNoPlan(t *testing.T) {
	h, trig := substrateHarness(t)
	h.start()
	defer h.stop()

	// A promise the cold process never made, and a tick it never planned.
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "promise", Why: "give the person the note"}})
	h.waitIdle(1)
	if in := h.file("stdin", 1); !strings.Contains(in, "give the person the note") {
		t.Errorf("the core's words did not reach the prompt:\n%s", in)
	}
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick", Why: "look around"}})
	h.waitIdle(2)
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "whenever"}})
	time.Sleep(150 * time.Millisecond)
	if h.turns() != 2 {
		t.Errorf("a reason the spot does not know ran a turn: %d", h.turns())
	}
}

// idleProbe notes how many times the supervisor said it was idle while a turn
// was running.
type idleProbe struct {
	Turner
	trig   *fakeTrigger
	during chan int
}

func (p *idleProbe) Run(ctx context.Context, t agent.Turn) agent.Result {
	p.during <- p.trig.idleCount()
	time.Sleep(100 * time.Millisecond) // the supervisor's loop goes on beside the turn
	p.during <- p.trig.idleCount()
	return p.Turner.Run(ctx, t)
}

func TestTheTriggerIsToldTheSpotIsIdleOnlyWhenNothingRuns(t *testing.T) {
	h, trig := substrateHarness(t)
	probe := &idleProbe{trig: trig, during: make(chan int, 4)}
	h.wrap = func(tn Turner) Turner { probe.Turner = tn; return probe }
	h.start()
	defer h.stop()

	h.waitFor("the first idle", func() bool { return trig.idleCount() > 0 })
	before := trig.idleCount()
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick"}})
	a, b := <-probe.during, <-probe.during
	if a != before || b != before {
		t.Errorf("idle was said during a turn: %d before, %d at its start, %d later", before, a, b)
	}
	h.waitIdle(1)
	h.waitFor("idle after the turn", func() bool { return trig.idleCount() > before })
	// The wake report was handed to the trigger before the idle that lets it go.
	trig.mu.Lock()
	n := len(trig.wakes)
	trig.mu.Unlock()
	if n == 0 {
		t.Error("no wake report was kept for the idle")
	}
}

func TestAStreamSpotNeverSaysItIsIdleAndKeepsNoWindow(t *testing.T) {
	trig := newFakeTrigger(true)
	h := newHarness(t, func(c *config.Config) { c.PollInterval = time.Hour })
	h.trig = trig
	h.set("next", `{"after":"2h"}`)
	h.start()
	defer h.stop()
	h.waitIdle(1)
	time.Sleep(100 * time.Millisecond)
	if trig.idleCount() != 0 {
		t.Error("a spot with a stream was made to report on idle")
	}
	if _, err := os.Stat(filepath.Join(h.state, ledger.SupervisorDir, ledger.WindowFile)); err == nil {
		t.Error("a window was written for a spot that is not suspended")
	}
}

func TestWhatASuspensionWouldLoseIsOnTheDurableDirectory(t *testing.T) {
	h, trig := substrateHarness(t)
	h.core.setIssues(issue("a", "warning", 0))
	h.start()
	defer h.stop()

	// A wake reads the core: the baseline is the issue state it saw.
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick"}})
	h.waitIdle(1)
	h.waitFor("the window to be written", func() bool {
		w := h.sup.led.LoadWindow()
		return len(w.Edges) > 0 && len(w.Recent) > 0
	})
	h.stop()

	// The next process starts cold, and a new issue is news against that baseline.
	h.core.setIssues(issue("a", "warning", 0), issue("b", "critical", 0))
	h.start()
	trig.say(triggers.Pending{Rooms: []string{"bus"}})
	h.waitFor("an edge wake for the issue that came meanwhile", func() bool { return h.turns() >= 2 })
	if in := h.file("stdin", 2); !strings.Contains(in, "new issue") {
		t.Fatalf("the restored baseline did not tell the new issue:\n%s", in)
	}
}

func TestTheSpansAreFlushedBeforeTheReportsLeave(t *testing.T) {
	h, trig := substrateHarness(t)
	var order []string
	var mu sync.Mutex
	trig.onIdle = func() { mu.Lock(); order = append(order, "idle"); mu.Unlock() }
	h.flush = func() { mu.Lock(); order = append(order, "flush"); mu.Unlock() }
	h.start()
	defer h.stop()
	h.waitFor("the first idle", func() bool { return trig.idleCount() > 0 })
	before := trig.idleCount()
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick"}})
	h.waitIdle(1)
	h.waitFor("an idle after the turn", func() bool { return trig.idleCount() > before })
	mu.Lock()
	defer mu.Unlock()
	for i, s := range order {
		if s == "idle" && (i == 0 || order[i-1] != "flush") {
			t.Fatalf("the reports left before the spans were flushed: %v", order)
		}
	}
}

func TestASubstrateSpotAsksTheCoreToMakeAnActorForATask(t *testing.T) {
	h, trig := substrateHarness(t)
	h.task = newScriptTurner(time.Minute)
	h.set("tasks", `[{"title":"check the cart","goal":"look at the cart pods and report"}]`)
	h.start()
	defer h.stop()
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick"}})
	h.waitIdle(1)
	h.waitFor("the core to be asked", func() bool { trig.mu.Lock(); defer trig.mu.Unlock(); return len(trig.started) == 1 })
	time.Sleep(100 * time.Millisecond)
	if h.task.turnCount() != 0 {
		t.Fatal("the task ran in the spot: it is the core's actor to run")
	}
	trig.mu.Lock()
	got := trig.started[0]
	trig.mu.Unlock()
	if got != "check the cart|look at the cart pods and report" {
		t.Fatalf("%q", got)
	}
	// The next wake knows it was asked, and that Tria reads the result.
	h.set("tasks", "")
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick"}})
	h.waitIdle(2)
	if in := h.file("stdin", 2); !strings.Contains(in, "Tria reads its result") {
		t.Fatalf("the digest must say so:\n%s", in)
	}
}

func TestATaskTheCoreWillNotMakeIsInTheDigest(t *testing.T) {
	h, trig := substrateHarness(t)
	trig.startErr = errors.New("the core did not start the task (status 403)")
	h.set("tasks", `[{"title":"check the cart","goal":"g"}]`)
	h.start()
	defer h.stop()
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick"}})
	h.waitIdle(1)
	h.set("tasks", "")
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick"}})
	h.waitIdle(2)
	if in := h.file("stdin", 2); !strings.Contains(in, "not started: the core did not start the task") {
		t.Fatalf("a refused task is in the digest:\n%s", in)
	}
}
