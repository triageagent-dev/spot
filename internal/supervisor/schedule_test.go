package supervisor

import (
	"strings"
	"testing"
	"time"

	"github.com/triageagent-dev/spot/internal/config"
	"github.com/triageagent-dev/spot/internal/ledger"
	"github.com/triageagent-dev/spot/internal/triggers"
)

// quiet is how long a test waits to show that nothing happened.
const quiet = 120 * time.Millisecond

func streamHarness(t *testing.T, connected bool) (*harness, *fakeTrigger) {
	t.Helper()
	trig := newFakeTrigger(connected)
	h := newHarness(t, func(c *config.Config) { c.PollInterval = 5 * time.Millisecond })
	h.trig = trig
	h.set("next", `{"after":"3m","why":"fake"}`)
	return h, trig
}

func TestWithTheStreamUpTheCoreSendsTheTickAndTheSpotKeepsNoTimerForIt(t *testing.T) {
	h, trig := streamHarness(t, true)
	h.start()
	defer h.stop()
	h.waitIdle(1)

	// The planned time passes, and the spot waits for the core.
	h.clock.Advance(10 * time.Minute)
	time.Sleep(quiet)
	if h.turns() != 1 {
		t.Fatalf("the spot woke itself at the planned time with the stream up: %d turns", h.turns())
	}
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick", Why: "the core's copy of the words"}})
	h.waitIdle(2)
	if in := h.file("stdin", 2); !strings.Contains(in, "Wake reason: tick") || !strings.Contains(in, "Your reason: the core's copy of the words") {
		t.Errorf("the ordered wake:\n%s", in)
	}
}

func TestACoreThatSendsNoTickIsNotWaitedForPastMaxWake(t *testing.T) {
	h, _ := streamHarness(t, true)
	h.start()
	defer h.stop()
	h.waitIdle(1)

	h.clock.Advance(time.Hour)
	time.Sleep(quiet)
	if h.turns() != 1 {
		t.Fatalf("the net caught the spot before maxWake: %d turns", h.turns())
	}
	h.clock.Advance(61 * time.Minute) // two hours since the last wake
	h.waitIdle(2)
	if in := h.file("stdin", 2); !strings.Contains(in, "Wake reason: tick") {
		t.Errorf("prompt:\n%s", in)
	}
}

func TestWithoutTheStreamTheSpotWakesItselfAtThePlannedTime(t *testing.T) {
	h, _ := streamHarness(t, false)
	h.cfg.PollInterval = 5 * time.Millisecond
	h.start()
	defer h.stop()
	h.waitIdle(1)
	h.clock.Advance(4 * time.Minute)
	h.waitIdle(2)
}

func TestATickPlannedLaterIsStale(t *testing.T) {
	h, trig := streamHarness(t, true)
	h.start()
	defer h.stop()
	h.waitIdle(1)

	// The plan is 3 minutes ahead: a tick that arrives now belongs to a plan the
	// spot has already replaced.
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "tick", Why: "old"}})
	time.Sleep(quiet)
	if h.turns() != 1 {
		t.Fatalf("a stale tick woke the spot: %d turns", h.turns())
	}
	// Nor does an unknown reason, nor a promise the spot does not hold.
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "whenever"}})
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "promise"}})
	time.Sleep(quiet)
	if h.turns() != 1 {
		t.Fatalf("an order the spot cannot place woke it: %d turns", h.turns())
	}
}

func TestAPromiseTheCoreSendsRunsThePromisedWake(t *testing.T) {
	h, trig := streamHarness(t, true)
	led, err := ledger.New(h.state)
	must(t, err)
	led.SetClock(h.clock.Now)
	must(t, led.SavePromise(ledger.Promise{At: h.clock.Now().Add(3 * time.Minute), Why: "give the note"}))
	h.start()
	defer h.stop()
	h.waitIdle(1)

	h.clock.Advance(4 * time.Minute)
	trig.say(triggers.Pending{Order: &triggers.Order{Reason: "promise", Why: "give the note"}})
	h.waitIdle(2)
	if in := h.file("stdin", 2); !strings.Contains(in, "Your reason: give the note") {
		t.Errorf("the promised wake:\n%s", in)
	}
	if got := h.sup.led.LoadPromise(); !got.At.IsZero() {
		t.Errorf("a wake that fired is still promised: %+v", got)
	}
}

func TestEveryWakeReportCarriesThePlanAndThePromise(t *testing.T) {
	h, trig := streamHarness(t, true)
	led, err := ledger.New(h.state)
	must(t, err)
	led.SetClock(h.clock.Now)
	due := h.clock.Now().Add(90 * time.Minute)
	must(t, led.SavePromise(ledger.Promise{At: due, Why: "give the note"}))
	h.set("next", `{"after":"2h","why":"fake"}`) // later than the promise, which stays
	h.start()
	defer h.stop()
	h.waitIdle(1)
	h.waitFor("the wake line", func() bool { return len(trig.wakeLines()) == 1 })

	w := trig.wakeLines()[0]
	// The promise comes before the plan the agent asked for: it is the next wake.
	if w.Next != due.UnixMilli() || w.NextWhy != "give the note" || w.NextSource != "agent" {
		t.Errorf("the plan: %+v", w)
	}
	if w.Promise == nil || w.Promise.At != due.UnixMilli() || w.Promise.Why != "give the note" || !w.Promise.Human {
		t.Errorf("the promise: %+v", w.Promise)
	}
	trig.mu.Lock()
	sc := trig.schedules[len(trig.schedules)-1]
	trig.mu.Unlock()
	if sc.Next != w.Next || sc.NextWhy != "give the note" || sc.MaxWakeMs != (2*time.Hour).Milliseconds() || sc.Promise == nil || sc.BackoffMs != 0 {
		t.Errorf("the schedule the next hello is answered with: %+v", sc)
	}
}

func TestAFailedWakeReportsItsBackoffInTheSchedule(t *testing.T) {
	h, trig := streamHarness(t, true)
	h.set("mode", "fail")
	h.start()
	defer h.stop()
	h.waitFor("the failed wake", func() bool { return h.status().Failures == 1 })
	trig.mu.Lock()
	sc := trig.schedules[len(trig.schedules)-1]
	trig.mu.Unlock()
	if sc.BackoffMs != (2*time.Minute).Milliseconds() || sc.Next == 0 {
		t.Errorf("schedule after a failure: %+v", sc)
	}
}

func TestAShortLaneReportKeepsThePlanItDoesNotChange(t *testing.T) {
	h, trig, g := lanes(t)
	defer h.stop()
	g.set(func() { g.gated = true })
	h.post("team", say("alice", "alice", "@example-spot look at checkout"))
	trig.say(triggers.Pending{Rooms: []string{"team"}})
	h.waitFor("the long turn to run", g.isRunning)
	h.post("team", say("alice", "alice", "@example-spot are you there?"))
	trig.say(triggers.Pending{Rooms: []string{"team"}})
	h.waitFor("the short lane", func() bool { return h.fast.turnCount() == 1 })
	h.waitFor("its line", func() bool {
		for _, w := range trig.wakeLines() {
			if w.Lane == "short" {
				return true
			}
		}
		return false
	})
	for _, w := range trig.wakeLines() {
		if w.Lane == "short" && w.Next == 0 {
			t.Errorf("a short-lane line would wipe the core's plan: %+v", w)
		}
	}
	g.set(func() { g.gated = false })
	close(g.release)
	h.waitIdle(2)
}
