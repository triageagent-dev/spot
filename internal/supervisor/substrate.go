package supervisor

import (
	"encoding/json"
	"time"

	"github.com/triageagent-dev/spot/internal/edges"
	"github.com/triageagent-dev/spot/internal/ledger"
)

// The parts of the supervisor that exist because a substrate actor (the core's
// ADR-107) is suspended whenever it is idle and starts cold on the next wake: a
// process holds nothing between wakes but what is on its durable directory.

// Idler is a trigger that keeps its reports until the supervisor is idle and
// then sends them all (internal/triggers.Link).
type Idler interface{ Idle() }

// busy reports whether the table holds a wake that runs or a task that waits.
func (w *work) busy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.running) > 0 || len(w.queue) > 0
}

// quiescent reports whether nothing is running and nothing is waiting for the
// core's request to be taken. Held edges do not count: an actor keeps none
// across a suspension, and the next wake reads the core.
func (s *Supervisor) quiescent() bool {
	return s.side == nil && !s.readNext && !s.work.busy()
}

// idle is the supervisor having nothing left to do: its window is written to the
// durable directory, then the trigger sends what it kept, the wake report last.
// The core may suspend the actor as soon as it has that report.
func (s *Supervisor) idle() {
	if !s.cfg.Substrate() || s.trig == nil || !s.quiescent() {
		return
	}
	if len(s.trig.C()) > 0 {
		// The core has already asked for another wake: the loop takes it first,
		// and the report waits for the end of that one.
		return
	}
	s.saveWindow()
	if i, ok := s.trig.(Idler); ok {
		i.Idle()
	}
}

// loadWindow takes back what the last process of the actor left.
func (s *Supervisor) loadWindow() {
	w := s.led.LoadWindow()
	for _, r := range w.Recent {
		s.recent = append(s.recent, tokenAt{at: r.At, tokens: r.Tokens})
	}
	s.edgeWakes = append(s.edgeWakes, w.EdgeWakes...)
	if len(w.Edges) > 0 {
		var snap edges.Snapshot
		if err := json.Unmarshal(w.Edges, &snap); err != nil {
			s.log.Warn("the saved issue baseline is unreadable; the next look starts a new one", "err", err)
			return
		}
		s.tracker.Restore(snap)
	}
}

// saveWindow writes the window.
func (s *Supervisor) saveWindow() {
	now := s.now()
	w := ledger.Window{}
	for _, r := range s.recent {
		if now.Sub(r.at) < time.Hour {
			w.Recent = append(w.Recent, ledger.WindowSample{At: r.at, Tokens: r.tokens})
		}
	}
	for _, t := range s.edgeWakes {
		if now.Sub(t) < time.Hour {
			w.EdgeWakes = append(w.EdgeWakes, t)
		}
	}
	if snap := s.tracker.Snapshot(); snap != nil {
		if b, err := json.Marshal(snap); err == nil {
			w.Edges = b
		}
	}
	if err := s.led.SaveWindow(w); err != nil {
		s.log.Error("saving the window", "err", err)
	}
}
