package triggers

import (
	"context"
	"strings"
	"sync"
	"unicode"
)

// inbox is what the core has said since the supervisor last looked: it is the
// same for the websocket stream (Client) and for the HTTP link of a substrate
// actor (Link). The mutex also guards what the owner keeps beside it.
type inbox struct {
	signal  chan struct{}
	mu      sync.Mutex
	pending Pending
	budget  *Budget
	// control answers the core's controls; see SetControl.
	cmu     sync.Mutex
	control ControlFunc
}

func newInbox() inbox { return inbox{signal: make(chan struct{}, 1)} }

// C signals that something is pending; it is coalesced, so one receive may
// stand for many frames.
func (b *inbox) C() <-chan struct{} { return b.signal }

// Take returns what happened since the last call and clears it.
func (b *inbox) Take() Pending {
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.pending
	b.pending = Pending{}
	return p
}

// Budget is the last budget the core sent, false before any.
func (b *inbox) Budget() (Budget, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.budget == nil {
		return Budget{}, false
	}
	return *b.budget, true
}

// SetControl sets what answers the core's controls. Without it every control is
// refused.
func (b *inbox) SetControl(f ControlFunc) {
	b.cmu.Lock()
	b.control = f
	b.cmu.Unlock()
}

func (b *inbox) wake() {
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

// add keeps what a frame from the core says.
func (b *inbox) add(f frame) {
	b.mu.Lock()
	if n := cleanName(f.Name); n != "" {
		b.pending.Name = n
	}
	if f.Budget != nil && sane(*f.Budget) {
		v := *f.Budget
		b.budget = &v
		b.pending.Budget = true
	}
	if o := f.Wake; o != nil && (o.Reason == "promise" || b.pending.Order == nil || b.pending.Order.Reason != "promise") {
		o.Why = strings.ToValidUTF8(clipTo(o.Why, maxOrderWhy), "")
		b.pending.Order = o
	}
	b.pending.Hello = b.pending.Hello || f.Hello
	b.pending.Issues = b.pending.Issues || f.Issues
	for _, r := range f.Rooms {
		seen := false
		for _, x := range b.pending.Rooms {
			seen = seen || x == r
		}
		if !seen {
			b.pending.Rooms = append(b.pending.Rooms, r)
		}
	}
	b.mu.Unlock()
	b.wake()
}

// markManual keeps a wake a person asked for from the console until the loop
// takes it.
func (b *inbox) markManual(why string) {
	why = strings.ToValidUTF8(clipTo(why, maxOrderWhy), "")
	b.mu.Lock()
	b.pending.Manual = &Order{Reason: "manual", Why: why}
	b.mu.Unlock()
	b.wake()
}

// manualAnswer asks for the wake and answers the control that carried it: "asked",
// with what the spot has in hand.
func (b *inbox) manualAnswer(id, why string) ControlResult {
	b.markManual(why)
	res := ControlResult{ID: id, OK: true, Msg: "wake asked"}
	b.cmu.Lock()
	f := b.control
	b.cmu.Unlock()
	if f != nil {
		ctx, cancel := context.WithTimeout(context.Background(), ControlTimeout)
		res.Tasks = f(ctx, Control{ID: id, Op: "list"}).Tasks
		cancel()
	}
	return res
}

// run applies a control the way the stream does and returns the answer: a wake
// is asked for, anything else goes to the spot's ControlFunc, bounded by
// ControlTimeout.
func (b *inbox) run(ctl Control) ControlResult {
	if ctl.Op == "wake" {
		return b.manualAnswer(ctl.ID, ctl.Why)
	}
	res := ControlResult{ID: ctl.ID}
	b.cmu.Lock()
	f := b.control
	b.cmu.Unlock()
	switch {
	case len(ctl.Task) > maxTaskKey:
		res.Msg = "the task key is too long"
	case f == nil:
		res.Msg = "this spot does not answer controls yet"
	default:
		ctx, cancel := context.WithTimeout(context.Background(), ControlTimeout)
		done := make(chan ControlResult, 1)
		go func() { done <- f(ctx, ctl) }()
		select {
		case res = <-done:
		case <-ctx.Done():
			res.Msg = "the spot did not finish in time; ask for the list to see where it stands"
		}
		cancel()
		res.ID = ctl.ID
	}
	return res
}

// maxName bounds the persona name the core gives the spot.
const maxName = 64

// maxTokensFigure bounds a figure of a budget frame: far above any real day of
// tokens, far below what would overflow a sum.
const maxTokensFigure = int64(1) << 40

// cleanName keeps a name the spot can match a mention against: valid text with
// no control characters, cut to maxName.
func cleanName(n string) string {
	n = strings.TrimSpace(strings.ToValidUTF8(clipTo(n, maxName), ""))
	if strings.ContainsFunc(n, unicode.IsControl) {
		return ""
	}
	return n
}

// sane reports whether the figures of a budget are ones the core could have
// counted. A frame that is not is dropped whole.
func sane(b Budget) bool {
	for _, v := range []int64{b.Spent, b.Own, b.Cap, b.Remaining} {
		if v < 0 || v > maxTokensFigure {
			return false
		}
	}
	return true
}
