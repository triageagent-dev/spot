package schedule

import (
	"strings"
	"testing"
	"time"
)

const (
	min = 2 * time.Minute
	max = 2 * time.Hour
)

func TestRequestClamps(t *testing.T) {
	cases := []struct {
		name  string
		after string
		want  time.Duration
		src   Source
	}{
		{"in range", "15m", 15 * time.Minute, SourceAgent},
		{"too short", "5s", min, SourceClamped},
		{"too long", "48h", max, SourceClamped},
		{"exactly min", "2m", min, SourceAgent},
		{"exactly max", "2h", max, SourceAgent},
		{"compound", "1h30m", 90 * time.Minute, SourceAgent},
		{"padded", " 10m ", 10 * time.Minute, SourceAgent},
		{"empty", "", min, SourceGarbled},
		{"bad duration", "soon", min, SourceGarbled},
		{"a bare number", "15", min, SourceGarbled},
		{"negative", "-5m", min, SourceGarbled},
		{"zero", "0s", min, SourceGarbled},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := Request(c.after, "why", min, max)
			if w.After != c.want || w.Source != c.src {
				t.Fatalf("got %s/%s (%s), want %s/%s", w.After, w.Source, w.Note, c.want, c.src)
			}
			if c.src != SourceAgent && w.Note == "" {
				t.Fatalf("a defaulted or clamped wake must explain itself")
			}
		})
	}
}

func TestWhyIsBounded(t *testing.T) {
	w := Request("10m", strings.Repeat("x", 5000), min, max)
	if len(w.Why) != maxWhy {
		t.Fatalf("why length %d, want %d", len(w.Why), maxWhy)
	}
}

func TestDefault(t *testing.T) {
	w := Default(min, SourceMissing, "no call")
	if w.After != min || w.Source != SourceMissing || w.Note != "no call" {
		t.Fatalf("%+v", w)
	}
}

func TestAfterTurnKeepsAnEarlierPromiseAndMakesAPersonsRequestOne(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	min, max := 10*time.Minute, 2*time.Hour

	// A person asks for a note in 3 minutes: not held to the floor, and a promise.
	p := AfterTurn(TurnEnd{Now: now, Min: min, Max: max, Human: true, Asked: true, After: "3m", Why: "the note"})
	if len(p.Promises) != 1 || p.Wake.After != 3*time.Minute || p.Promises[0].At != now.Add(3*time.Minute) || !p.Promises[0].Human || p.Promises[0].Why != "the note" {
		t.Fatalf("a person's request: %+v", p)
	}

	// An edge wake that wants the idle schedule does not push the promise back.
	q := AfterTurn(TurnEnd{Now: now.Add(time.Minute), Min: min, Max: max, Asked: true, After: "1h", Why: "idle", Promises: p.Promises})
	if q.At != p.Promises[0].At || q.Why != "the note" || q.Wake.Note == "" || len(q.Promises) != 1 {
		t.Fatalf("an earlier promise is kept: %+v", q)
	}

	// The promised wake itself clears it; the agent's own request is then not a human promise.
	r := AfterTurn(TurnEnd{Now: now.Add(3 * time.Minute), Min: min, Max: max, Tick: true, Asked: true, After: "1h", Why: "idle", Promises: p.Promises})
	if r.At != now.Add(3*time.Minute).Add(time.Hour) || len(r.Promises) != 0 {
		t.Fatalf("after the promised wake: %+v", r)
	}

	// No call: the minimum; a failure raises it to the backoff.
	f := AfterTurn(TurnEnd{Now: now, Min: min, Max: max, Failed: true, Backoff: 40 * time.Minute})
	if f.Wake.Source != SourceMissing || f.Wake.After != 40*time.Minute || len(f.Promises) != 0 || !f.Kept.None() {
		t.Fatalf("a failed turn: %+v", f)
	}
}

func TestAnEarlierWakeTheAgentAskedForOnItsOwnIsKeptButIsNoPromise(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	min, max := 10*time.Minute, 2*time.Hour
	a := AfterTurn(TurnEnd{Now: now, Min: min, Max: max, Asked: true, After: "30m", Why: "look again"})
	if len(a.Promises) != 0 || a.Kept.At != now.Add(30*time.Minute) {
		t.Fatalf("an agent's own wake: %+v", a)
	}
	b := AfterTurn(TurnEnd{Now: now.Add(time.Minute), Min: min, Max: max, Asked: true, After: "2h", Why: "routine", Kept: a.Kept})
	if b.At != a.Kept.At || b.Why != "look again" || len(b.Promises) != 0 {
		t.Fatalf("kept: %+v", b)
	}
}

func TestPromisesAreAListAndNoneMovesAnother(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	var ps Promises
	for _, d := range []time.Duration{72 * time.Hour, 24 * time.Hour, 96 * time.Hour} {
		var ok bool
		if ps, ok = ps.Add(now, Promise{At: now.Add(d), Why: "ask", Human: true}); !ok {
			t.Fatalf("%v refused", d)
		}
	}
	if len(ps) != 3 || ps[0].At != now.Add(24*time.Hour) || ps[2].At != now.Add(96*time.Hour) {
		t.Fatalf("Monday, Tuesday and Friday are three entries, earliest first: %+v", ps)
	}
	if got, ok := ps.Add(now, Promise{At: now.Add(24 * time.Hour), Why: "ask", Human: true}); ok || len(got) != 3 {
		t.Fatalf("the same promise twice: %+v", got)
	}
	if _, ok := ps.Add(now, Promise{At: now.Add(MaxPromiseAhead + time.Hour), Why: "too far"}); ok {
		t.Fatal("a promise beyond seven days was taken")
	}
	if _, ok := ps.Add(now, Promise{Why: "no time"}); ok {
		t.Fatal("a promise without a time was taken")
	}
	var full Promises
	for i := 0; i < MaxPromises; i++ {
		full, _ = full.Add(now, Promise{At: now.Add(time.Duration(i+1) * time.Minute), Why: "x"})
	}
	if len(full) != MaxPromises {
		t.Fatalf("held %d", len(full))
	}
	if _, ok := full.Add(now, Promise{At: now.Add(time.Hour), Why: "one more"}); ok {
		t.Fatal("a promise past the cap was taken")
	}
	if got := ps.Fired(now.Add(24 * time.Hour)); len(got) != 2 || got[0].At != now.Add(72*time.Hour) {
		t.Fatalf("a fired promise is removed, the others stay: %+v", got)
	}
	if !ps.Due(now.Add(24*time.Hour)) || ps.Due(now) {
		t.Fatal("due")
	}
}

func TestAfterShortTurnAddsAPromiseNothingMovesAnother(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	cur := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, true, "5m", "later", nil)
	if len(cur) != 1 || cur[0].At != now.Add(5*time.Minute) || !cur[0].Human {
		t.Fatalf("first: %+v", cur)
	}
	if got := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, true, "30m", "much later", cur); len(got) != 2 || got[0] != cur[0] {
		t.Fatalf("a later request must be one more entry: %+v", got)
	}
	if got := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, true, "2m", "sooner", cur); len(got) != 2 || got[0].At != now.Add(2*time.Minute) {
		t.Fatalf("an earlier request goes first: %+v", got)
	}
	if got := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, false, "", "", cur); len(got) != 1 {
		t.Fatalf("no request changed it: %+v", got)
	}
	if got := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, true, "9h", "idle", cur); len(got) != 1 {
		t.Fatalf("an idle request is no promise: %+v", got)
	}
}
